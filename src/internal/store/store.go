package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/example/distributed-compute-platform/internal/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct{ DB *pgxpool.Pool }

func New(db *pgxpool.Pool) *Store { return &Store{DB: db} }

const columns = `id::text, type, payload, status, result, error, attempts, max_attempts, cancel_requested, batch_id, created_at, updated_at, finished_at`

func scan(row pgx.Row) (model.Job, error) {
	var j model.Job
	err := row.Scan(&j.ID, &j.Type, &j.Payload, &j.Status, &j.Result, &j.Error, &j.Attempts, &j.MaxAttempts, &j.CancelRequested, &j.BatchID, &j.CreatedAt, &j.UpdatedAt, &j.FinishedAt)
	return j, err
}
func (s *Store) Get(ctx context.Context, id string) (model.Job, error) {
	return scan(s.DB.QueryRow(ctx, `SELECT `+columns+` FROM jobs WHERE id=$1::uuid`, id))
}
func (s *Store) Create(ctx context.Context, r model.SubmitRequest) (model.Job, error) {
	id, err := model.ID()
	if err != nil {
		return model.Job{}, err
	}
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return model.Job{}, err
	}
	defer tx.Rollback(ctx)
	var key any
	if r.IdempotencyKey != "" {
		key = r.IdempotencyKey
	}
	var inserted string
	err = tx.QueryRow(ctx, `INSERT INTO jobs (id,type,payload,max_attempts,idempotency_key,batch_id) VALUES ($1::uuid,$2,$3,$4,$5,$6) ON CONFLICT (idempotency_key) DO NOTHING RETURNING id::text`, id, r.Type, r.Payload, r.MaxAttempts, key, r.BatchID).Scan(&inserted)
	if errors.Is(err, pgx.ErrNoRows) {
		j, e := scan(tx.QueryRow(ctx, `SELECT `+columns+` FROM jobs WHERE idempotency_key=$1`, r.IdempotencyKey))
		return j, e
	}
	if err != nil {
		return model.Job{}, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO outbox (job_id) VALUES ($1::uuid)`, inserted); err != nil {
		return model.Job{}, err
	}
	j, err := scan(tx.QueryRow(ctx, `SELECT `+columns+` FROM jobs WHERE id=$1::uuid`, inserted))
	if err != nil {
		return model.Job{}, err
	}
	return j, tx.Commit(ctx)
}
func (s *Store) Cancel(ctx context.Context, id string) (model.Job, error) {
	_, err := s.DB.Exec(ctx, `UPDATE jobs SET status=CASE WHEN status='queued' THEN 'canceled' ELSE status END, cancel_requested=CASE WHEN status='running' THEN true ELSE cancel_requested END, finished_at=CASE WHEN status='queued' THEN now() ELSE finished_at END, updated_at=now() WHERE id=$1::uuid AND status IN ('queued','running')`, id)
	if err != nil {
		return model.Job{}, err
	}
	return s.Get(ctx, id)
}
func (s *Store) Claim(ctx context.Context, id, consumer string, leaseSeconds int) (model.Job, bool, error) {
	j, err := scan(s.DB.QueryRow(ctx, `UPDATE jobs SET status='running', attempts=attempts+1, claimed_by=$2, lease_expires_at=now()+($3::int * interval '1 second'), updated_at=now() WHERE id=$1::uuid AND (status='queued' OR (status='running' AND lease_expires_at < now())) AND attempts < max_attempts AND NOT cancel_requested RETURNING `+columns, id, consumer, leaseSeconds))
	if errors.Is(err, pgx.ErrNoRows) {
		return model.Job{}, false, nil
	}
	return j, err == nil, err
}
func (s *Store) Heartbeat(ctx context.Context, id, consumer string, leaseSeconds int) (bool, error) {
	var canceled bool
	err := s.DB.QueryRow(ctx, `UPDATE jobs SET lease_expires_at=now()+($3::int * interval '1 second') WHERE id=$1::uuid AND claimed_by=$2 AND status='running' RETURNING cancel_requested`, id, consumer, leaseSeconds).Scan(&canceled)
	if errors.Is(err, pgx.ErrNoRows) {
		return true, nil
	}
	return canceled, err
}
func (s *Store) Succeed(ctx context.Context, id, consumer string, result json.RawMessage) (bool, error) {
	tag, err := s.DB.Exec(ctx, `UPDATE jobs SET status=CASE WHEN cancel_requested THEN 'canceled' ELSE 'succeeded' END, result=CASE WHEN cancel_requested THEN NULL ELSE $3::jsonb END, finished_at=now(), lease_expires_at=NULL, claimed_by=NULL, updated_at=now() WHERE id=$1::uuid AND claimed_by=$2 AND status='running'`, id, consumer, result)
	return tag.RowsAffected() == 1, err
}
func (s *Store) Fail(ctx context.Context, id, consumer, message string) (string, error) {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	var attempts, max int
	var cancel bool
	err = tx.QueryRow(ctx, `SELECT attempts,max_attempts,cancel_requested FROM jobs WHERE id=$1::uuid AND claimed_by=$2 AND status='running' FOR UPDATE`, id, consumer).Scan(&attempts, &max, &cancel)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	status := "failed"
	if cancel {
		status = "canceled"
	} else if attempts < max {
		status = "queued"
	}
	if len(message) > 1024 {
		message = message[:1024]
	}
	_, err = tx.Exec(ctx, `UPDATE jobs SET status=$2,error=$3,claimed_by=NULL,lease_expires_at=NULL,finished_at=CASE WHEN $2='queued' THEN NULL ELSE now() END,updated_at=now() WHERE id=$1::uuid`, id, status, message)
	if err != nil {
		return "", err
	}
	if status == "queued" {
		delay := time.Duration(1<<min(attempts-1, 5)) * 500 * time.Millisecond
		_, err = tx.Exec(ctx, `INSERT INTO outbox(job_id,available_at) VALUES($1::uuid,now()+($2::bigint * interval '1 millisecond'))`, id, delay.Milliseconds())
		if err != nil {
			return "", err
		}
	}
	return status, tx.Commit(ctx)
}
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type OutboxEntry struct {
	ID    int64
	JobID string
}

func (s *Store) TakeOutbox(ctx context.Context, owner string, n int) ([]OutboxEntry, error) {
	rows, err := s.DB.Query(ctx, `WITH due AS (SELECT id FROM outbox WHERE published_at IS NULL AND available_at<=now() AND (claimed_until IS NULL OR claimed_until<now()) ORDER BY id LIMIT $2 FOR UPDATE SKIP LOCKED) UPDATE outbox o SET claimed_by=$1,claimed_until=now()+interval '15 seconds' FROM due WHERE o.id=due.id RETURNING o.id,o.job_id::text`, owner, n)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var entries []OutboxEntry
	for rows.Next() {
		var e OutboxEntry
		if err := rows.Scan(&e.ID, &e.JobID); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}
func (s *Store) MarkPublished(ctx context.Context, id int64, owner string) error {
	_, err := s.DB.Exec(ctx, `UPDATE outbox SET published_at=now(),claimed_by=NULL,claimed_until=NULL WHERE id=$1 AND claimed_by=$2 AND published_at IS NULL`, id, owner)
	return err
}
func (s *Store) RecoverTerminal(ctx context.Context) (int64, error) {
	tag, err := s.DB.Exec(ctx, `UPDATE jobs SET status=CASE WHEN cancel_requested THEN 'canceled' ELSE 'failed' END, error=CASE WHEN cancel_requested THEN 'canceled' ELSE 'worker lease expired after final attempt' END, finished_at=now(),claimed_by=NULL,lease_expires_at=NULL,updated_at=now() WHERE status='running' AND lease_expires_at<now() AND (attempts>=max_attempts OR cancel_requested)`)
	return tag.RowsAffected(), err
}
func (s *Store) List(ctx context.Context, limit int) ([]model.Job, error) {
	rows, err := s.DB.Query(ctx, `SELECT `+columns+` FROM jobs ORDER BY created_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]model.Job, 0)
	for rows.Next() {
		j, err := scan(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, j)
	}
	return items, rows.Err()
}

type BatchStats struct {
	BatchID   string `json:"batch_id"`
	Total     int64  `json:"total"`
	Queued    int64  `json:"queued"`
	Running   int64  `json:"running"`
	Succeeded int64  `json:"succeeded"`
	Failed    int64  `json:"failed"`
	Canceled  int64  `json:"canceled"`
}

func (s *Store) Stats(ctx context.Context, batchID string) (BatchStats, error) {
	st := BatchStats{BatchID: batchID}
	err := s.DB.QueryRow(ctx, `SELECT COUNT(*),COUNT(*) FILTER (WHERE status='queued'),COUNT(*) FILTER (WHERE status='running'),COUNT(*) FILTER (WHERE status='succeeded'),COUNT(*) FILTER (WHERE status='failed'),COUNT(*) FILTER (WHERE status='canceled') FROM jobs WHERE batch_id=$1`, batchID).Scan(&st.Total, &st.Queued, &st.Running, &st.Succeeded, &st.Failed, &st.Canceled)
	return st, err
}
func IsNotFound(err error) bool { return errors.Is(err, pgx.ErrNoRows) }
func ValidateUUID(id string) error {
	if len(id) != 36 {
		return fmt.Errorf("invalid job id")
	}
	for i, c := range id {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return fmt.Errorf("invalid job id")
			}
		} else if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
			return fmt.Errorf("invalid job id")
		}
	}
	return nil
}
