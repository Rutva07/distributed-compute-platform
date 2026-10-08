package model

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type Job struct {
	ID              string          `json:"id"`
	Type            string          `json:"type"`
	Payload         json.RawMessage `json:"payload"`
	Status          string          `json:"status"`
	Result          json.RawMessage `json:"result,omitempty"`
	Error           string          `json:"error,omitempty"`
	Attempts        int             `json:"attempts"`
	MaxAttempts     int             `json:"max_attempts"`
	CancelRequested bool            `json:"cancel_requested"`
	BatchID         string          `json:"batch_id,omitempty"`
	CreatedAt       time.Time       `json:"created_at"`
	UpdatedAt       time.Time       `json:"updated_at"`
	FinishedAt      *time.Time      `json:"finished_at,omitempty"`
}

type SubmitRequest struct {
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	MaxAttempts    int             `json:"max_attempts,omitempty"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	BatchID        string          `json:"batch_id,omitempty"`
}

func ID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	h := hex.EncodeToString(b)
	return fmt.Sprintf("%s-%s-%s-%s-%s", h[:8], h[8:12], h[12:16], h[16:20], h[20:]), nil
}
func (s *SubmitRequest) Validate() error {
	switch s.Type {
	case "json_transform", "text", "numeric", "aggregate", "simulated_io":
	default:
		return errors.New("unsupported job type")
	}
	if len(s.Payload) == 0 || !json.Valid(s.Payload) {
		return errors.New("payload must be valid JSON")
	}
	if s.MaxAttempts == 0 {
		s.MaxAttempts = 3
	}
	if s.MaxAttempts < 1 || s.MaxAttempts > 10 {
		return errors.New("max_attempts must be between 1 and 10")
	}
	if len(s.IdempotencyKey) > 128 || len(s.BatchID) > 128 {
		return errors.New("identifier exceeds 128 characters")
	}
	return nil
}
