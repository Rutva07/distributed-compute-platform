CREATE TABLE IF NOT EXISTS jobs (
 id UUID PRIMARY KEY,
 type TEXT NOT NULL,
 payload JSONB NOT NULL,
 status TEXT NOT NULL DEFAULT 'queued' CHECK (status IN ('queued','running','succeeded','failed','canceled')),
 result JSONB,
 error TEXT NOT NULL DEFAULT '',
 attempts INTEGER NOT NULL DEFAULT 0,
 max_attempts INTEGER NOT NULL DEFAULT 3,
 idempotency_key TEXT UNIQUE,
 batch_id TEXT NOT NULL DEFAULT '',
 claimed_by TEXT,
 lease_expires_at TIMESTAMPTZ,
 cancel_requested BOOLEAN NOT NULL DEFAULT false,
 created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 finished_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS jobs_batch_idx ON jobs(batch_id) WHERE batch_id <> '';
CREATE INDEX IF NOT EXISTS jobs_status_idx ON jobs(status);
CREATE TABLE IF NOT EXISTS outbox (
 id BIGSERIAL PRIMARY KEY,
 job_id UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
 available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
 claimed_by TEXT,
 claimed_until TIMESTAMPTZ,
 published_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS outbox_due_idx ON outbox(available_at, id) WHERE published_at IS NULL;
