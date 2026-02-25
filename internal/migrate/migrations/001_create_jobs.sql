CREATE TABLE jobs (
    id                  UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    queue               TEXT        NOT NULL,
    handler_name        TEXT        NOT NULL,
    payload             JSONB       NOT NULL DEFAULT '{}',
    payload_hash        TEXT        NOT NULL,
    state               TEXT        NOT NULL DEFAULT 'pending',
    -- valid states: pending | running | completed | dead | canceled
    -- NO 'failed' state: retryable failures stay pending, terminal = dead
    priority            INT         NOT NULL DEFAULT 0,
    scheduled_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at        TIMESTAMPTZ,
    canceled_at         TIMESTAMPTZ,
    retry_count         INT         NOT NULL DEFAULT 0,
    max_retries         INT         NOT NULL DEFAULT 3,
    idempotency_key     TEXT        NOT NULL,
    locked_by           TEXT,
    locked_at           TIMESTAMPTZ,
    lock_expires_at     TIMESTAMPTZ,
    last_error          TEXT,
    last_error_at       TIMESTAMPTZ,
    current_execution_id UUID,
    state_version       INT         NOT NULL DEFAULT 0,

    CONSTRAINT jobs_state_check CHECK (
        state IN ('pending', 'running', 'completed', 'dead', 'canceled')
    ),
    CONSTRAINT jobs_idempotency_unique UNIQUE (queue, idempotency_key)
);

-- Primary poll index. state omitted from columns because the partial
-- WHERE clause already guarantees every row has state='pending'.
CREATE INDEX idx_jobs_poll
    ON jobs (queue, scheduled_at, priority DESC, created_at)
    WHERE state = 'pending';

-- Reaper scan: find expired running leases
CREATE INDEX idx_jobs_reaper
    ON jobs (state, lock_expires_at)
    WHERE state = 'running';

-- state_version for streaming clients polling for changes
CREATE INDEX idx_jobs_state_version ON jobs (id, state_version);
