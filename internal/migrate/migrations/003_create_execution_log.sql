-- Append-only. payload is stored here in full even though it duplicates the
-- job row. This makes deterministic replay bulletproof: it reads payload from
-- the log row, so it works even after the job row is cleaned up.
-- Valid outcomes: completed | failed | canceled | orphaned
-- The outcome 'dead' is NEVER used — that is a job state, not an execution outcome.
CREATE TABLE execution_log (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id          UUID        NOT NULL REFERENCES jobs(id),
    worker_id       UUID        NOT NULL REFERENCES workers(id),
    worker_hostname TEXT        NOT NULL,
    handler_name    TEXT        NOT NULL,
    attempt         INT         NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMPTZ,
    outcome         TEXT,
    -- valid: completed | failed | canceled | orphaned
    -- NEVER 'dead' — that is a job state, not an execution outcome
    error_message   TEXT,
    CONSTRAINT execution_log_outcome_check CHECK (
        outcome IS NULL
        OR outcome IN ('completed', 'failed', 'canceled', 'orphaned')
    ),
    trace_id        TEXT        NOT NULL,
    payload         JSONB       NOT NULL,
    payload_hash    TEXT        NOT NULL
);

CREATE INDEX idx_execlog_job_id ON execution_log (job_id);
CREATE INDEX idx_execlog_trace  ON execution_log (trace_id);
