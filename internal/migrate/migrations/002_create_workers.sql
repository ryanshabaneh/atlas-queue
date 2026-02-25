CREATE TABLE workers (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    hostname        TEXT        NOT NULL,
    queues          TEXT[]      NOT NULL,
    last_heartbeat  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status          TEXT        NOT NULL DEFAULT 'active',
    registered_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
