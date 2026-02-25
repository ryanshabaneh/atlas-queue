-- Fires on state, canceled_at, and scheduled_at changes.
-- The canceled_at trigger is critical: without it, canceling a running job
-- (which only sets canceled_at, not state) would produce no NOTIFY event
-- and streaming clients would miss the cancel request.
CREATE OR REPLACE FUNCTION notify_job_event() RETURNS trigger AS $$
BEGIN
    IF (OLD.state IS DISTINCT FROM NEW.state)
        OR (OLD.canceled_at IS DISTINCT FROM NEW.canceled_at)
        OR (OLD.scheduled_at IS DISTINCT FROM NEW.scheduled_at
            AND NEW.state = 'pending')
    THEN
        PERFORM pg_notify(
            'job_events',
            json_build_object(
                'job_id',           NEW.id::text,
                'state',            NEW.state,
                'state_version',    NEW.state_version,
                'cancel_requested', (NEW.canceled_at IS NOT NULL)::boolean
            )::text
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER job_state_notify
    AFTER UPDATE OF state, canceled_at, scheduled_at ON jobs
    FOR EACH ROW EXECUTE FUNCTION notify_job_event();
