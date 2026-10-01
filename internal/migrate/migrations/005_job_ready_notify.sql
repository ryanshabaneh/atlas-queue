-- Wakes idle workers when a job becomes claimable, so they can block on
-- LISTEN job_ready instead of polling. Separate from job_events (004), which
-- is a per-job state stream for gRPC clients; this channel carries only the
-- queue name because workers just need to know which queue to drain.
--
-- NOTIFY is delivered on commit, so a woken worker always sees the row.
-- Rows that become pending with a future scheduled_at still notify; the worker
-- drains, finds nothing claimable, and sleeps until MIN(scheduled_at).
CREATE OR REPLACE FUNCTION notify_job_ready() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('job_ready', NEW.queue);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- New submissions.
CREATE TRIGGER job_ready_insert_notify
    AFTER INSERT ON jobs
    FOR EACH ROW
    WHEN (NEW.state = 'pending')
    EXECUTE FUNCTION notify_job_ready();

-- Transitions back to pending: retries (markRetry) and reaper reclaims.
CREATE TRIGGER job_ready_update_notify
    AFTER UPDATE OF state ON jobs
    FOR EACH ROW
    WHEN (OLD.state IS DISTINCT FROM 'pending' AND NEW.state = 'pending')
    EXECUTE FUNCTION notify_job_ready();
