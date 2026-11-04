-- queueforge schema, version 1.
--
-- Every state change goes through queueforge_check_job_change(), which rejects any transition
-- that is not listed in queueforge_transitions. The README state diagram is checked against that
-- table by tests/test_schema.py.

CREATE TABLE queueforge_transitions (
    from_state text NOT NULL,
    to_state   text NOT NULL,
    PRIMARY KEY (from_state, to_state)
);

INSERT INTO queueforge_transitions (from_state, to_state) VALUES
    ('queued',  'running'),    -- claimed by a worker
    ('queued',  'cancelled'),  -- cancelled before it started
    ('failed',  'running'),    -- retry claimed after its backoff delay
    ('failed',  'cancelled'),  -- cancelled while waiting for a retry
    ('running', 'succeeded'),  -- handler returned, lease still valid
    ('running', 'failed'),     -- handler raised or lease expired, attempts remain
    ('running', 'dead'),       -- attempts exhausted or permanent error
    ('running', 'cancelled'),  -- cooperative cancellation acknowledged
    ('running', 'queued'),     -- released by a worker shutting down
    ('dead',    'queued');     -- operator redrive from the dead-letter state

CREATE TABLE queueforge_jobs (
    id                   bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    queue                text             NOT NULL CHECK (queue <> ''),
    task                 text             NOT NULL CHECK (task <> ''),
    payload              jsonb            NOT NULL DEFAULT '{}'::jsonb,
    state                text             NOT NULL DEFAULT 'queued'
        CHECK (state IN ('queued', 'running', 'succeeded', 'failed', 'dead', 'cancelled')),
    priority             integer          NOT NULL DEFAULT 0,
    run_at               timestamptz      NOT NULL DEFAULT now(),
    -- Number of times the job has been claimed. Never decreases, so (id, attempts) is a
    -- monotonic fencing number that handlers can pass to downstream systems.
    attempts             integer          NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts         integer          NOT NULL DEFAULT 5 CHECK (max_attempts >= 1),
    backoff_base_seconds double precision NOT NULL DEFAULT 1 CHECK (backoff_base_seconds >= 0),
    backoff_cap_seconds  double precision NOT NULL DEFAULT 300,
    idempotency_key      text CHECK (idempotency_key <> ''),
    lease_token          uuid,
    lease_expires_at     timestamptz,
    locked_by            text,
    heartbeat_at         timestamptz,
    cancel_requested     boolean          NOT NULL DEFAULT false,
    last_error           text,
    created_at           timestamptz      NOT NULL DEFAULT now(),
    updated_at           timestamptz      NOT NULL DEFAULT now(),
    finished_at          timestamptz,
    CONSTRAINT queueforge_jobs_backoff_cap CHECK (backoff_cap_seconds >= backoff_base_seconds),
    -- A lease exists exactly while the job is running.
    CONSTRAINT queueforge_jobs_lease_iff_running CHECK (
        (state = 'running'
            AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL AND locked_by IS NOT NULL)
        OR (state <> 'running'
            AND lease_token IS NULL AND lease_expires_at IS NULL AND locked_by IS NULL)
    ),
    CONSTRAINT queueforge_jobs_finished_iff_terminal CHECK (
        (state IN ('succeeded', 'dead', 'cancelled')) = (finished_at IS NOT NULL)
    )
);

-- Idempotency keys are unique per queue for as long as the row exists.
CREATE UNIQUE INDEX queueforge_jobs_idempotency
    ON queueforge_jobs (queue, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

-- Claim path: runnable jobs in priority order.
CREATE INDEX queueforge_jobs_runnable
    ON queueforge_jobs (queue, priority DESC, run_at, id)
    WHERE state IN ('queued', 'failed');

-- Reaper path: running jobs by lease expiry.
CREATE INDEX queueforge_jobs_leases
    ON queueforge_jobs (queue, lease_expires_at)
    WHERE state = 'running';

-- One row per claim: who ran it, under which lease, and how it ended.
CREATE TABLE queueforge_attempts (
    job_id      bigint      NOT NULL REFERENCES queueforge_jobs (id) ON DELETE CASCADE,
    attempt     integer     NOT NULL CHECK (attempt >= 1),
    worker_id   text        NOT NULL,
    lease_token uuid        NOT NULL,
    started_at  timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    outcome     text CHECK (
        outcome IN ('succeeded', 'failed', 'lease_expired', 'cancelled', 'released')
    ),
    error       text,
    PRIMARY KEY (job_id, attempt),
    CONSTRAINT queueforge_attempts_outcome_iff_finished CHECK (
        (outcome IS NULL) = (finished_at IS NULL)
    )
);

CREATE FUNCTION queueforge_check_job_change() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'queued' THEN
            RAISE EXCEPTION 'queueforge: jobs must be inserted as queued, got %', NEW.state
                USING ERRCODE = 'check_violation';
        END IF;
        RETURN NEW;
    END IF;

    IF NEW.state IS DISTINCT FROM OLD.state AND NOT EXISTS (
        SELECT 1 FROM queueforge_transitions t
        WHERE t.from_state = OLD.state AND t.to_state = NEW.state
    ) THEN
        RAISE EXCEPTION 'queueforge: illegal transition % -> % for job %',
            OLD.state, NEW.state, OLD.id
            USING ERRCODE = 'check_violation';
    END IF;

    IF NEW.attempts < OLD.attempts THEN
        RAISE EXCEPTION 'queueforge: attempts may not decrease for job %', OLD.id
            USING ERRCODE = 'check_violation';
    END IF;

    -- A running job keeps one lease token; a new token requires a new claim.
    IF OLD.state = 'running' AND NEW.state = 'running'
       AND NEW.lease_token IS DISTINCT FROM OLD.lease_token THEN
        RAISE EXCEPTION 'queueforge: lease token of running job % may not change', OLD.id
            USING ERRCODE = 'check_violation';
    END IF;

    NEW.updated_at := now();
    RETURN NEW;
END;
$$;

CREATE TRIGGER queueforge_jobs_check_change
    BEFORE INSERT OR UPDATE ON queueforge_jobs
    FOR EACH ROW EXECUTE FUNCTION queueforge_check_job_change();
