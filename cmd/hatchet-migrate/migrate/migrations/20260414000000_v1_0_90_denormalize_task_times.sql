-- +goose Up
-- +goose StatementBegin

-- Denormalize started_at, finished_at, and duration_ms onto v1_tasks_olap so that
-- sort queries can use indexes instead of computing these values via LATERAL JOINs
-- to v1_task_events_olap at query time. This is critical for performance at scale
-- (hundreds of millions of rows).

ALTER TABLE v1_tasks_olap ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ;
ALTER TABLE v1_tasks_olap ADD COLUMN IF NOT EXISTS finished_at TIMESTAMPTZ;
ALTER TABLE v1_tasks_olap ADD COLUMN IF NOT EXISTS duration_ms BIGINT;

-- Guard the existing v1_tasks_olap update trigger so it only fires when
-- readable_status is modified. Without this, the denormalization UPDATEs
-- (which only touch started_at/finished_at/duration_ms) would fire the cascade
-- and cause unnecessary writes to v1_runs_olap and v1_task_status_updates_tmp.
--
-- Uses AFTER UPDATE OF readable_status so the trigger doesn't fire at all for
-- updates that don't touch the status column. The IS DISTINCT FROM guard inside
-- handles the edge case of a no-op SET readable_status = same_value.
DROP TRIGGER IF EXISTS v1_tasks_olap_status_update_trigger ON v1_tasks_olap;

CREATE OR REPLACE FUNCTION v1_tasks_olap_status_update_function()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE
        v1_runs_olap r
    SET
        readable_status = n.readable_status
    FROM new_rows n
    JOIN old_rows o ON n.id = o.id AND n.inserted_at = o.inserted_at
    WHERE
        r.id = n.id
        AND r.inserted_at = n.inserted_at
        AND r.kind = 'TASK'
        AND n.readable_status IS DISTINCT FROM o.readable_status;

    INSERT INTO v1_task_status_updates_tmp (
        tenant_id,
        dag_id,
        dag_inserted_at
    )
    SELECT
        n.tenant_id,
        n.dag_id,
        n.dag_inserted_at
    FROM new_rows n
    JOIN old_rows o ON n.id = o.id AND n.inserted_at = o.inserted_at
    WHERE n.dag_id IS NOT NULL
      AND n.readable_status IS DISTINCT FROM o.readable_status;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER v1_tasks_olap_status_update_trigger
AFTER UPDATE OF readable_status ON v1_tasks_olap
REFERENCING OLD TABLE AS old_rows NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION v1_tasks_olap_status_update_function();

-- Trigger function: when events are inserted into v1_task_events_olap, update the
-- denormalized columns on v1_tasks_olap. Fires once per statement (batch-friendly).
CREATE OR REPLACE FUNCTION v1_task_events_olap_denormalize_times()
RETURNS TRIGGER AS $$
BEGIN
    -- Update started_at for STARTED events
    UPDATE v1_tasks_olap t
    SET started_at = sub.started_at
    FROM (
        SELECT n.task_id, n.task_inserted_at, MAX(n.event_timestamp) AS started_at
        FROM new_rows n
        WHERE n.event_type = 'STARTED'
        GROUP BY n.task_id, n.task_inserted_at
    ) sub
    WHERE t.id = sub.task_id
      AND t.inserted_at = sub.task_inserted_at
      AND (t.started_at IS NULL OR t.started_at < sub.started_at);

    -- Update finished_at for terminal events
    UPDATE v1_tasks_olap t
    SET finished_at = sub.finished_at
    FROM (
        SELECT n.task_id, n.task_inserted_at, MAX(n.event_timestamp) AS finished_at
        FROM new_rows n
        WHERE n.readable_status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EVICTED')
        GROUP BY n.task_id, n.task_inserted_at
    ) sub
    WHERE t.id = sub.task_id
      AND t.inserted_at = sub.task_inserted_at
      AND (t.finished_at IS NULL OR t.finished_at < sub.finished_at);

    -- Recompute duration_ms for any task touched by this batch that now has both
    -- timestamps. This handles out-of-order arrival (e.g. terminal event arrives
    -- before STARTED event in a separate batch).
    UPDATE v1_tasks_olap t
    SET duration_ms = (EXTRACT(EPOCH FROM (t.finished_at - t.started_at)) * 1000)::bigint
    FROM (SELECT DISTINCT n.task_id, n.task_inserted_at FROM new_rows n) sub
    WHERE t.id = sub.task_id
      AND t.inserted_at = sub.task_inserted_at
      AND t.started_at IS NOT NULL
      AND t.finished_at IS NOT NULL
      AND (t.duration_ms IS NULL
           OR t.duration_ms IS DISTINCT FROM (EXTRACT(EPOCH FROM (t.finished_at - t.started_at)) * 1000)::bigint);

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER v1_task_events_olap_denormalize_trigger
AFTER INSERT ON v1_task_events_olap
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION v1_task_events_olap_denormalize_times();

-- Covering indexes for sort queries. Each index is designed so that the planner can
-- do a Merge Append of index scans across pruned partitions with LIMIT, reading only
-- the top rows and stopping early.

-- Sort by createdAt (default sort)
CREATE INDEX IF NOT EXISTS idx_v1_tasks_olap_tenant_created
ON v1_tasks_olap (tenant_id, inserted_at DESC, id DESC);

-- Sort by startedAt
CREATE INDEX IF NOT EXISTS idx_v1_tasks_olap_tenant_started
ON v1_tasks_olap (tenant_id, started_at DESC NULLS LAST, inserted_at DESC, id DESC)
WHERE started_at IS NOT NULL;

-- Sort by finishedAt
CREATE INDEX IF NOT EXISTS idx_v1_tasks_olap_tenant_finished
ON v1_tasks_olap (tenant_id, finished_at DESC NULLS LAST, inserted_at DESC, id DESC)
WHERE finished_at IS NOT NULL;

-- Sort by duration
CREATE INDEX IF NOT EXISTS idx_v1_tasks_olap_tenant_duration
ON v1_tasks_olap (tenant_id, duration_ms DESC NULLS LAST, inserted_at DESC, id DESC)
WHERE duration_ms IS NOT NULL;

-- Backfill existing data from v1_task_events_olap, one day partition at a time.
-- Enumerates partitions from pg_inherits (avoids scanning the whole table for dates).
-- Uses correlated subqueries keyed on (task_id, task_inserted_at) to leverage the
-- v1_task_events_olap PK: (task_id, task_inserted_at, id).
--
-- NOTE: This runs in a single transaction. For very large databases (100M+ rows),
-- consider extracting the backfill loop into a PROCEDURE with per-day COMMITs
-- to reduce WAL pressure and lock duration.
DO $$
DECLARE
    day_start TIMESTAMPTZ;
    day_end TIMESTAMPTZ;
    partition_name TEXT;
    rows_affected BIGINT;
BEGIN
    -- Iterate over first-level (daily) child partitions of v1_tasks_olap.
    -- Partition names follow the pattern v1_tasks_olap_YYYYMMDD.
    FOR partition_name IN
        SELECT c.relname
        FROM pg_inherits i
        JOIN pg_class c ON i.inhrelid = c.oid
        WHERE i.inhparent = 'v1_tasks_olap'::regclass
        ORDER BY c.relname
    LOOP
        -- Extract date bounds from partition name (v1_tasks_olap_YYYYMMDD).
        -- Uses session timezone, matching how partitions are created in
        -- create_v1_olap_partition_with_date_and_status.
        BEGIN
            day_start := to_timestamp(right(partition_name, 8), 'YYYYMMDD');
            day_end := day_start + INTERVAL '1 day';
        EXCEPTION WHEN OTHERS THEN
            CONTINUE;  -- skip partitions with unexpected naming
        END;

        -- Backfill started_at using correlated subquery (uses events PK)
        UPDATE v1_tasks_olap t
        SET started_at = (
            SELECT MAX(e.event_timestamp)
            FROM v1_task_events_olap e
            WHERE e.task_id = t.id
              AND e.task_inserted_at = t.inserted_at
              AND e.event_type = 'STARTED'
        )
        WHERE t.inserted_at >= day_start
          AND t.inserted_at < day_end
          AND t.started_at IS NULL
          AND EXISTS (
            SELECT 1 FROM v1_task_events_olap e
            WHERE e.task_id = t.id
              AND e.task_inserted_at = t.inserted_at
              AND e.event_type = 'STARTED'
          );

        GET DIAGNOSTICS rows_affected = ROW_COUNT;
        IF rows_affected > 0 THEN
            RAISE NOTICE 'Backfilled started_at for % rows in %', rows_affected, partition_name;
        END IF;

        -- Backfill finished_at using correlated subquery (uses events PK)
        UPDATE v1_tasks_olap t
        SET finished_at = (
            SELECT MAX(e.event_timestamp)
            FROM v1_task_events_olap e
            WHERE e.task_id = t.id
              AND e.task_inserted_at = t.inserted_at
              AND e.readable_status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EVICTED')
        )
        WHERE t.inserted_at >= day_start
          AND t.inserted_at < day_end
          AND t.finished_at IS NULL
          AND EXISTS (
            SELECT 1 FROM v1_task_events_olap e
            WHERE e.task_id = t.id
              AND e.task_inserted_at = t.inserted_at
              AND e.readable_status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EVICTED')
          );

        GET DIAGNOSTICS rows_affected = ROW_COUNT;
        IF rows_affected > 0 THEN
            RAISE NOTICE 'Backfilled finished_at for % rows in %', rows_affected, partition_name;
        END IF;

        -- Backfill duration_ms for this day
        UPDATE v1_tasks_olap
        SET duration_ms = (EXTRACT(EPOCH FROM (finished_at - started_at)) * 1000)::bigint
        WHERE inserted_at >= day_start
          AND inserted_at < day_end
          AND started_at IS NOT NULL
          AND finished_at IS NOT NULL
          AND duration_ms IS NULL;

        GET DIAGNOSTICS rows_affected = ROW_COUNT;
        IF rows_affected > 0 THEN
            RAISE NOTICE 'Backfilled duration_ms for % rows in %', rows_affected, partition_name;
        END IF;
    END LOOP;
END $$;

ANALYZE v1_tasks_olap;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TRIGGER IF EXISTS v1_task_events_olap_denormalize_trigger ON v1_task_events_olap;
DROP FUNCTION IF EXISTS v1_task_events_olap_denormalize_times();

DROP INDEX IF EXISTS idx_v1_tasks_olap_tenant_created;
DROP INDEX IF EXISTS idx_v1_tasks_olap_tenant_started;
DROP INDEX IF EXISTS idx_v1_tasks_olap_tenant_finished;
DROP INDEX IF EXISTS idx_v1_tasks_olap_tenant_duration;

ALTER TABLE v1_tasks_olap DROP COLUMN IF EXISTS started_at;
ALTER TABLE v1_tasks_olap DROP COLUMN IF EXISTS finished_at;
ALTER TABLE v1_tasks_olap DROP COLUMN IF EXISTS duration_ms;

-- Restore the original update trigger without the OLD TABLE guard
DROP TRIGGER IF EXISTS v1_tasks_olap_status_update_trigger ON v1_tasks_olap;

CREATE OR REPLACE FUNCTION v1_tasks_olap_status_update_function()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE
        v1_runs_olap r
    SET
        readable_status = n.readable_status
    FROM new_rows n
    WHERE
        r.id = n.id
        AND r.inserted_at = n.inserted_at
        AND r.kind = 'TASK';

    INSERT INTO v1_task_status_updates_tmp (
        tenant_id,
        dag_id,
        dag_inserted_at
    )
    SELECT
        tenant_id,
        dag_id,
        dag_inserted_at
    FROM new_rows
    WHERE dag_id IS NOT NULL;

    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER v1_tasks_olap_status_update_trigger
AFTER UPDATE ON v1_tasks_olap
REFERENCING NEW TABLE AS new_rows
FOR EACH STATEMENT
EXECUTE FUNCTION v1_tasks_olap_status_update_function();
-- +goose StatementEnd
