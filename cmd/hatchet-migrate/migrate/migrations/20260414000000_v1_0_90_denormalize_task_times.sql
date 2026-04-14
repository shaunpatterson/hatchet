-- +goose Up
-- +goose StatementBegin

-- Denormalize started_at, finished_at, and duration_ms onto v1_tasks_olap so that
-- sort queries can use indexes instead of computing these values via LATERAL JOINs
-- to v1_task_events_olap at query time. This is critical for performance at scale
-- (hundreds of millions of rows).

ALTER TABLE v1_tasks_olap ADD COLUMN IF NOT EXISTS started_at TIMESTAMPTZ;
ALTER TABLE v1_tasks_olap ADD COLUMN IF NOT EXISTS finished_at TIMESTAMPTZ;
ALTER TABLE v1_tasks_olap ADD COLUMN IF NOT EXISTS duration_ms BIGINT;

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

-- Backfill existing data from v1_task_events_olap.
-- NOTE: On very large databases this may take a while. Consider running the backfill
-- in a separate session with smaller batches if the migration times out.

-- Backfill started_at
UPDATE v1_tasks_olap t
SET started_at = sub.started_at
FROM (
    SELECT e.task_id, e.task_inserted_at, MAX(e.event_timestamp) AS started_at
    FROM v1_task_events_olap e
    WHERE e.event_type = 'STARTED'
    GROUP BY e.task_id, e.task_inserted_at
) sub
WHERE t.id = sub.task_id
  AND t.inserted_at = sub.task_inserted_at
  AND t.started_at IS NULL;

-- Backfill finished_at
UPDATE v1_tasks_olap t
SET finished_at = sub.finished_at
FROM (
    SELECT e.task_id, e.task_inserted_at, MAX(e.event_timestamp) AS finished_at
    FROM v1_task_events_olap e
    WHERE e.readable_status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EVICTED')
    GROUP BY e.task_id, e.task_inserted_at
) sub
WHERE t.id = sub.task_id
  AND t.inserted_at = sub.task_inserted_at
  AND t.finished_at IS NULL;

-- Backfill duration_ms (only for tasks with both started_at and finished_at)
UPDATE v1_tasks_olap
SET duration_ms = (EXTRACT(EPOCH FROM (finished_at - started_at)) * 1000)::bigint
WHERE started_at IS NOT NULL
  AND finished_at IS NOT NULL
  AND duration_ms IS NULL;

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
-- +goose StatementEnd
