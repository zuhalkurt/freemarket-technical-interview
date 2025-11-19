For high-volume ingestion, I use a watermark-based micro-batch streaming approach, making sure the loads are idempotent, efficient, and easy to reconcile.

## Watermark Tracking and Overlap

I use a control table to track the last successfully processed last_updated_at. The source query includes a minimal overlap (lets say 2
minutes) before the watermark to protect against system clock skew and late-arriving data.

## Incremental Landing (Bronze)

Only the changed records are landed into the Bronze Delta/Lake table.

## Idempotent Upsert (Silver MERGE)

I apply a MERGE-on-key approach to insert new records and update existing ones.

- Handling Deletes: This must account for logical deletes. I use soft-deletes where the source signals a deletion via an is_active=FALSE flag, which the MERGE operation updates in Silver instead of physically deleting the historical record.
- Optimization: Silver tables are partitioned by business date and Z-Ordered by the primary key and the timestamp. This reduces file scanning costs during the high-frequency MERGE operations.

## Critical Data Completeness Validations

The run is only committed if the following checks pass:

- Reconciliation: Aggregated totals from the source are matched against Silver aggregates to confirm the batch has been fully and accurately captured.
- Watermark Continuity: Validates that there are no gaps or regressions in the change-tracking field, ensuring no time slices were missed.
- Primary Key Uniqueness: Confirms that no duplicate primary keys exist in the Silver table after the MERGE. 

- Mandatory Field Null Checks: Ensures that key business-critical fields (e.g.,IDs, timestamps, financial amounts) are not unexpectedly null in the incoming batch.
- Volume Anomaly Detection: Flags unusually high or low row counts relative to historical patterns, which may indicate missing or duplicated data.

If any critical validation fails, the Silver commit is rolled back, the run is flagged, and
the watermark is not advanced.
