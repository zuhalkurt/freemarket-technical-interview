# TASKS

## How would you design ingestion of this JSON into a Data Lake using Bronze > Silver > Gold layers?

I would ingest this JSON using a Medallion Architecture enforced by a versioned schema
contract to ensure data integrity, compliance, and scalability.

### Bronze Layer

- I store the JSON exactly as received, in a single variant column.
- I apply column-level encryption or tokenization for any identified PII fields before persisting.
- Metadata captures both event_time (source timestamp) and processing_time.
- No schema inference is allowed.

### Silver Layer

- This layer enforces the versioned schema contract.
- The pipeline attempts to extract and cast data based on the active versioned schema version.
- Any record failing a Data Quality Check ( missing required fields, invalid type etc.) is rejected to the Dead Letter Queue (DLQ).
- The pipeline fails fast and does not advance the watermark if a significant number of records are rejected.
- I flatten nested objects into a generic, normalized row structure (client_id, partner_bank_name, appetite, etc.).
- I apply strict type casting, mandating DECIMAL(38, X) precision for all financial fields and enforcing primary key uniqueness using idempotent upsert logic (MERGE with deduplication).

### Gold Layer

- I design stable, denormalized data marts and aggregated views (e.g., Client Risk Overview, Partner Performance).
- Gold models must stick to a stable, versioned contract to shield BI tools and internal applications from changes in the underlying Silver structure.

## How would you handle potential changes to nests and field names in the future?

I adopt a contract-driven approach where pipeline failure is preferable to silent data
corruption.

### Versioned Schema Contract Registry

- I maintain a central, versioned Data Contract that explicitly defines the expected logical name, data type, and physical JSON path for all fields.
- The Silver pipeline's extraction logic is entirely dependent on this active contract version.

### Fail-Fast Enforcement

- Upon ingestion, if the incoming payload structure deviates from the active versioned schema (a required field is missing or the physical JSON path isnot found etc.).
- The Silver process fails the entire batch immediately and generates an alert (through Slack channels,PagerDuty etc.) (P1 - P2 etc, depending on the criticality of the dataset). This forces an architectural review.

### Handling Permanent Changes (New Version)

- If the schema change is permanent (Credit.Score is renamed to Risk.Score), Data owners approve a new V-Schema version (V2.0).
- The Silver extraction logic is updated using coalescing logic to maintain backward compatibility. This allows reprocessing of historical data while integrating new structure without disruption.

### Dead Letter Queue (DLQ)

- Records that fail individual critical checks (invalid data type for a non-breaking field) or records from a failing batch are routed to a DLQ for manual inspection and reconciliation, preventing corrupted records from entering the Silver layer.

- ## Alternative Approach — Blueprint Schema (when JSON has many fields and frequently changing schemas)

For very wide or deeply nested JSON structures, I may also use a Blueprint Schema
approach:

### Blueprint Schema

Pre-defined structural template that describes the shape of the expected JSON, including nested objects, arrays, and optional fields.
This approach works particularly well when dealing with very large JSON payloads because it lets you define the overall shape of the data without tracking every individual field.

It reduces maintenance by grouping related fields together and allows the pipeline to absorb soft changes, like new optional fields, without immediately requiring a new schema version.
At the same time, it still provides enough structure to prevent breaking changes from slipping through unnoticed.

In practice, the Bronze layer validates each payload against the active blueprint, non-breaking changes are allowed through, and anything structurally incompatible is routed to a DLQ with an alert. Silver then reads from this stable blueprint instead of relying on dozens or hundreds of explicit JSON paths.
It gives teams a scalable balance and also strong governance without the overhead of constantly versioning minor upstream tweaks.
