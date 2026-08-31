# MongoDB → PostgreSQL Migration — Design Document

**Branch:** `feature/mongo-postgres-adapter`  
**Status:** Design phase — no code written yet

---

## Scope

This document covers the design decisions for migrating data from a MongoDB source to a
PostgreSQL target. MongoDB → MongoDB is explicitly out of scope (mongodump/mongorestore
handles that natively). PostgreSQL → MongoDB is deferred.

---

## Core Problem

MongoDB is document-oriented and schema-less. PostgreSQL is relational and requires a fixed
schema before any data can be inserted. Every design decision in this feature flows from
that mismatch:

- There is no catalog to introspect for column types — we must **sample documents**
- Documents can be arbitrarily nested — we must decide how to flatten or store them
- The same field can hold different BSON types across documents — we must pick a winner
- Fields can be absent from some documents — those become nullable columns
- The migration engine needs the user's schema decisions **before** data transfer begins

---

## Workflow

```
xferdb project create   (source = mongodb://, target = postgres://)
        ↓
xferdb project analyze  ← NEW: sample documents, infer schema, optionally consult AI
        ↓
xferdb project schema   ← NEW: review inferred schema, override field decisions
        ↓
xferdb migrate          ← uses saved schema plan; no re-sampling at runtime
```

The schema plan is a prerequisite for migration. `xferdb migrate` will fail fast if no
plan exists for a mongo→postgres project.

---

## Phase 1 — Schema Analysis

### Document Sampling

Sample N documents per collection using MongoDB's `$sample` aggregation stage:

```
db.collection.aggregate([{ $sample: { size: N } }])
```

Default sample size: **2,000 documents**. Configurable via `--sample-size` on the
`analyze` command. Larger samples improve accuracy at the cost of analysis time.

For each document in the sample, walk every key recursively and record:
- Field name (dot-notation for nested: `address.city`)
- BSON type observed
- Whether the value was null or the field was absent

### Field Frequency Table

After sampling, each field has a frequency record:

```
Field            Occurrences   Types observed (count)
─────────────────────────────────────────────────────
_id              2000/2000     ObjectId(2000)
customer_id      2000/2000     Int64(2000)
total            2000/2000     Double(1940), Decimal128(60)
address          2000/2000     Document(2000)
tags             1740/2000     Array(1740)
notes            460/2000      String(420), Null(40)
metadata         820/2000      Document(820)
```

### Nesting Strategy — Two Options

**Option A: Store as `jsonb` (default)**
Nested documents and arrays are stored as a single `jsonb` column. The MongoDB
structure is preserved as-is. Queryable in Postgres via `->>`, `@>`, `jsonb_path_query`.

```
address   jsonb    -- {"city": "NYC", "zip": "10001", "country": "US"}
tags      jsonb    -- ["promo", "vip"]
```

**Option B: Flatten one level**
Each key of a nested document becomes its own column, prefixed with the parent field name.
Arrays cannot be flattened — they remain `jsonb`.

```
address_city      text
address_zip       text
address_country   text
tags              jsonb    -- arrays always stay as jsonb
```

Flattening rules:
- Only one level deep. `address.billing.street` → `address_billing` as `jsonb`, not further flattened.
- If two documents have different keys under the same nested document, all observed keys
  become columns (all nullable).
- Arrays of documents are never flattened — stored as `jsonb`.

Per-field decision: the user can set `address=flatten` and `metadata=jsonb` independently.
The default for any nested document field is `jsonb`.

---

## Type Mapping

Canonical BSON → PostgreSQL type mapping applied during both schema inference and data transfer:

| BSON Type       | PostgreSQL Type     | Notes |
|-----------------|---------------------|-------|
| `ObjectId`      | `text`              | Stored as 24-char hex string. Default for `_id`. |
| `String`        | `text`              | Direct. |
| `Int32`         | `integer`           | Direct. |
| `Int64`         | `bigint`            | Direct. |
| `Double`        | `double precision`  | Direct. |
| `Decimal128`    | `numeric`           | Converted via `.String()` → `numeric`. |
| `Boolean`       | `boolean`           | Direct. |
| `Date`          | `timestamptz`       | Converted via `.Time()`. |
| `Array`         | `jsonb`             | Always. No column-level type inference for array elements. |
| `Document`      | `jsonb`             | Unless strategy is `flatten`. |
| `Binary`        | `bytea`             | Direct. |
| `Null` / absent | `NULL`              | Both map to NULL. Column marked nullable. |
| `Regex`         | `text`              | Pattern string stored; flags dropped. |

### Polymorphic Fields

When a field holds multiple BSON types across documents, pick the **widest compatible type**:

```
Double + Decimal128         → numeric      (exact; covers both)
Int32  + Int64              → bigint       (covers both)
Int32  + Double             → double precision
Any numeric + String        → text         (universal fallback)
Any type    + Document      → jsonb        (universal container)
String + Null               → text, nullable
```

If no widest type covers all observed values cleanly, fall back to `jsonb`. The analysis
output flags every polymorphic field as a warning so the user can review the decision.

### The `_id` Field

`_id` is always the primary key in the target table. Type options:

| Source `_id` type | Default PG type | Alternative |
|-------------------|----------------|-------------|
| `ObjectId`        | `text`         | none (ObjectId has no PG native) |
| `Int32` / `Int64` | `bigint`       | `integer` if fits |
| `String`          | `text`         | none |
| `UUID` (Binary subtype 4) | `uuid` | `text` |

The column is always named `_id` in the target by default. Rename is supported via the
schema plan (`_id → id`) but creates a mismatch the user must be aware of.

---

## Schema Plan

### What It Stores

The schema plan is the authoritative record of user-approved field decisions for a
project. It is persisted in the state DB and read by the Mongo source adapter at
migration time.

One row per field per collection:

```sql
CREATE TABLE schema_plans (
    project_id   TEXT     NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    collection   TEXT     NOT NULL,
    field_name   TEXT     NOT NULL,   -- dot-notation for nested: "address.city"
    pg_column    TEXT     NOT NULL,   -- target column name (may differ from field_name)
    pg_type      TEXT     NOT NULL,   -- e.g. "text", "bigint", "jsonb", "timestamptz"
    strategy     TEXT     NOT NULL,   -- "direct" | "flatten" | "as_jsonb" | "skip"
    is_pk        BOOLEAN  NOT NULL DEFAULT FALSE,
    nullable     BOOLEAN  NOT NULL DEFAULT TRUE,
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (project_id, collection, field_name)
);
```

### Strategies

| Strategy   | Meaning |
|-----------|---------|
| `direct`   | Convert BSON value to the declared `pg_type` and insert as a scalar column |
| `as_jsonb` | Serialize the value (document, array, or scalar) to JSON and store as `jsonb` |
| `flatten`  | Expand the nested document into child columns (one schema_plan row per child key) |
| `skip`     | Exclude this field entirely — not inserted into the target |

---

## Phase 2 — Data Transfer

### ReadBatch (Mongo Source)

Keyset pagination on `_id` — no OFFSET scan:

```go
filter := bson.D{}
if opts.LastPK != nil {
    // Resume from checkpoint: decode stored hex/string back to native _id type
    filter = bson.D{{Key: "_id", Value: bson.D{{Key: "$gt", Value: lastID}}}}
}
cursor := coll.Find(ctx, filter,
    options.Find().
        SetSort(bson.D{{Key: "_id", Value: 1}}).
        SetLimit(int64(opts.Limit)))
```

Checkpoint stores `_id` as a string in `checkpoints.last_pk` (already TEXT in state DB).
Encoding:
- `ObjectId` → 24-char hex string
- `int32` / `int64` → decimal string
- `string` → stored as-is
- `UUID` → standard UUID string format

### Value Conversion (Source Side)

**Decision: conversion happens inside the Mongo source adapter's `ReadBatch`, not in the
engine or target adapter.**

By the time `Batch.Records` is returned from `ReadBatch`, every value is a plain Go type
that any target adapter can handle. The Mongo adapter reads the schema plan to know what
type each field should become and applies the mapping:

```
primitive.ObjectID  → string  (hex)
primitive.DateTime  → time.Time
primitive.Decimal128 → string  (then target parses as numeric)
bson.D / bson.M     → json.Marshal → []byte  (target receives as jsonb)
primitive.A (array) → json.Marshal → []byte
int32               → int64
int64               → int64
float64             → float64
bool                → bool
[]byte              → []byte
nil                 → nil
```

Fields with strategy `skip` are omitted from the record map entirely.
Fields with strategy `flatten` are expanded: `{"address": {"city":"NYC","zip":"10001"}}`
becomes `{"address_city": "NYC", "address_zip": "10001"}` in the record map.

### Segment Workers

For the first implementation: **sequential reads only** (no parallel PK-range splits).

ObjectId range splitting is possible in theory (hex-sortable min/max split) but the
boundaries are time-based, not data-distribution-based, which produces uneven segments
for older collections. Deferred to a follow-up.

The OFFSET fallback also does not apply — MongoDB OFFSET is a full collection scan and
performs poorly on large collections. We use the keyset cursor exclusively.

---

## Phase 3 — Post-Schema (Indexes)

After all documents are transferred, create indexes on the target Postgres table.

Source: the Mongo source adapter's `GetSchema` (reading from the schema plan) returns a
`TableSchema` with an `Indexes []IndexDef` field populated from MongoDB's `listIndexes`
output — translated to Postgres index definitions.

MongoDB → Postgres index translation:

| MongoDB index type       | Postgres equivalent        | Notes |
|--------------------------|---------------------------|-------|
| Single field ascending   | `CREATE INDEX ... (col ASC)` | Direct |
| Single field descending  | `CREATE INDEX ... (col DESC)` | Direct |
| Compound                 | `CREATE INDEX ... (col1, col2)` | Direct |
| Unique                   | `CREATE UNIQUE INDEX ...` | Direct |
| Text index               | `CREATE INDEX ... USING gin(to_tsvector(...))` | Approximate |
| 2dsphere / geo           | Skip + warn | No Postgres equivalent without PostGIS |
| TTL index                | Skip + warn | Postgres has no native TTL |
| Hashed                   | Skip + warn | No equivalent |
| Wildcard                 | Skip + warn | No equivalent |
| Partial                  | `CREATE INDEX ... WHERE ...` | If filter expression is simple equality |

Indexes on skipped fields or flattened-but-absent columns are automatically skipped.
The analysis output warns the user about any indexes that cannot be translated.

---

## AI Integration

The existing `analyzer/ai/` stub is extended for Mongo→Postgres schema analysis.

### What the AI receives

Per collection, a structured prompt containing:
- Collection name and estimated document count
- Field frequency table (name, coverage %, observed BSON types)
- Two or three anonymized sample documents (actual field structure, values redacted)
- Current inferred type decisions and any ambiguous fields flagged

### What the AI returns

Per field (especially ambiguous ones):
- Recommended strategy and type
- One-sentence reasoning

Example AI annotation:

```
total: recommend numeric over double precision.
  3% of documents use Decimal128, which indicates monetary values. Using
  double precision would introduce floating-point rounding on those rows.
  numeric is exact for both Double and Decimal128.

address: recommend jsonb over flatten.
  The nested document has 11 observed subfields with only 60% average coverage.
  Flattening produces 11 nullable columns, most of which most queries will not
  touch. jsonb preserves the structure and lets you query selectively.

metadata: recommend skip.
  Only present in 41% of documents with no consistent subfield structure across
  the sample. Likely application-internal state. Confirm with the team before
  including.
```

### AI is optional

If no AI provider is configured, analysis runs rule-based only. The output is the same
structure; the `AIReasoning` fields are empty.

---

## New Packages

```
adapters/mongo/
    source.go         Connect, Close, Ping, ListTables, GetSchema, GetRowCount,
                      ReadBatch, CheckPermissions
    convert.go        BSON → Go type conversion using schema plan
    checkpoint.go     _id encode/decode for checkpoint storage

analyzer/mongo/
    sampler.go        Connect to MongoDB, run $sample aggregation, build frequency table
    infer.go          Frequency table → field options + recommended TableSchema
    translate.go      MongoDB index definitions → Postgres IndexDef equivalents
    ai.go             Feed inference results to AI provider, merge recommendations

state/
    schema_plan.go    CRUD: SavePlan, GetPlan, DeletePlan, ListPlanCollections
```

### Changes to existing packages

```
registry/registry.go        Register "mongodb" source adapter; add mongodb:// DSN parser
state/metadb.go             Add schema_plans table to migration; add schema_plan.go CRUD
api/handlers/projects.go    Extend /analyze for mongo source; add /schema-plan endpoints
cmd/xferdb/commands/        Add `project analyze` and `project schema` subcommands
```

---

## New API Endpoints

```
POST   /api/v1/projects/:id/analyze        (enhanced — detects mongo source, runs sampling)
GET    /api/v1/projects/:id/schema-plan    return saved schema plan per collection
PUT    /api/v1/projects/:id/schema-plan    save user's field decisions
DELETE /api/v1/projects/:id/schema-plan    wipe plan (force re-analyze)
```

`POST /analyze` response shape (mongo source):

```json
{
  "type": "mongo_infer",
  "collections": [
    {
      "name": "orders",
      "estimated_count": 1247893,
      "sample_size": 2000,
      "fields": [
        {
          "name": "_id",
          "coverage_pct": 100,
          "bson_types": [{"type": "ObjectId", "count": 2000}],
          "recommended": {"pg_column": "_id", "pg_type": "text", "strategy": "direct", "is_pk": true},
          "options": [{"pg_type": "text", "strategy": "direct"}]
        },
        {
          "name": "total",
          "coverage_pct": 100,
          "bson_types": [{"type": "Double", "count": 1940}, {"type": "Decimal128", "count": 60}],
          "polymorphic": true,
          "recommended": {"pg_column": "total", "pg_type": "numeric", "strategy": "direct"},
          "options": [
            {"pg_type": "numeric",           "strategy": "direct"},
            {"pg_type": "double precision",  "strategy": "direct"},
            {"pg_type": "jsonb",             "strategy": "as_jsonb"}
          ],
          "ai_reasoning": "3% Decimal128 suggests monetary values; numeric is exact for both."
        },
        {
          "name": "address",
          "coverage_pct": 100,
          "bson_types": [{"type": "Document", "count": 2000}],
          "recommended": {"pg_column": "address", "pg_type": "jsonb", "strategy": "as_jsonb"},
          "options": [
            {"pg_type": "jsonb", "strategy": "as_jsonb"},
            {"pg_type": "",      "strategy": "flatten"},
            {"pg_type": "",      "strategy": "skip"}
          ],
          "ai_reasoning": "11 subfields with inconsistent presence; jsonb avoids sparse nullable columns."
        }
      ],
      "index_warnings": [
        "TTL index on `expires_at` cannot be translated to Postgres — skipped",
        "Text index on `description` translated to GIN tsvector approximation"
      ]
    }
  ]
}
```

---

## New CLI Commands

```bash
# Sample collections and infer schema (prints proposed Postgres schema)
xferdb project analyze
xferdb project analyze --sample-size 5000
xferdb project analyze --ai                      # include AI annotations

# Review saved schema plan
xferdb project schema

# Override individual field decisions
xferdb project schema --set orders.address=flatten
xferdb project schema --set orders.metadata=skip
xferdb project schema --set orders.total=numeric

# Reset and re-analyze
xferdb project schema --reset
```

---

## Build Order

Tasks are numbered; each is blocked by the ones listed.

| # | Task | Package | Blocked by |
|---|------|---------|-----------|
| M1 | Schema plan state table + CRUD | `state/schema_plan.go` | — |
| M2 | MongoDB DSN parser | `registry/registry.go` | — |
| M3 | Document sampler + frequency table | `analyzer/mongo/sampler.go` | M2 |
| M4 | Schema inference + field options | `analyzer/mongo/infer.go` | M3 |
| M5 | MongoDB index → Postgres translation | `analyzer/mongo/translate.go` | M4 |
| M6 | AI integration for mongo schema | `analyzer/mongo/ai.go` | M4 |
| M7 | API: extend /analyze + schema-plan endpoints | `api/handlers/` | M1, M4, M6 |
| M8 | CLI: `project analyze` + `project schema` | `cmd/xferdb/commands/` | M7 |
| M9 | BSON → Go type converter | `adapters/mongo/convert.go` | M1 |
| M10 | Mongo source adapter | `adapters/mongo/source.go` | M9, M5 |
| M11 | Register mongo adapter | `registry/registry.go` | M10 |
| M12 | End-to-end test: mongo → postgres | — | M11, M8 |

---

## Open Questions

These are not yet decided and should be confirmed before implementing the affected tasks.

1. **Sample size default** — 2,000 documents is a guess. Does it need to be larger for typical collections? Is it configurable globally in the project config or only per-analyze run?

2. **Flatten depth** — Design says one level only. Do we ever need two levels? Keeping it at one avoids a recursive explosion of columns.

3. **`_id` column rename** — Default keeps `_id` as the column name. Should we default to `id` instead since `_id` is a MongoDB convention that looks odd in Postgres?

4. **Re-analysis after plan exists** — If the user runs `analyze` again after a plan is saved, do we overwrite the plan or require `--reset` first?

5. **Partial plans** — Can the user save a partial plan (some collections decided, others not) and still run a migration for only the decided collections?

6. **AI provider** — Anthropic (Claude) or OpenAI first? The stub supports both. Recommendation: Anthropic since that's the natural fit for this project.

7. **Sample documents in AI prompt** — Should we include actual sample documents (with potential PII) or only the field frequency table? Safest default: frequency table only, with an opt-in flag (`--ai-include-samples`) that the user explicitly enables.
