# MongoDB → PostgreSQL Migration — Design Document

---

## Current Status

_Last updated: 2026-09-01. Update this section at the end of every session._

### Milestones

| # | Task | Status | Branch / PR |
|---|------|--------|-------------|
| M0 | Sequential keyset resume (`Batch.LastKey`) | ✅ Merged to main | — |
| M1 | Schema plan state table + CRUD + migration snapshot | ✅ Merged to main | PR #29 |
| M2 | MongoDB DSN scheme detection + registry | ✅ Merged to main | PR #28 |
| M3 | Document sampler + frequency table | 🔄 PR open, needs merge | `feature/m3-mongo-sampler` |
| M4 | Schema inference + field options + name sanitizer | 🔄 PR open, needs merge | `feature/m4-schema-inference` |
| M5 | Index translation (v1 btree subset) | 🔄 PR open, needs merge | `feature/m5-index-translation` |
| M6 | AI integration for mongo schema (optional) | 🔄 PR open, needs merge | `feature/m6-ai-integration` |
| M7 | API: `/analyze` mongo branch + schema-plan endpoints | 🔄 PR open, needs merge | `feature/m7-api-schema-plan` |
| M8 | CLI: `project analyze` + `project schema` | 🔄 PR open, needs merge | `feature/m8-cli-schema-commands` |
| M9 | BSON → Go converter (recursive, no `bson.D` marshal) | 🔄 PR open, needs merge | `feature/m9-bson-converter` |
| M10 | Mongo source adapter + `SetPlan` | 🔄 PR open, needs merge | `feature/m10-mongo-source-adapter` |
| M11 | Register mongo adapter in registry | 🔄 PR open, needs merge | `feature/m11-register-mongo-adapter` |
| M12 | End-to-end test: mongo → postgres | 🔄 PR open, needs merge | `feature/m12-e2e-test` |

### Next session starting point

All milestones are implemented and all branches have been pushed. The remaining
work is merging PRs in dependency order on Gitea:

1. Merge M3 first (sampler, no deps beyond M0–M2 which are on main)
2. Merge M4 (inference, depends on M3)
3. Merge M9 (converter, depends on M1 on main)
4. Merge M5 (index translation, depends on M4)
5. Merge M10 (adapter, depends on M9 + M5)
6. Merge M6 (AI stub, depends on M4)
7. Merge M11 (registry wiring, depends on M10)
8. Merge M7 (API, depends on M11 + M6 + M1)
9. Merge M8 (CLI, depends on M7)
10. Merge M12 (e2e test, depends on M8 + M11)

Each PR's base branch should be the previous milestone's branch so the diff is
scoped. After merging the chain, `main` will have the full MongoDB pipeline.

To run the e2e test once PRs are merged:
```
XFERDB_TEST_MONGO_DSN=mongodb://localhost:27017/testdb \
XFERDB_TEST_PG_DSN="postgres://user:pass@localhost/testdb?sslmode=disable" \
go test ./e2e/... -v -timeout 120s
```

### Process rule

All changes go through feature branches and PRs — **never push directly to main**.

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
plan exists for a collection included in the run.

`analyze` **persists the recommended plan as a draft** immediately. `project schema --set`
only overrides individual fields. The user does not have to `--set` every field before
migrating.

---

## Phase 1 — Schema Analysis

### Document Sampling

Sample N documents per collection using MongoDB's `$sample` aggregation stage:

```
db.collection.aggregate([{ $sample: { size: N } }])
```

Default sample size: **2,000 documents**. Configurable via `--sample-size` on the
`analyze` command only (not a global project setting). Larger samples improve accuracy
at the cost of analysis time. Rare fields the sample never saw are handled at transfer
time (see Runtime extras), not by raising the default N.

Read preference: `secondaryPreferred` for both sampling and `ReadBatch`, so analysis
and migration do not contend with the application's primary.

`ListTables` / sampling skip: `system.*` collections, views, time-series collections,
and GridFS (`*.files` / `*.chunks`). Compound `_id` (document-typed) is out of scope
for v1 — analysis fails that collection with a clear error.

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
The default for any nested document field is `jsonb`. Flatten is one level only — never
recurse. Normalizing arrays-of-documents into child tables is out of scope.

### Column names

Inference sanitizes every `pg_column`:
- Invalid identifier characters → `_`
- Leading digit → prefix `_`
- Truncate to 63 bytes (Postgres identifier limit)
- On collision (`address.city` flatten → `address_city` vs a top-level `address_city`),
  suffix `_2`, `_3`, …

The original Mongo field path is always preserved in `schema_plans.field_name`.

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

The column is always named `_id` in the target by default. Rename to `id` is a
schema-plan override, not the default. `_id` cannot be skipped — it is the resume key.

---

## Schema Plan

### What It Stores

The schema plan is the authoritative record of field decisions for a project. `analyze`
writes the inferred recommendation. User `--set` marks those rows `overridden`.
Re-running `analyze` without `--reset` refreshes non-overridden rows only.

One row per field per collection. A parent with `strategy = flatten` is **not** a
target column: `pg_column` and `pg_type` are empty, and each child key has its own row.

```sql
CREATE TABLE schema_plans (
    project_id   TEXT     NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    collection   TEXT     NOT NULL,
    field_name   TEXT     NOT NULL,   -- dot-notation for nested: "address.city"
    pg_column    TEXT     NOT NULL DEFAULT '',  -- empty when strategy is flatten or skip
    pg_type      TEXT     NOT NULL DEFAULT '',  -- empty when strategy is flatten or skip
    strategy     TEXT     NOT NULL,   -- "direct" | "flatten" | "as_jsonb" | "skip"
    is_pk        BOOLEAN  NOT NULL DEFAULT FALSE,
    nullable     BOOLEAN  NOT NULL DEFAULT TRUE,
    overridden   BOOLEAN  NOT NULL DEFAULT FALSE, -- true after user --set; analyze will not clobber
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (project_id, collection, field_name)
);
```

### Plan injection (required)

`SourceAdapter` only receives `ConnectionConfig` today. The Mongo adapter cannot see
the state DB. The engine loads the plan after `Connect` and injects it:

```go
type PlanConsumer interface {
    SetPlan(plan []state.SchemaPlanRow) error
}
```

`ListTables` / `GetSchema` then return `TableSchema` built from the plan (Postgres column
names and types, plus btree indexes that survived translation), plus a nullable
`_extra jsonb` column that is not a plan row. If a source field is already named
`_extra`, the sanitizer suffixes it (`_extra_2`). Phase 1 `CreateTable` uses that
schema unchanged.

If a collection in the run has no plan rows, the engine fails before Phase 1.

### Frozen copy on the migration

At migration start, the engine copies the project plan onto the migration record
(`migrations.schema_plan JSON`). Resume and `ReadBatch` conversion use the frozen copy,
not the live `schema_plans` table, so a `--set` during a run cannot change conversion
mid-flight.

### Strategies

| Strategy   | Meaning |
|-----------|---------|
| `direct`   | Convert BSON value to the declared `pg_type` and insert as a scalar column |
| `as_jsonb` | Serialize the value (document, array, or scalar) to JSON and store as `jsonb` |
| `flatten`  | Expand the nested document into child columns (one schema_plan row per child key) |
| `skip`     | Exclude this field entirely — not inserted into the target |

---

## Phase 2 — Data Transfer

### Engine change — sequential keyset resume (blocker)

Today `transferTableSequential` always calls `ReadBatch` with `Offset`/`Limit` and
resumes as `offset = rowsTransferred`. Checkpoints have `last_pk TEXT` but the
sequential path never writes or reads it. `Batch.LastPK` is `int64` and is only used
by integer PK-range workers.

Mongo OFFSET is a collection scan. Sequential keyset is not optional for this adapter.

Required engine/adapter type changes:

1. Add `LastKey string` to `Batch` (opaque resume token). Keep `LastPK int64` for
   integer segment workers — do not overload it.
2. Sequential loop: on resume, if the last checkpoint `LastPK` is non-empty, pass it as
   `BatchOptions.LastPK` and do **not** use OFFSET. After each successful write, store
   `batch.LastKey` into `checkpoints.last_pk`.
3. SQL adapters keep current OFFSET sequential behaviour: they leave `LastKey` empty,
   so resume stays offset-based for Postgres/MySQL/SQLite.
4. Mongo `ReadBatch` ignores `Offset` and uses `LastPK` exclusively.
5. `GetPKRange` on Mongo returns "not supported". `_id` as `text` fails
   `detectIntegerPK`, so `--segment-workers` will not take the integer PK path. Table-level
   `--table-workers` still works (collections are independent; no FKs).

This is task **M0**. The Mongo adapter cannot be used without it.

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

Do **not** `json.Marshal` `bson.D` / `bson.M` directly. `bson.D` encodes as
`[{"Key":"city","Value":"NYC"}]`, not `{"city":"NYC"}`. Conversion walks the value and
produces a `map[string]interface{}` (or `[]interface{}` for arrays), converting nested
ObjectId / DateTime / Decimal128 / Binary along the way, then `json.Marshal`s that.

```
primitive.ObjectID   → string (hex)
primitive.DateTime   → time.Time
primitive.Decimal128 → string  (lib/pq sends text; INSERT column type is numeric)
bson.D / bson.M      → recursive map → json.Marshal → []byte
primitive.A          → recursive slice → json.Marshal → []byte
int32                → int64
int64                → int64
float64              → float64
bool                 → bool
[]byte               → []byte
nil                  → nil
```

Unmapped BSON types (MinKey, MaxKey, JavaScript, DBPointer, Timestamp, CodeWithScope)
become `jsonb` via Extended JSON, and analysis warns.

Fields with strategy `skip` are omitted from the record map entirely.
Fields with strategy `flatten` are expanded: `{"address": {"city":"NYC","zip":"10001"}}`
becomes `{"address_city": "NYC", "address_zip": "10001"}` in the record map.

### Runtime extras and type mismatches

Sampling will miss rare fields. Every target table gets an `_extra jsonb` column.

| Situation | Default |
|-----------|---------|
| Field not in the plan | Stash `{field_path: value}` into `_extra`. Do not fail. |
| Planned field, value BSON type does not convert to `pg_type` | `on_error=abort` (default): fail the batch. `on_error=skip`: skip the document. |
| Field in plan with strategy `skip` | Dropped. |

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

`IndexDef` today is `{Name, Columns, Unique}` — no DESC, no `WHERE`, no expression
indexes. v1 only translates what that struct can express:

| MongoDB index type       | v1 behaviour | Notes |
|--------------------------|--------------|-------|
| Single field ascending   | `CREATE INDEX` on the mapped `pg_column` | Direct |
| Compound of mapped columns | `CREATE INDEX (col1, col2)` | Direct |
| Unique                   | `CREATE UNIQUE INDEX` | Warn: Mongo unique-on-missing ≠ Postgres (Postgres allows multiple NULLs) |
| Nested path on a `jsonb` column (`address.city`) | Skip + warn | Needs an expression index `IndexDef` cannot represent |
| Descending               | Create as btree (direction ignored) + warn | `IndexDef` has no DESC |
| Text / 2dsphere / TTL / hashed / wildcard / partial | Skip + warn | Deferred |

Indexes on skipped fields are skipped. Nested-path indexes on `jsonb` columns are
**not** turned into `(col->>'path')` expression indexes in v1.

Text→GIN tsvector and partial-filter translation are explicitly out of v1. Analysis
lists them as warnings so the user can create them by hand.

---

## AI Integration

The existing `analyzer/ai/` stub is extended for Mongo→Postgres schema analysis.

### What the AI receives

Per collection, a structured prompt containing:
- Collection name and estimated document count
- Field frequency table (name, coverage %, observed BSON types)
- Current inferred type decisions and any ambiguous fields flagged
- Sample documents **only** when `--ai-include-samples` is set (values redacted).
  Default prompt is the frequency table only — no PII in the prompt.

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
                      GetPKRange (unsupported), ReadBatch, CheckPermissions, SetPlan
    convert.go        BSON → Go type conversion using schema plan (recursive; no bson.D marshal)
    checkpoint.go     _id encode/decode for checkpoint LastKey

analyzer/mongo/
    sampler.go        Connect to MongoDB, $sample, frequency table
    infer.go          Frequency table → field options + recommended TableSchema + name sanitizer
    translate.go      MongoDB index definitions → btree IndexDef (v1 subset) + warnings
    ai.go             Optional: feed inference to AI provider, merge recommendations

state/
    schema_plan.go    CRUD: SavePlan, GetPlan, DeletePlan, ListPlanCollections
```

### Changes to existing packages

```
engine/table.go             Sequential keyset: pass/save Batch.LastKey (M0)
adapters/adapter.go         Add Batch.LastKey string
registry/registry.go        Detect mongodb:// and mongodb+srv:// scheme; set Type="mongodb";
                            store full URI in ConnectionConfig.DSN unchanged.
                            Register "mongodb" source adapter constructor.
state/metadb.go             schema_plans table; migrations.schema_plan JSON snapshot
api/handlers/projects.go    Branch /analyze for mongo source; schema-plan endpoints
cmd/xferdb/commands/        project analyze + project schema
```

### M2 — Why we detect but do not parse the MongoDB DSN

The existing `ParseDSN` for Postgres and MySQL breaks the URI into individual fields
(`Host`, `Port`, `Database`, `Username`, `Password`, `SSLMode`) because those adapters
reconstruct a driver-specific string from those fields later.

MongoDB connection strings cannot be safely decomposed this way. A typical URI carries
options that have no field in `ConnectionConfig`:

```
mongodb://user:pass@host1:27017,host2:27017/mydb?replicaSet=rs0&authSource=admin&tls=true
mongodb+srv://user:pass@cluster.mongodb.net/mydb?retryWrites=true&w=majority
```

- Multiple hosts (replica set members) — no place in `ConnectionConfig`
- `replicaSet`, `authSource`, `readPreference`, `w`, `tls` — all lost if we only keep
  `Host`/`Port`/`Database`
- `mongodb+srv://` triggers a DNS SRV lookup in the driver — the scheme itself is
  meaningful and must not be rewritten

The MongoDB Go driver (`go.mongodb.org/mongo-driver`) accepts the full URI directly via
`options.Client().ApplyURI(uri)` and handles all of this natively. Parsing it ourselves
would only introduce loss.

`ConnectionConfig` already has a `DSN string` field for exactly this case: "raw DSN;
overrides individual fields when set." The Mongo adapter reads `cfg.DSN` directly and
passes it to `ApplyURI`. No field reconstruction needed.

M2 therefore only needs to:
1. Detect the `mongodb://` or `mongodb+srv://` scheme in `ParseDSN`
2. Set `cfg.Type = "mongodb"` and `cfg.DSN = <full original URI>`
3. Optionally extract `cfg.Database` from the URI path for display in `project list`
4. Register `"mongodb"` in the source adapter registry

`GetRowCount` uses `estimatedDocumentCount` (fast, approximate). Exact counts on large
collections are too expensive for ETA.

---

## New API Endpoints

Existing `POST /analyze` is a source-vs-target **diff** (`Analysis` JSON). Mongo inference
is a different shape. Same URL, branched on source type, with a discriminator so current
clients do not parse the wrong struct:

```
POST   /api/v1/projects/:id/analyze        relational: existing diff
                                       mongo source: sample + infer; persist draft plan
GET    /api/v1/projects/:id/schema-plan    return saved schema plan per collection
PUT    /api/v1/projects/:id/schema-plan    save/override field decisions (sets overridden)
DELETE /api/v1/projects/:id/schema-plan    wipe plan (force re-analyze)
```

Re-analyze without DELETE / `--reset` refreshes inferred rows and leaves `overridden`
rows intact. `DELETE` (CLI `--reset`) wipes the collection's plan first.

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
xferdb project analyze --ai                      # include AI annotations (frequency table only)
xferdb project analyze --ai --ai-include-samples   # opt-in: redacted sample docs in the prompt

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
| M0 | Sequential keyset resume (`Batch.LastKey`) | `engine/table.go`, `adapters/adapter.go` | — |
| M1 | Schema plan state table + CRUD + migration snapshot | `state/schema_plan.go` | — |
| M2 | MongoDB DSN scheme detection + registry registration | `registry/registry.go` | — |
| M3 | Document sampler + frequency table | `analyzer/mongo/sampler.go` | M2 |
| M4 | Schema inference + field options + name sanitizer | `analyzer/mongo/infer.go` | M3 |
| M5 | Index translation (v1 btree subset) + warnings | `analyzer/mongo/translate.go` | M4 |
| M6 | AI integration for mongo schema (optional) | `analyzer/mongo/ai.go` | M4 |
| M7 | API: mongo `/analyze` branch + schema-plan endpoints | `api/handlers/` | M1, M4 |
| M8 | CLI: `project analyze` + `project schema` | `cmd/xferdb/commands/` | M7 |
| M9 | BSON → Go converter (recursive, no `bson.D` marshal) | `adapters/mongo/convert.go` | M1 |
| M10 | Mongo source adapter + `SetPlan` | `adapters/mongo/source.go` | M0, M9 |
| M11 | Register mongo adapter | `registry/registry.go` | M10 |
| M12 | End-to-end test: mongo → postgres | — | M11, M8 |

M5 and M6 are not blockers for M7/M10. Indexes can ship as "none + warnings" and AI can
stay no-op until a provider key is configured.

### Dependency diagram

```
M0  (engine: Batch.LastKey — touches existing code, do first)
     │
     ├─────────────────────────────────────────┐
     │                                         │
M1 (schema_plans table)   M2 (DSN parser)      │
     │                         │               │
     ├──── M9 (converter)      M3 (sampler)    │
     │         │               │               │
     │         └──────── M10 ──┘← M4 (infer)  │
     │              (adapter)   │    ├── M5    │
     │                  │       │    └── M6    │
     │                  │       └──── M7 (API) │
     │                  │              │       │
     │                  └──── M11      M8 (CLI)│
     │                          │       │     │
     └──────────────────────── M12 ────┘─────┘
              (end-to-end test)
```

**Start here:** M0 first (existing code, lowest risk to land early), then M1 and M2 in
parallel, then proceed down the two tracks (analysis pipeline and adapter) concurrently.

---

## Decisions (was: open questions)

1. **Sample size default** — 2,000. Per-run `--sample-size` only.
2. **Flatten depth** — One level. No recursion. No array-of-documents → child tables.
3. **`_id` column name** — Keep `_id`. Rename is an override.
4. **Re-analysis** — Refreshes inferred rows; does not clobber `overridden`. `--reset` / DELETE wipes first.
5. **Partial plans** — Yes. Fail only for collections that are in the run (`--tables` or all) and have no plan.
6. **AI provider** — Whichever key is configured. Do not block M7 on M6.
7. **AI samples** — Frequency table only by default. `--ai-include-samples` is opt-in.
