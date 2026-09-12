# XferDB Configuration File Reference

XferDB reads an optional structured config file (`xferdb.yaml` or `xferdb.json`)
so server settings, migration defaults, and named projects can live in version
control instead of on the command line. This document is the field-by-field
reference; `examples/xferdb.yaml` is the same content as a runnable, annotated
example.

TOML is documented (`examples/xferdb.toml`) but not yet parseable — tracked
for v0.6.1. Pointing `--config` at a `.toml` file returns an error explaining
how to convert it in the meantime.

## Locating the file

First match wins:

1. `--config /path/to/xferdb.yaml`
2. `$XFERDB_CONFIG`
3. `./xferdb.{yaml,yml,json}` in the current working directory
4. `~/.xferdb/config.{yaml,yml,json}`

No config file is not an error — XferDB falls back to CLI flags and built-in
defaults.

## Precedence

For every value a config file can supply, highest wins:

1. CLI flag (e.g. `--table-workers=4`), only when explicitly passed
2. Environment variable (e.g. `XFERDB_TABLE_WORKERS=4`)
3. Config file value
4. Built-in default

This applies to `defaults.*` (consumed by `migrate` and `project create`) and
to a project's own `transfer.*` block.

## Environment variable expansion

DSN, username, and password fields support `${VAR}` and `${VAR:-default}`
substitution from the process environment, expanded when the file is loaded —
so credentials never have to live in the file itself.

| Form | Behavior |
| --- | --- |
| `${VAR}` | Value of `VAR`. Load fails with a clear error if `VAR` is unset. |
| `${VAR:-default}` | Value of `VAR` if set and non-empty, else `default`. |
| `$VAR` (bare, no braces) | Value of `VAR`, or empty string if unset. No error. |

The bracketed/bare distinction is intentional: `${VAR}` is an explicit "this
is required" assertion, so it fails loudly; a bare `$VAR` degrades quietly.

## Top-level structure

```yaml
server:    # xferdb server settings
defaults:  # applied by `migrate` / `project create` before flags
projects:  # named projects, an alternative to `project create`
```

### `server`

| Field | Type | Default | Notes |
| --- | --- | --- | --- |
| `addr` | string | `:8080` | Listen address for `xferdb server`. `:PORT` or `HOST:PORT`. |
| `log_level` | string | `info` | `debug`, `info`, `warn`, or `error`. |
| `log_file` | string | _(none)_ | Path to a JSON log file; stderr text output continues either way. |
| `state_dir` | string | `~/.xferdb` | Where the state database lives. |

### `defaults`

Consumed by `migrate` and `project create` via `config.ResolveTransferConfig`,
before CLI flags are applied.

| Field | Type | Env override | Notes |
| --- | --- | --- | --- |
| `table_workers` | int | `XFERDB_TABLE_WORKERS` | Tables migrated concurrently. `0`/unset means "don't override". |
| `segment_workers` | int | `XFERDB_SEGMENT_WORKERS` | Workers per table, splitting by PK range. |
| `batch_size` | int | `XFERDB_BATCH_SIZE` | Rows per batch. `0`/unset defers to the project's own default. |
| `bulk_copy` | bool | `XFERDB_BULK_COPY` | Use the PostgreSQL `COPY` protocol when supported. |
| `async_pipeline` | bool | `XFERDB_ASYNC_PIPELINE` | Overlap reads and writes. |
| `on_error` | string | — | `abort` or `skip`. |
| `tables` | []string | — | Limit migrations to specific tables. Empty means "all tables". |

The env vars above are read as `XFERDB_<NAME>`: unset means "don't override";
`true`/`1`/`yes`/`on` and `false`/`0`/`no`/`off` (case-insensitive) both count
as explicitly set, so `XFERDB_BULK_COPY=false` can turn off a config-file
`bulk_copy: true` — a bare bool couldn't tell "unset" from "false".

### `projects[]`

An alternative to `xferdb project create`. A project referenced by
`--project <name>` that isn't found on the server but is defined here is
created automatically — no separate `create`/`use` step needed.

| Field | Type | Notes |
| --- | --- | --- |
| `name` | string | Required, must be unique within the file. |
| `description` | string | Optional. |
| `source` / `target` | connection config | See below. |
| `transfer` | transfer config | Same fields as `defaults`, scoped to this project. |

**Connection config** (`source:` / `target:`):

| Field | Notes |
| --- | --- |
| `type` | `postgres`, `mysql`, `mongodb`, or `sqlite`. Optional if `dsn` already implies it. |
| `dsn` | Full connection string. Supports `${VAR}` expansion. Overrides the individual fields below if set. |
| `host`, `port`, `database`, `username`, `password`, `ssl_mode` | Used when `dsn` isn't set. `username`/`password` also support `${VAR}` expansion. |

## CLI

```bash
xferdb config init [path]        # write a starter xferdb.yaml (default ./xferdb.yaml)
xferdb config validate           # exit 0 if the resolved config is valid, 1 otherwise
xferdb config show               # print the resolved config file
xferdb config path                # print the resolved config file's path
```

All four accept `--config /path/to/file` (or `$XFERDB_CONFIG`) like every
other command.

## Example

See [`examples/xferdb.yaml`](../examples/xferdb.yaml) (fully annotated,
loadable) and [`examples/xferdb.json`](../examples/xferdb.json) (JSON,
equivalent content). [`examples/xferdb.toml`](../examples/xferdb.toml) shows
the same structure in TOML for reference, ahead of TOML support landing.
