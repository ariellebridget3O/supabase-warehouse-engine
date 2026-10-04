# API — warehouse-engine v1 wire contract

Every JSON envelope carries `v: 1`. Grounded in `supabase/functions/_shared/wh_entrypoint.ts` (routes/auth/envelopes) + `wh_engine_core.ts` (request parse, success envelope, error map). `qid` echoes the request's `qid` when it is a string, else `null`.

## Deployment URL shape

```
https://<project-ref>.supabase.co/functions/v1/warehouse-engine/<route>
```

The handler locates the `/warehouse-engine` marker inside `url.pathname` and routes on the remainder; **trailing slashes are stripped** — the one pinned lenient normalization beyond WHATWG. A wrong path shape yields `400 no route`.

## Authentication

- Every route except `OPTIONS *` and `GET /health` requires:
  - `Authorization: Bearer $WHE_BEARER_TOKEN` (timing-safe compare against the fn secret), **and**
  - an `apikey` header (**presence-only** — value validation is the platform gateway's job; the config pins `verify_jwt = false`, so the fn-level bearer check is the gate).
- `OPTIONS` is exempt (browsers cannot attach auth headers on preflights).
- `GET /health` is dispatched **before** `checkAuth` — it is a config-version probe, unauthenticated.

### Auth failure envelopes (all *before* route dispatch)

| Condition | Status | Body |
|---|---|---|
| `WHE_BEARER_TOKEN` secret unset on the deployment | `500` | `{"v":1,"error":{"code":"internal","message":"server has no WHE_BEARER_TOKEN secret set"}}` |
| `Authorization` missing or not `Bearer …` | `401` | `{"v":1,"error":{"code":"malformed","message":"auth rejected before route dispatch (missing_bearer)","auth_kind":"missing_bearer"}}` |
| Bearer present but wrong token | `401` | same shape, `auth_kind: "invalid_token"` |
| `apikey` header missing | `401` | same shape, `auth_kind: "bad_apikey"` |

**`auth_kind` semantics:** the field appears **only** on the three fn-level `401` kinds above (`missing_bearer` / `invalid_token` / `bad_apikey`). It is the discriminator between the function's own auth rejection and a **platform-gateway 401** (JWT verification left on — a bare gateway 401 carries no `auth_kind`). See README rescue line 4.

## Routes

| # | Route | Auth | Response |
|---|---|---|---|
| 1 | `OPTIONS *` (any path) | exempt | `204` with `Access-Control-Allow-Origin: *`, `Allow-Methods: GET,POST,OPTIONS`, `Allow-Headers: Authorization, Content-Type, apikey`, `Max-Age: 86400` |
| 2 | `GET /health` | none | `200 {"v":1,"ok":true,"directory_version":<int>,"engine_build":"<sha7>|null"}` — a cheap config version probe **only** (no directory embed). r124 A8: `engine_build` is the generated build stamp (null when absent; additive — never a 500 over the stamp). Probe failure ⇒ `500 {"v":1,"ok":false,"error":{"code":"internal","message":…}}`. |
| 3 | `POST /query` | yes | see below |
| 4 | anything else | yes | `400 {"v":1,"error":{"code":"malformed","message":"no route <METHOD> <path>"}}` |

## `POST /query`

### Request (JSON object; fields the v1 parse accepts)

| Field | Type/law |
|---|---|
| `qid` | optional string; echoed on every envelope |
| `table` | required; must match `IDENT_RE` `^[a-zA-Z_][a-zA-Z0-9_]*$` |
| `query.select` | aggregate list, ops `sum|count|min|max|avg`, optional `col` + `alias` |
| `query.where` | optional; ops `eq|neq|gt|gte|lt|lte|between|is` |
| `query.groupBy` | optional group keys (grouped ⇒ `rows` response shape; absent ⇒ scalar `result` shape) |
| `query.limit` | optional non-negative integer or `null` |
| `having` / `orderBy` / `offset` / `distinct` | **neutral values dropped; non-neutral rejected 400** (e.g. `OFFSET > 0` names the `409 page_unavailable` class — rejected at plan time) |
| `read_plane` | exact `primary` (default) or `replica` |
| `min_lsn` | pg_lsn text `/^[0-9A-Fa-f]{1,8}\/[0-9A-Fa-f]{1,8}$/` (no trim); **requires** `read_plane:"replica"`; with the `WH_RYW_V1` lever OFF it is a `400 malformed` naming the lever (`WH_RYW_V1=on`) |
| `column_types` / `column_scales` | optional maps column → string / number; keys must be plain identifiers; violation ⇒ 400 |
| `directory_snapshot` | optional signed snapshot string (snapshot mode; verify chain sig → TTL → version — ANY failure is a silent full embed, never an error) |
| `idempotency_key` / write-batch markers | mark the body a **write plan** — fence gate pre-parse (below) |
| `write_epoch` | additive optional carry, adjudicated by the fence's G-W4 when the fence is open |

### Processing order (exact)

1. Body must be a JSON **object** ⇒ else `400 malformed` (`qid: null`).
2. **Deploy gate first:** `hasRealFetcher` is `false` at HEAD (`FLIP_hasRealFetcher`, `wh_entrypoint.ts:102`) ⇒ **every `/query` returns the pinned 500 before any directory work**:
   `{"v":1,"qid":…,"error":{"code":"internal","message":"real shard fetcher lands after live probes #1/#2 (PAT-gated design freeze for the QC2 compile)"}}` — a deployed stub must never serve plausible empty 200s.
3. **Write-plan fence (pre-parse):** fenced ⇒ `503 {"v":1,"qid","error":{"code":"read_only_mode","message":…}}`; an **unwired fence dep** ⇒ `500 internal` (G-W1 fail-closed — the engine cannot vouch the write, and never masquerades unreadable config as 503).
4. Parse gates (§request table) ⇒ `400 malformed` on any violation.
5. RYW lever check (§`min_lsn`).
6. Snapshot replay (if `WH_SNAPSHOT_KEY` set + snapshot supplied), else full directory embed; rows projected to the queried table.
7. Fan-out + merge under coverage/quorum laws (`tier_warm` 404 when no serving rows; `quorum_unmet` 409; `capacity_exceeded` 422).

### Success `200`

The engine envelope (grouped plans carry `rows`, scalar plans carry `result`):

```json
{
  "v": 1,
  "qid": "<echo>",
  "directory_version": 3,
  "coverage": "<coverage token>",
  "coverage_ratio": 1.0,
  "partial": false,
  "rows":    [{"k": [ …group keys… ], "aggs": { "<alias>": … }}],
  "warnings": [ {"shard": …, "code": …, "est_rows": 0, "retried": false} ],
  "perShard": [ … ],
  "latency_ms": 12,
  "directory_snapshot": "<signed, full-embed only, WH_SNAPSHOT_KEY set>"
}
```

`directory_snapshot` is attached **only** on a fresh full embed when `WH_SNAPSHOT_KEY` is set — a valid replay skips the re-attach (the client's copy is still current).

### Error-code map (WhEngineError → HTTP status)

| `error.code` | Status | Meaning |
|---|---|---|
| `malformed` | 400 | request shape/parse violation (incl. all auth 401s above) |
| `capacity_exceeded` | 422 | plan exceeds capacity law |
| `tier_warm` | 404 | no serving directory rows for the table (warm tier) |
| `page_unavailable` | 409 | pagination beyond v0 (e.g. `OFFSET > 0`) |
| `quorum_unmet` | 409 | merge quorum not reached |
| `plan_untemplated` | 400 | plan-honesty 4xx (rpcMode plane only — no template covers the plan) |
| anything else | 500 | internal |

Error envelopes carry `directory_version` whenever known, plus `perShard` and `latency_ms` when available:

```json
{"v":1,"qid":…,"directory_version":3,"error":{"code":"…","message":"…"},"perShard":[…],"latency_ms":…}
```

A non-`WhEngineError` throw ⇒ `500 {"v":1,"qid":…,"error":{"code":"internal","message":"internal engine error"}}` — raw internal error text **never** goes on the wire (detail goes to the isolate log).
