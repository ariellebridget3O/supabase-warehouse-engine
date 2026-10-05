# API — warehouse-engine v1 wire contract

Every JSON envelope carries `v: 1`. Grounded in `supabase/functions/_shared/wh_entrypoint.ts` (routes/auth/envelopes) + `wh_engine_core.ts` (request parse, success envelope, error map). `qid` echoes the request's `qid` when it is a string, else `null`.

## Consumer contract dependency — agent-fetch-kit

The engine's relayed fetches consume agent-fetch-kit's fleet backend. Consumer
contract surface, pinned as of afk `601f0da` (2026-10-05): `_parse_env_file`
semantics (`lib/fetchkit/config.py`), the `RelayResponse.text()` envelope
(`lib/fetchkit/keypool.py`), and the afk CHANGELOG breaking-change law — consult
the afk CHANGELOG before bumping this pin.

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
| `query.join` | optional join descriptor `{"table":"wh_probe_dim","type":"inner","on":{"left":"region","right":"region"}}` — strictly validated WHEN PRESENT (absent/null ⇒ absent; every request without it parses byte-identically); see §`query.join` below |
| `query.limit` | optional non-negative integer or `null` |
| `query.fetch_rows` | optional positive integer or `null` (r133 — the named alias of `query.limit`); **mutually exclusive with `query.limit`** and the exclusion is PRESENCE-based: any co-presence is `400 malformed` with the fixed string `"query.fetch_rows and query.limit are mutually exclusive"` — `{fetch_rows:25, limit:null}` is a 400 even though `limit:null` alone legally means no-truncation (`0`/negative/non-integer ⇒ `400 malformed` too) |
| `query.fetch_rows` semantics | an **alias of `limit`, not pagination** — it inherits `limit`'s per-shard-`LIMIT` row-selection semantics (each shard returns its FIRST K groups, key-ascending, merged under the ordinary laws), so the merged output is deterministic only while fleet bands are aligned/uniform; there is no offset/keyset continuation (`OFFSET > 0` stays `409 page_unavailable`) |
| `having` / `orderBy` / `offset` / `distinct` | **neutral values dropped; non-neutral rejected 400** (e.g. `OFFSET > 0` names the `409 page_unavailable` class — rejected at plan time) |
| `read_plane` | exact `primary` (default) or `replica` |
| `min_lsn` | pg_lsn text `/^[0-9A-Fa-f]{1,8}\/[0-9A-Fa-f]{1,8}$/` (no trim); **requires** `read_plane:"replica"`; with the `WH_RYW_V1` lever OFF it is a `400 malformed` naming the lever (`WH_RYW_V1=on`) |
| `column_types` / `column_scales` | optional maps column → string / number; keys must be plain identifiers; violation ⇒ 400 |
| `directory_snapshot` | optional signed snapshot string (snapshot mode; verify chain sig → TTL → version — ANY failure is a silent full embed, never an error) |
| `idempotency_key` / write-batch markers | mark the body a **write plan** — fence gate pre-parse (below) |
| `write_epoch` | additive optional carry, adjudicated by the fence's G-W4 when the fence is open |

### `query.join` — co-located agg-join (r129)

Optional descriptor inside the `query` object (camelCase `groupBy` composes with it — grouped rows shape). Wire grammar (the only join binding live today, template class W6):

```json
{
  "table": "wh_probe_dim",
  "type": "inner",
  "on": { "left": "region", "right": "region" }
}
```

Strict parse law (`wh_engine_core.ts:536-583`; every violation a fixed-string `400 malformed`, never an echo of the input):

- `table` — plain identifier (`IDENT_RE`) and must **differ** from the query table (self-join is not v1).
- `type` — the literal `"inner"` only.
- `on` — object with **only** `left`/`right` keys, both plain identifiers. Unknown keys inside `join` OR inside `join.on` are rejected (tighter than the outer query object's ignore, which is untouched).

Plan-time gates (all `400` via the entrypoint ladder — no 500 fall-through):

- **`join_template_required`** — a join request must derive the join-class template (W6, the manifest row carrying the `join` binding) over the `wh_query` RPC plane; non-join-class templates never serve join plans (derivation is join-aware: non-join plans EXCLUDE join-class rows, join plans derive ONLY them).
- **`join_key_mismatch`** — the request's `on.left`/`on.right` must EQUAL the selected template's declared manifest `join.{left,right}` (the template body hardcodes the keys — a wrong bind would silently return a WRONG oracle).
- **`join_not_colocated`** — fail-closed colocation gate: the dim relation must be broadcast-reference (`is_reference === true` on every dim placement — undefined/false ⇒ reject) and EXACTLY ONE serving-or-draining dim placement must exist on every selected ref.

**The bare-count law (col-strict adapter law):** the join plan's row-count agg must be a BARE count — `{"op":"count","alias":"n"}` with **no `col`**. The adapter matches a plan agg to a template encoding only when the (op, col) pair is identical — a bare `count(*)` never matches a col-scoped encoding and vice versa. W6's `n` encoding is bare, so a col-scoped count (e.g. `count(id)`) makes the plan unservable ⇒ every shard returns `envelope_invalid`/excluded ⇒ the request fails 400 (proven live r130).

### Processing order (exact)

1. Body must be a JSON **object** ⇒ else `400 malformed` (`qid: null`).
2. **`hasRealFetcher` is `true` since r118** (`FLIP_hasRealFetcher`, `wh_entrypoint.ts:105`) — `/query` is live and proceeds to the fence + directory work. The historical pre-flip deploy gate (a pinned 500 "real shard fetcher lands after live probes #1/#2" before ANY work) is retired; the flip constant remains the rollback site (flipping is a reviewed code change, never an env).
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
  "perShard": [ {"shard": …, "ok": true, "latencyMs": …, "error": null, "partial_rows": …, "partial_bytes": …} ],
  "latency_ms": 12,
  "phases": { "pre_chain_ms": …, "handshake_ms": …, "fanout_ms": … },
  "directory_snapshot": "<signed, full-embed only, WH_SNAPSHOT_KEY set>"
}
```

- **`phases`** (r121) — `{pre_chain_ms, handshake_ms, fanout_ms}` sub-span timings on **success envelopes only** (omitted on errors). `pre_chain_ms` is the pre-chain wall (fence consult + atomic directory read), `handshake_ms` is `0` on unsampled calls (the inventory GET survives only as a 1-in-16 sampled backstop), `fanout_ms` is the measured fanout block duration.
- **`perShard[]` additive keys (r129)** — each ok-entry additionally carries `partial_rows` (the partial's row count) and `partial_bytes` (the UTF-8 byte length of `JSON.stringify(envelope.partial)` — the serialization convention of record). **Ok-arms only**: error arms carry NEITHER key and stay byte-identical to their pre-r129 shapes.
- **`warnings[]` r138 additions** — two new additive codes joined the per-shard warning grammar (same `{shard, code, est_rows, retried:false}` shape):
  - `row_estimate_mismatch` with the literal shard label `<merged>` — the **C1 row-estimate reconciliation advisory**. Arms IFF the plan is NON-join, `limitK == null` (no `query.limit` / `query.fetch_rows`), the plan carries a bare col-less `count` agg, EVERY dispatched placement answered ok with a usable `row_estimate` (positive integer — 0 = never-statted, disarms), the request's window is RECONCILABLE (every `where` predicate is on the shard key; point/inequality key predicates disarm; a key range window must FULLY cover every dispatched span — a window that slices a span makes the merged count lawfully smaller than Σ), and the merged count_star ≠ Σ row_estimate. Emits EXACTLY ONE warning post-merge; **never fails the request** (rows/partial/coverage untouched — a 200 stays a 200). Silence law: full-coverage unfiltered bare-count waves whose merged count matches the directory estimates emit NOTHING. Disarms under any client clamp (`limit`/`fetch_rows`), under `query.join` (the join plane's completeness law is EXACT-oracle, not row reconciliation), under ANY where filter that breaks span comparability (non-key predicate, point key predicate, span-slicing window — a filtered wave is lawful reduction, never drift), and on any partial wave (a dead shard's missing contribution is an outage echo, already warned per-shard — never estimate drift).
  - `fleet_de_listed` / `coverage_floor_unmet` — the **F-1b re-adjudicated empty-selection verdict + F-2 completeness floor**. The pre-r138 empty/under-selection success shape (`coverage:'0/0'`, `coverage_ratio:1`, `partial:false`, empty rows/scalars) is RE-PINNED: it now carries `partial:true` + ONE `fleet_de_listed` warning (cold-start-after-idle degrades LOUDLY instead of returning a mathematically-honest, semantically-poisonous "complete answer: no data exists").
- **`query.min_shards` (r138, F-2)** — additive OPTIONAL positive integer in the query body; **absent = byte-identical pre-r138 behavior**. Parse-time type violation (present but not an integer ≥ 1) ⇒ `400 malformed` (`"query.min_shards must be a positive integer"`); SEMANTIC enforcement is post-merge: ok shards < `min_shards` ⇒ `partial:true` + ONE `coverage_floor_unmet` warning and the data is STILL returned (READS DEGRADE — never a 4xx on a read).
- **Freshness keeper (r138, F-1a)** — after a fanout where shards answered 2xx, the engine stamps `last_health_at` for the ok-set (stats-only write; the 0013 dv trigger is `after update of state, key_min, key_max, hash_slot, schema_version` — `last_health_at` is EXCLUDED, so the stamp provably never bumps `directory_version`). Failure is log-only and never fails the query. This keeps a serving fleet inside the 90s-fresh directory window under steady traffic; the manual freshness one-liner remains the cold-start instrument.
- `directory_snapshot` is attached **only** on a fresh full embed when `WH_SNAPSHOT_KEY` is set — a valid replay skips the re-attach (the client's copy is still current).

### Error-code map (WhEngineError → HTTP status)

| `error.code` | Status | Meaning |
|---|---|---|
| `malformed` | 400 | request shape/parse violation (incl. all auth 401s above) |
| `capacity_exceeded` | 422 | plan exceeds capacity law |
| `tier_warm` | 404 | no serving directory rows for the table (warm tier) |
| `page_unavailable` | 409 | pagination beyond v0 (e.g. `OFFSET > 0`) |
| `quorum_unmet` | 409 | merge quorum not reached |
| `plan_untemplated` | 400 | plan-honesty 4xx (rpcMode plane only — no template covers the plan) |
| `join_template_required` | 400 | a `query.join` request derived a NON-join-class template — join plans require the join-class template (W6) over the RPC plane |
| `join_key_mismatch` | 400 | the request's `join.on.{left,right}` do not equal the selected join template's declared manifest `join` binding |
| `join_not_colocated` | 400 | colocation gate fail-closed: the dim relation is not broadcast-reference (`is_reference !== true` somewhere) or lacks exactly one serving-or-draining placement on a selected ref |
| anything else | 500 | internal |

Error envelopes carry `directory_version` whenever known, plus `perShard` and `latency_ms` when available:

```json
{"v":1,"qid":…,"directory_version":3,"error":{"code":"…","message":"…"},"perShard":[…],"latency_ms":…}
```

A non-`WhEngineError` throw ⇒ `500 {"v":1,"qid":…,"error":{"code":"internal","message":"internal engine error"}}` — raw internal error text **never** goes on the wire (detail goes to the isolate log).
