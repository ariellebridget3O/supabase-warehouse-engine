# DESIGN — architecture of the warehouse-engine extraction

Source of record: supabase-fleet-manager @ `ca4d280`, verified by the r116 audits (`design_audit.md`, `inventory_verify.md`, `consumer_contract.md`). Line references below point into this repo's copied tree.

## 1. Purity island law

The `wh_*` modules are a **pure library island**: no `Deno.env`, no DB client imports, no storage. I/O is **injected** at the shell boundary:

- Every wh_\* module is env-free and DB-free (self-declared and battery-pinned in their headers: `wh_types.ts:11-12`, `wh_handshake.ts:14-15`, `wh_shard_channel.ts:11`).
- Two documented nuances (the law is "no env / no DB", not "no network"):
  - `_shared/wh_entrypoint.ts:205` — the ONE env read inside a `wh_*` module: `checkAuth` reads the bearer secret (`WHE_BEARER_TOKEN`; FM name `FLEET_TOKEN` retired). Handler-side auth is by-design the shell-adjacent exception. (`whe_store.ts` — the non-island seam, §2 — additionally reads the platform-injected `SUPABASE_URL` / `SUPABASE_SERVICE_ROLE_KEY` pair.)
  - `_shared/wh_shard_channel.ts:213` — the default `rawFetch` is the platform `fetch`; injectable, overridable in tests.
- Timers are injected too (`defaultWhEngineTimers`, `wh_engine_core.ts`), so the whole merge/quorum/latency surface is deterministically testable offline.
- **The shell owns the `WH_*` levers**: `warehouse-engine/index.ts` is the only place `WH_SHARD_KEYS`, `WH_SNAPSHOT_KEY`, `WH_RYW_V1`, `WH_REAL_FETCHER` are read; `SUPABASE_URL` / `SUPABASE_SERVICE_ROLE_KEY` are read by the shell *and* by the `whe_store` seam (§2). Each value is threaded into the core as a plain dep (purity law F-N8).

## 2. The consumer-store seam (`whe_store.ts`)

FM's `_shared/supabase-client.ts` is **not copied whole**: it drags `types.ts` and embeds the FM config-JWT-skew gate, config cache, audit and quota helpers — fleet-manager baggage. It is replaced by `_shared/whe_store.ts` (new seam in this repo; its `db()` body is a verbatim excerpt of the FM file — see PROVENANCE), the consumer-store seam:

- `db()` semantics preserved: a single cached supabase-js service-role client built from `SUPABASE_URL` + `SUPABASE_SERVICE_ROLE_KEY` (platform-injected on Supabase), and the store exposes exactly the **6 read sites / 4 surfaces** the shell consumed (table in §6) with result shapes structurally identical to the supabase-js `{data, count, error}` triples the pure readers already consume.
- PostgREST stays the read protocol (supabase-js), **not** a raw pg/pooler client: the geo fence's fail-closed logic depends on PostgREST's jsonb rendering semantics (jsonb numbers arrive as JS numbers — `geo_write_fence.ts:68-71,168-172`), and the battery pins those shapes.
- `WhEngineDeps` already IS the injection seam — the store is a **wiring-side** refactor only; nothing below `wh_entrypoint.ts` changes.
- `whe_store_test.ts` (**NEW**) pins the exact query shapes (paged directory read with `count:'exact'`, ONE combined fence `.in('key', FENCE_CONFIG_KEYS)` round trip, `limit(2)` on `v_geo_directory`) against a recording fake of the supabase builder chain.

## 3. `WhEngineDeps` — the test seam

`wh_entrypoint.ts:104-184` declares every I/O dep optional and injected: `probeDirectoryVersion`, `readDirectory`, `fetcher`, `handshake`, `readGeoDirectory`, `readGeoMode`, `readTableReference`, `fetchFenceConfig`, `ownProjectRef`, `snapshotKey`, `hasRealFetcher`, `rpcMode`, `rywGateEnabled`, `timers`. Consequences:

- The offline battery is **hermetic** ("no DB, no PAT, no network"): tests inject fakes at the dep level; the DB-adjacent logic (pagination, version coercion, ≤1-row tripwire) lives in pure `wh_directory_reader.ts`; the real shell is imported **as text** and never executed by tests.
- Every unwired dep fails **closed or degrades byte-identically** — the gate ladders (G1..G6 read plane, G-W1..W5 write plane) own all fail-closed decisions, never the wiring layer.
- Static source-text pins in `wh_entrypoint_test.ts` require the shell to keep exact wiring expressions (`parseShardKeyEnv(Deno.env.get('WH_SHARD_KEYS'))`, `rpcMode: Deno.env.get('WH_REAL_FETCHER') === 'on'`, `rywGateEnabled: rywGateEnabledFromEnv(Deno.env.get('WH_RYW_V1'))`, `hasRealFetcher: FLIP_hasRealFetcher` with the constant `false`).

## 4. Geo GATE decision — gate, don't parameterize

`FENCE_CONFIG_KEYS = ['geo_read_only','geo_write_epoch','geo_primary_override']` (`geo_write_fence.ts:50`) is a **pinned DB key list**, not a feature flag. The extraction **GATES the geo legs by migration presence** instead of parameterizing the key list, because:

1. **The pins forbid parameterization** — `geo_write_fence_test.ts:158` asserts the exact array; the D28 law pins the fence read as ONE combined query, never widened to a second query. An env-shaped key list would add an env surface to a pure module.
2. **Gating is already implemented for free** — every geo/fence dep is optional and every absent/unwired path fails closed:
   - fence dep **unwired** on a write plan ⇒ `500` G-W1 fail-closed (`wh_entrypoint.ts:272-273`) — a write can never slip ungated, and unreadable config is never masqueraded as 503-or-writable;
   - geo rows **absent** but config readable ⇒ `read_only !== false` ⇒ fenced (`geo_write_fence.ts:165`); epoch absent ⇒ fenced (`:173`) — loud `503 read_only_mode`;
   - `geo_mode` unreadable ⇒ `null` ⇒ replica gate fails closed to primary; `is_reference` unknown ⇒ `null` ⇒ coverage-null replicas fail closed;
   - read dispatch with no override ⇒ `placements` = the pre-r49 behavior, byte-identical.
3. **v0 has no write branch anyway** — the op union (`sum|count|min|max|avg`) contains no mutating ops; the fence's only live trigger is an `idempotency_key`-marked body, which correctly gets 503/500.

**No geo migrations ship in 0.1.0.** The geo control plane (FM `0017_geo_control_plane.sql`: `v_geo_directory`, `geo_mode` seed) and the RYW stamp feed (FM `0022`/`0023`) are deliberately absent: the engine needs **no code change** when they land — apply the migration, the store reads the real rows, the legs activate.

## 5. Deployment topology

- **Engine host** = its own Supabase project (free-tier friendly). Runs the edge function + the consumer-store schema (`db/migrations/0013` catalog + `0014` loader RPCs + `0016` roll-off seal).
- **Shard projects** each run `db/shard-migrations/0015_wh_query_rpc.sql` (the `wh_query` RPC + registry + hash law `WH403`) + shard `0016_seal_roll_off.sql` (`facts_blocks`/`unpack_block`/`facts_events`, seal functions) + the W1–W5 template seed rendered from `db/shard-templates/` per `manifest.json`.
- **Single-project shape** (the default): the engine host IS the only shard — the own-ref arm resolves `SUPABASE_URL`'s subdomain with the engine's own service key, so 0015/0016/seed are applied to the same project. The `db/migrations/` vs `db/shard-migrations/` split exists precisely so project-side migration runners never glob the shard-side 0015 onto the wrong host (0015's header documents the contract).
- **Two partial unique indexes** in 0013 encode the cross-writer placement invariant (storage constraints, not app locks) — the free-tier guardrail instead of extra services.
- `v_warehouse_directory` is the normative routing predicate spelled ONCE server-side; every consumer (engine, loaders, watchdogs) reads this view — no engine-side WHERE drift.

## 6. The 6 DB read surfaces

All reads run through the one consumer-store client (`whe_store.ts`). 6 read sites / 4 surfaces (the census's "4 DB reads" was an undercount):

| # | Surface | Query shape | Consumed by |
|---|---|---|---|
| 1 | `v_warehouse_directory` | `.select('*', {count:'exact'}).range(from,to)` paged at 1000 | directory fan-out targets + snapshot payload |
| 2 | `config` key `warehouse_directory_version` | `.select('value').eq('key',…).maybeSingle()` | `GET /health` + snapshot verify chain |
| 3 | `v_geo_directory` | `.select('*').limit(2)` (≤1-row population; >1 row = law-breakage tripwire) | replica-plane gate ladder G1..G6 |
| 4 | `config` key `geo_mode` | `.select('value').eq('key',…).maybeSingle()` | geo mode gate (null ⇒ fail closed) |
| 5 | `warehouse_tables` `is_reference` | `.select('is_reference').eq('logical_name',…).maybeSingle()` | coverage-null replica fail-closed |
| 6 | `config` fence keys | `.select('key,value').in('key', FENCE_CONFIG_KEYS)` — ONE round trip, 3 keys | write-plane fence G-W1..W5 + legacy read dispatch R3 |

## 7. "Ships disabled" — the flip doctrine

- `FLIP_hasRealFetcher = false` (`wh_entrypoint.ts:102`) is the SINGLE flip site. Pre-flip, `POST /query` answers the pinned 500 **before any directory work** — a deployed stub must never serve plausible-looking empty 200s.
- Flip conditions (pinned in the constant's comment): the real fetcher frozen in (QC2 compile design-frozen and wired), the PAT wall down, and live probes #1/#2/#3 GREEN (#1 platform auth round-trip, #2 directory read, #3 `wh_query` RPC round-trip against a seeded shard).
- Flipping is a **code change on a reviewed diff** — never a deploy-time env. The env levers `WH_REAL_FETCHER` (exact `on` ⇒ rpcMode wire shape) and `WH_RYW_V1` (exact `on` ⇒ RYW `min_lsn` adjudication) compose with the gate but cannot bypass it: pre-flip, `WH_REAL_FETCHER=on` is inert.
- `wh_entrypoint_test.ts` pins the constant `false` by source-text assertion — the battery itself enforces the doctrine.

## 8. r121 OPT-1b runbook note — the fold's failure-surface shift

With the per-query inventory handshake folded into `wh_query` per-call eligibility (r121), unsampled calls surface shard-side transport/auth/429/5xx failures as their honest non-exempt classes ⇒ **fail_fast 500** (the old degrade-200 masking is gone for every non-WH400/401 class). **Rolling deploy: one shard at a time** — a half-deployed fleet shows mixed WH-code vocabulary until every shard runs the 0015 wave. **Key rotation: rotate `WH_SHARD_KEYS` (config) FIRST, then deploy the engine** — a new engine against old shard keys reads as transport failure, not refusal.
