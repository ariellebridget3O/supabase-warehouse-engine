# Changelog

## 0.1.0 — extraction from supabase-fleet-manager @ `ca4d280`

Standalone repo `supabase-warehouse-engine` ("whe"): the warehouse-engine edge function + the `wh_*` library island, extracted from `ariellebridget3O/supabase-fleet-manager` @ `ca4d280` ("r115 fm: add .github/workflows/test.yml"). MIT inherited (see LICENSE + PROVENANCE.md for the per-file ledger).

### Copied verbatim

- **10 `wh_*` modules — 4,630 LOC**: `wh_types.ts` (116), `wh_canonical.ts` (328), `wh_merge.ts` (550), `wh_engine_core.ts` (1,671), `wh_handshake.ts` (488), `wh_snapshot.ts` (212), `wh_shard_channel.ts` (296), `wh_directory_reader.ts` (220), `wh_entrypoint.ts` (543), `wh_testutil.ts` (206) — plus the runtime-required **`geo_write_fence.ts`** (342 LOC, the write fence + `FENCE_CONFIG_KEYS`).
- **10 test files — 7,502 LOC**: the 9 `wh_*_test.ts` files (6,605) + `geo_write_fence_test.ts` (897).
- **Fixtures — 3,144 LOC**: `wh_fixtures/{E1_groupby,E2_avg,E7_min,E12_groupkeys,E14_empty}.json`.
- **Shell**: `warehouse-engine/index.ts` (185 LOC, r40 thin-shell extraction).
- **Migrations**: engine-host `db/migrations/{0013_warehouse_catalog,0014_loader_rpc,0016_rolloff_seal}.sql` + shard-side `db/shard-migrations/{0015_wh_query_rpc,0016_seal_roll_off}.sql`.
- **Templates**: `db/shard-templates/{W1..W5}.sql` + `manifest.json` (sha256-pinned body-of-record) — W1–W5.
- **Scripts**: `render_wh_seed_wave.py` (241 LOC), `lint_shard_templates.py` (518 LOC).

### Renamed / replaced

- **`FLEET_TOKEN` → `WHE_BEARER_TOKEN`** — the FM bearer env name is retired (the bearer is this repo's own credential; checkAuth is the rename site, with its fail-closed 500 message). `WH_SNAPSHOT_KEY` / `WH_SHARD_KEYS` / `WH_RYW_V1` / `WH_REAL_FETCHER` keep their names (already namespaced, test-pinned verbatim).
- **`supabase-client.ts` → `whe_store.ts`** — the consumer-store seam (`db()` semantics preserved; FM's config-JWT-skew gate, config cache, audit and quota helpers deliberately NOT carried).
- **`supabase/config.toml`** now pins `[functions.warehouse-engine] verify_jwt = false` (FM's config had no engine entry — the shape was carried only by the deploy flag).

### Posture

- **Geo gated fail-closed**: no geo migrations ship in 0.1.0 (FM `0017` control plane + `0022`/`0023` stamp feed stay behind). Unwired fence ⇒ `500` on write plans; absent geo rows ⇒ `503 read_only_mode`; replica deps fail closed to primary. The legs activate on migration presence — no code change.
- **Ships disabled**: `FLIP_hasRealFetcher = false`; every `POST /query` answers the pinned pre-flip 500 ("real shard fetcher lands after live probes #1/#2") before any directory work. `GET /health` is live once 0013 is applied. Flipping is a reviewed code change, never an env.

### Known gaps (targeted v0.1.1)

- `migrate.sh` (Management-API SQL runner + `sql_split.awk`) and `templates_test.ts` land in v0.1.1 — apply migrations via psql/SQL editor for now.
- Geo control plane + RYW stamp feed: later rounds (see DESIGN.md §4).
- `deno check` of the engine entrypoint had **never** run in FM — this repo's CI now type-checks it; first green is the extraction-round verification.

### New (no FM provenance)

README, API, DESIGN, DEPLOY, CHANGELOG, PROVENANCE, LICENSE (MIT, dual copyright), `.github/workflows/test.yml`, `Makefile`, `scripts/run-tests.mjs`, `deno.json`, `supabase/config.toml`, `_shared/whe_store.ts` + `_shared/whe_store_test.ts`.
