# Changelog

## Unreleased — r121 OPT-1b: handshake fold (core leg)

- **Engine core**: the per-query inventory handshake is retired from the critical path — eligibility is enforced shard-side per `wh_query` call (WH400/WH401 → `template_missing` at `classifyFetchFailure`, `detail` kept, `est_rows` re-attached from the directory row); the inventory GET survives as a 1-in-16 SAMPLED backstop (`K_SAMPLING = 7`, `sha256(qid utf8)[0] & 15` bucket, Web Crypto) with today's fail-closed exclusion semantics when fired.
- **max_rows F15 goes manifest-side**: a plan limit strictly above the matched template's `ENGINE_TEMPLATE_MANIFEST.max_rows` refuses pre-fanout with ZERO shard POSTs (`==` passes; the sampled inventory `max_rows` column remains the registry-drift signal).
- **`phases` response field**: `{pre_chain_ms, handshake_ms, fanout_ms}` on success envelopes only (omitted on errors) — `pre_chain_ms` is entrypoint-threaded (`ExecuteArgs.timings`), `handshake_ms` is 0 on unsampled calls, `fanout_ms` is the measured fanout block duration.
- Accepted per design §1.3a: WH402/WH403/transport/429/5xx classes are non-exempt under `fail_fast` (honest 500 on unsampled calls) — rolling-deploy/key-rotation runbook note in DESIGN.md §8.
- **Battery re-pin (Leg 2, design §5 D6 ledger)**: 571 → 582 green — 9 inverted pins re-written to the folded observables (LETHAL 1/1b/2/3/3b/7, fail_fast-exemption, fleet/order pins across wh_handshake/wh_geo_plane/wh_shard_channel/wh_entrypoint tests), 10 lethal tests added (full mapped-refusal record + fail_fast twin, manifest max_rows 1001/1000-boundary, sampler determinism vectors + recording-fake + statelessness + `K_SAMPLING` identity, phases incl. absent-on-error, empty-hash guard, unmapped WH402/WH403/5xx fail_fast, F2-clamp-under-drift) with mutation RED-proofs (M1: mapping arm deleted → 3 files RED; M2: sampler predicate hard-false → 3 files RED); fence `stable()` strips the new `phases` timing field alongside `latency_ms`. Engine typecheck green (`make check`).

## 0.1.1 — migrate surface: adapted Management-API runner + lethal pins + docs retraction

### Added

- **`scripts/migrate.sh`** — the Management-API SQL runner, **adapted (NOT verbatim)** from `supabase-fleet-manager` @ `ca4d280` (1,763 LOC → 959; see PROVENANCE.md for the sanctioned-delta ledger): engine verify subset **0013/0014/0016**, `--shard` mode (retargets the glob to `db/shard-migrations/`, makes the directory guard mode-conditional, skips the engine-scoped `verify_migrations`), `--dry-run` (zero network/env), PAT-only `mgmt_query` (one statement per `POST /v1/projects/{ref}/database/query`, fail-fast with file/statement/snippet, transient 429/5xx/network retried ×3 with numeric `Retry-After` honored), `WHE_PROJECT_REF` env (the `FLEET_PROJECT_REF` name retired; `FLEET_TOKEN` gone — negative-pinned in tests).
- **`scripts/sql_split.awk`** — the POSIX-awk statement splitter port (RS `0x1e`; the byte-level twin of the runner's `read -d $'\x1e'` consumption), 135 LOC.
- **Makefile**: `migrate-dry` (zero-network runner rehearsal; runner exit code propagates) + `ci` (check + test + lint-templates + migrate-dry).
- **`supabase/functions/_shared/templates_test.ts`** — the fm harness (module-level passed/failed ledger + `eq()` JSON-compare + terminal `__report__`) ported with 36 lethal pins: verify-gate probes incl. the `pg_get_expr(pi.indpred, pi.indrelid)` 42703-regression killer, the new `0013+0014+0016 engine artifacts present` ok() summary, BOTH `collect_migrations` shard-guard dies + the mode-conditional guard, engine-default-glob confinement (`db/migrations` default must not end in `shard-migrations`), env contract (≥2 `WHE_PROJECT_REF`, `SUPABASE_ACCESS_TOKEN`) + the negative fm set (`FLEET_PROJECT_REF` / `FLEET_TOKEN` / `try_start_rotation` / `fleet_promote` / `edge_proxy_sql_0011` / `0001-0017 artifacts present`), migration-file identity fragments per tree, the `sql_split.awk` RS contract — plus the **manifest sha256 recompute** (each of W1–W5's file bytes hashed via `crypto.subtle.digest` and eq'd to `manifest.json`'s `template_hash`, pulling `lint_shard_templates.py`'s body-of-record invariant inside CI). Battery: 562 → 571 (9 new test cases, 0 failed).
- **Docs retraction (8 sites)**: README quickstart step 2 + known-gaps and DEPLOY §2/§3 now make `scripts/migrate.sh` the PRIMARY apply path (engine, `--shard`, `--file` for the rendered seed wave; `--verify-only` for a fallback-path catalog sweep) with psql/SQL editor demoted to documented fallback; the v0.1.0 "migrate.sh lands in v0.1.1" gaps retired; the engine-host **base-schema prerequisite** (`public.projects` / `public.orgs` / `public.config`) documented; PROVENANCE.md ledger updated.

### Fixed

- **Inherited-bug fix**: the extraction had dropped the source's `set -euo pipefail` on the standalone entry and the `mgmt_query` curl retry arm (429/5xx/network ×3 with Retry-After handling) — both restored in the landed runner, byte-equal in semantics to fm @ `ca4d280`.

## 0.1.0 — extraction from supabase-fleet-manager @ `ca4d280`

Standalone repo `supabase-warehouse-engine` ("whe"): the warehouse-engine edge function + the `wh_*` library island, extracted from `ariellebridget3O/supabase-fleet-manager` @ `ca4d280` ("r115 fm: add .github/workflows/test.yml"). MIT inherited (see LICENSE + PROVENANCE.md for the per-file ledger).

### Copied (verbatim unless noted)

- **10 `wh_*` modules — 4,637 LOC**: `wh_types.ts` (116), `wh_canonical.ts` (328), `wh_merge.ts` (550), `wh_engine_core.ts` (1,678 — diverged post-extraction from `ca4d280`'s 1,671: raw-wire `envelope: unknown` widening + narrow relay cast, see PROVENANCE), `wh_handshake.ts` (488), `wh_snapshot.ts` (212), `wh_shard_channel.ts` (296), `wh_directory_reader.ts` (220), `wh_entrypoint.ts` (543), `wh_testutil.ts` (206) — plus the runtime-required **`geo_write_fence.ts`** (342 LOC, the write fence + `FENCE_CONFIG_KEYS`). All byte-identical to `ca4d280` except the `FLEET_TOKEN` rename sites and `wh_engine_core.ts`.
- **10 test files — 7,502 LOC**: the 9 `wh_*_test.ts` files (6,605) + `geo_write_fence_test.ts` (897).
- **Fixtures — 3,144 LOC**: `wh_fixtures/{E1_groupby,E2_avg,E7_min,E12_groupkeys,E14_empty}.json`.
- **Shell**: `warehouse-engine/index.ts` (185 LOC, r40 thin-shell extraction).
- **Migrations**: engine-host `db/migrations/{0013_warehouse_catalog,0014_loader_rpc,0016_rolloff_seal}.sql` + shard-side `db/shard-migrations/{0015_wh_query_rpc,0016_seal_roll_off}.sql`.
- **Templates**: `db/shard-templates/{W1..W5}.sql` + `manifest.json` (sha256-pinned body-of-record) — W1–W5.
- **Scripts**: `render_wh_seed_wave.py` (241 LOC), `lint_shard_templates.py` (518 LOC).

### Renamed / replaced

- **`FLEET_TOKEN` → `WHE_BEARER_TOKEN`** — the FM bearer env name is retired (the bearer is this repo's own credential; checkAuth is the rename site, with its fail-closed 500 message). `WH_SNAPSHOT_KEY` / `WH_SHARD_KEYS` / `WH_RYW_V1` / `WH_REAL_FETCHER` keep their names (already namespaced, test-pinned verbatim).
- **`supabase-client.ts` → `whe_store.ts`** — the consumer-store seam (`db()` body a verbatim excerpt; semantics preserved; FM's config-JWT-skew gate, config cache, audit and quota helpers deliberately NOT carried).
- **`supabase/config.toml`** now pins `[functions.warehouse-engine] verify_jwt = false` (FM's config had no engine entry — the shape was carried only by the deploy flag).

### Posture

- **Geo gated fail-closed**: no geo migrations ship in 0.1.0 (FM `0017` control plane + `0022`/`0023` stamp feed stay behind). Unwired fence ⇒ `500` on write plans; absent geo rows ⇒ `503 read_only_mode`; replica deps fail closed to primary. The legs activate on migration presence — no code change.
- **Ships disabled**: `FLIP_hasRealFetcher = false`; every `POST /query` answers the pinned pre-flip 500 ("real shard fetcher lands after live probes #1/#2") before any directory work. `GET /health` is live once 0013 is applied. Flipping is a reviewed code change, never an env.

### Known gaps (targeted v0.1.1)

- `migrate.sh` (Management-API SQL runner + `sql_split.awk`) and `templates_test.ts` — **delivered in 0.1.1** (see the 0.1.1 entry above); `psql`/SQL editor remain documented fallbacks.
- Geo control plane + RYW stamp feed: later rounds (see DESIGN.md §4).
- `deno check` of the engine entrypoint had **never** run in FM — this repo's CI now type-checks it; first green is the extraction-round verification.

### New (no FM provenance)

README, API, DESIGN, DEPLOY, CHANGELOG, PROVENANCE, LICENSE (MIT, dual copyright), `.github/workflows/test.yml`, `Makefile`, `scripts/run-tests.mjs`, `deno.json`, `supabase/config.toml`, `_shared/whe_store_test.ts`.
