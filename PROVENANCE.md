# PROVENANCE — copy ledger

Every copied file originated in **`supabase-fleet-manager` @ `ca4d280`** (`main`, tree clean; origin `github.com/ariellebridget3O/supabase-fleet-manager`). Verbatim copies keep byte-diffability with the source repo (future `wh_*` fixes can be re-synced by diff). Byte-diff status (md5 per file against `ca4d280`): every ledgered file is byte-identical EXCEPT (i) the `FLEET_TOKEN`→`WHE_BEARER_TOKEN` rename sites, (ii) `wh_engine_core.ts`, and (iii) `scripts/migrate.sh` — the v0.1.1 ADAPTED extraction detailed below (`scripts/sql_split.awk` is a verbatim port). All three are detailed below. Files marked **NEW** have no FM provenance.

## License inheritance

The source repo is **MIT** (`supabase-fleet-manager/LICENSE`: "Copyright (c) 2026 Supabase Fleet Manager Contributors"). This repo is MIT; `LICENSE` preserves the FM copyright line verbatim **and** adds the new line — MIT §2 requires the copyright + license notice be preserved for substantial portions. Attribution is met by LICENSE retention + this ledger.

## Ledger — copied (source path @ `ca4d280`)

### Runtime modules (verbatim)

| This repo | Source (FM) @ `ca4d280` | LOC |
|---|---|---|
| `supabase/functions/_shared/wh_types.ts` | same path | 116 |
| `supabase/functions/_shared/wh_canonical.ts` | same path | 328 |
| `supabase/functions/_shared/wh_merge.ts` | same path | 550 |
| `supabase/functions/_shared/wh_engine_core.ts` | same path — **NOT verbatim** (diverged post-extraction, +7 lines vs source's 1,671): ok-arm `envelope` widened to the raw wire `unknown` (AM-1/F-N7), `Omit<WhPartialEnvelope,'shard'>` partial-return, narrow relay cast at the raced fan-out relay | 1,678 |
| `supabase/functions/_shared/wh_handshake.ts` | same path | 488 |
| `supabase/functions/_shared/wh_snapshot.ts` | same path | 212 |
| `supabase/functions/_shared/wh_shard_channel.ts` | same path | 296 |
| `supabase/functions/_shared/wh_directory_reader.ts` | same path | 220 |
| `supabase/functions/_shared/wh_entrypoint.ts` | same path | 543 |
| `supabase/functions/_shared/wh_testutil.ts` | same path (test helper; copied, not moved) | 206 |
| `supabase/functions/_shared/geo_write_fence.ts` | same path (not a `wh_*` file; runtime-required by wh_entrypoint + shell) | 342 |

Total runtime island: 4,979 LOC (10 `wh_*` = 4,637 + fence 342). The 4,630/4,972 figures seen in earlier drafts were the @`ca4d280` numbers before the `wh_engine_core.ts` divergence.

Rename sites (functional rename only; comments elsewhere): `wh_entrypoint.ts:205` is the env-name line — the same token rename also touches comments in `wh_snapshot.ts` / `wh_entrypoint.ts` and strings in `wh_entrypoint_test.ts` / `geo_write_fence_test.ts`; `wh_entrypoint_test.ts` additionally drops FM's `fleet-api` source-text pins (that surface stayed with FM). The shell `warehouse-engine/index.ts` differs from its @`ca4d280` counterpart by exactly 2 lines (the `whe_store` import + one rename comment).

### Tests + fixtures (verbatim; rename-touched files listed above)

| This repo | Source (FM) @ `ca4d280` | LOC |
|---|---|---|
| `supabase/functions/_shared/wh_canonical_test.ts` | same path | 297 |
| `supabase/functions/_shared/wh_differential_test.ts` | same path | 312 |
| `supabase/functions/_shared/wh_engine_core_test.ts` | same path | 932 |
| `supabase/functions/_shared/wh_entrypoint_test.ts` | same path | 940 |
| `supabase/functions/_shared/wh_geo_plane_test.ts` | same path | 900 |
| `supabase/functions/_shared/wh_handshake_test.ts` | same path | 901 |
| `supabase/functions/_shared/wh_merge_test.ts` | same path | 681 |
| `supabase/functions/_shared/wh_shard_channel_test.ts` | same path | 1,430 |
| `supabase/functions/_shared/wh_snapshot_test.ts` | same path | 212 |
| `supabase/functions/_shared/geo_write_fence_test.ts` | same path | 897 |
| `supabase/functions/_shared/wh_fixtures/{E1_groupby,E2_avg,E7_min,E12_groupkeys,E14_empty}.json` | same paths | 3,144 |

### Shell (verbatim at extraction; the `whe_store` seam rewrites only its store wiring)

| This repo | Source (FM) @ `ca4d280` | LOC |
|---|---|---|
| `supabase/functions/warehouse-engine/index.ts` | same path | 185 |

### DB + scripts (verbatim)

| This repo | Source (FM) @ `ca4d280` | LOC |
|---|---|---|
| `db/migrations/0013_warehouse_catalog.sql` | same path | 359 |
| `db/migrations/0014_loader_rpc.sql` | same path | 317 |
| `db/migrations/0016_rolloff_seal.sql` | same path | 352 |
| `db/shard-migrations/0015_wh_query_rpc.sql` | same path | 662 |
| `db/shard-migrations/0016_seal_roll_off.sql` | same path | 475 |
| `db/shard-templates/{W1_grouped_sum_count,W2_scalar_minmax,W3_scalar_avg_pair,W4_topk,W5_cold_agg}.sql` | same paths | 38 |
| `db/shard-templates/manifest.json` | same path | 170 |
| `scripts/render_wh_seed_wave.py` | same path | 241 |
| `scripts/lint_shard_templates.py` | same path | 518 |

The `db/migrations/` vs `db/shard-migrations/` split is load-bearing (0015's header: the project-side runner globs `db/migrations/*.sql`; shard files must never be globbed onto the engine host). The W1–W5 template files and `manifest.json` are **sha256-pinned bodies-of-record** — never reformat (the battery's `templates_test.ts` recomputes each hash from the file bytes).

### Shell scripts — v0.1.1 adapted extraction (NOT verbatim)

| This repo | Source (FM) @ `ca4d280` | LOC |
|---|---|---|
| `scripts/migrate.sh` | same path | 1,763 → 959 (adapted) |
| `scripts/sql_split.awk` | same path | 135 (verbatim port) |

`scripts/sql_split.awk` is a byte-identical port. `scripts/migrate.sh` is an **ADAPTED extraction — NOT verbatim** — with these sanctioned deltas:

- env rename `FLEET_PROJECT_REF` → `WHE_PROJECT_REF`; `FLEET_TOKEN` retired — `mgmt_query` is PAT-only (`SUPABASE_ACCESS_TOKEN`),
- `mktemp` prefixes `fleet-*` → `whe-*`,
- verify gate set pruned to the engine subset **0013/0014/0016** (FM-only gate columns deleted — no 0011 edge-proxy, no rotation/fleet_promote probes); the ok() summary rewritten to `0013+0014+0016 engine artifacts present`,
- FM-only die blocks deleted; 4 die texts reworded to engine-host phrasing,
- the pg_cron warn block deleted (`cron.job` probes are an FM-cron concern),
- `--shard` mode added: retargets the glob to `db/shard-migrations/` (a pre-exported `MIGRATIONS_DIR` still wins), makes the `collect_migrations()` directory guard MODE-CONDITIONAL (dies on a shard-tree override in engine mode only), and SKIPS the engine-scoped `verify_migrations` in shard mode,
- bug fix restored: `set -euo pipefail` on the standalone entry + the `mgmt_query` curl retry arm (429/5xx/network ×3, numeric Retry-After honored) — the extraction draft had dropped them; the landed runner restores fm @ `ca4d280` semantics with ONE hardening beyond the donor: the curl invocation is set-e-safe (`curl_rc=0; code="$(curl …)" || curl_rc=$?` — the donor's bare `code="$(curl …)"; curl_rc=$?` aborts under `set -e` before the retry arm can fire on a network-level curl failure),
- header rewritten as the whe ledger (two migration trees, idempotency ledger, base-schema prerequisite).

The retirement is enforced by NEGATIVE pins in `supabase/functions/_shared/templates_test.ts` (no `FLEET_PROJECT_REF` / `FLEET_TOKEN` / `try_start_rotation` / `fleet_promote` / `edge_proxy_sql_0011` / `0001-0017 artifacts present`), and the fm harness surface (`_shared/templates_test.ts`) is ported — NOT fm's t1/t3–t6 groups (0011 edge-proxy templates, fleet-api, rotation, management-api splitter: no such modules in whe).

## Ledger — NEW (no FM provenance)

| File | Note |
|---|---|
| `supabase/functions/_shared/whe_store.ts` | consumer-store seam. **NOT fully NEW**: the `db()` body is a verbatim excerpt of FM `_shared/supabase-client.ts` @ `ca4d280` (401 LOC file → 76; FM config-JWT/audit/quota helpers dropped, header rewritten). `db()` semantics preserved. |
| `supabase/functions/_shared/whe_store_test.ts` | recording-fake battery pinning the 6 store query shapes (FM has no `supabase-client` test). |
| `README.md`, `API.md`, `DESIGN.md`, `DEPLOY.md`, `CHANGELOG.md`, `PROVENANCE.md` | docs (this round). |
| `LICENSE` | MIT text; FM copyright line preserved + new line. |
| `.github/workflows/test.yml` | modeled on FM's workflow @ `ca4d280`; typecheck job ADDS the engine (FM deliberately excluded it). |
| `Makefile` | FM has none. |
| `scripts/run-tests.mjs` | loud deno detector (new). |
| `deno.json` | tasks only; FM has no deno.json anywhere (its `_shared/import_map.json` is unreferenced dead weight — not copied). |
| `supabase/config.toml` | minimal; adds the `[functions.warehouse-engine] verify_jwt=false` pin FM lacked. |
| `.gitignore` | carried from FM (proven safe). |

## Normative-source pointer map

Module/migration headers cite FM `research/` docs. **None of those files exist in `supabase-fleet-manager` @ `ca4d280` — nor anywhere in that repo's history** (`git log --all -- 'research/*'` is empty; the `ca4d280` tree has no `research/` directory). The citations dangle: the docs live in the worklog lane, not the source repo. Map retained (do not edit the headers; map, per the verbatim-copy doctrine):

| Cited in headers as | Normative for | Found in FM @ `ca4d280`? |
|---|---|---|
| `research/findings_wh_catalog_contract.md` | 0013/0014/0016 + the catalog contract | NO |
| `research/design_wh_query_rpc.md` | shard 0015 (signature, registry, hash law, envelope) | NO |
| `research/findings_wh_scatter_gather.md` | fan-out/merge contract background | NO |
| `research/findings_geo_failover_design.md` | geo gate ladder G1..G6 / G-W1..W5 background | NO |
| `research/design_r53_wh_ryw_lsn_poll.md` | `WH_RYW_V1` lever design (cited in wh_entrypoint.ts) | NO |
| `research/design_r69_shard_channel.md` | shard service-key channel D-decisions (cited in index.ts) | NO |

## Not copied (deliberate)

- `_shared/supabase-client.ts` + `_shared/types.ts` (FM fleet surface) — replaced by `whe_store.ts`.
- `_shared/import_map.json` — unreferenced in FM (dead weight).
- `_shared/http.ts` and everything else outside the engine closure (fleet-api, fleet-cron, watchdogs, deploy.sh — `migrate.sh` + `sql_split.awk` moved UP into the adapted-extraction ledger in v0.1.1).
- FM migrations `0017_geo_control_plane.sql`, `0022`/`0023` (geo/RYW) — geo gated fail-closed this round.
- FM research docs, `.env.example` (FM-shaped; a whe-shaped one may land later), Git history (no filter-repo graft round 1).
