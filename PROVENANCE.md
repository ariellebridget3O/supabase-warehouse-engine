# PROVENANCE — copy ledger

Every copied file originated in **`supabase-fleet-manager` @ `ca4d280`** (`main`, tree clean; origin `github.com/ariellebridget3O/supabase-fleet-manager`). Verbatim copies keep byte-diffability with the source repo (future `wh_*` fixes can be re-synced by diff). Files marked **NEW** have no FM provenance.

## License inheritance

The source repo is **MIT** (`supabase-fleet-manager/LICENSE`: "Copyright (c) 2026 Supabase Fleet Manager Contributors"). This repo is MIT; `LICENSE` preserves the FM copyright line verbatim **and** adds the new line — MIT §2 requires the copyright + license notice be preserved for substantial portions. Attribution is met by LICENSE retention + this ledger.

## Ledger — copied (source path @ `ca4d280`)

### Runtime modules (verbatim)

| This repo | Source (FM) @ `ca4d280` | LOC |
|---|---|---|
| `supabase/functions/_shared/wh_types.ts` | same path | 116 |
| `supabase/functions/_shared/wh_canonical.ts` | same path | 328 |
| `supabase/functions/_shared/wh_merge.ts` | same path | 550 |
| `supabase/functions/_shared/wh_engine_core.ts` | same path | 1,671 |
| `supabase/functions/_shared/wh_handshake.ts` | same path | 488 |
| `supabase/functions/_shared/wh_snapshot.ts` | same path | 212 |
| `supabase/functions/_shared/wh_shard_channel.ts` | same path | 296 |
| `supabase/functions/_shared/wh_directory_reader.ts` | same path | 220 |
| `supabase/functions/_shared/wh_entrypoint.ts` | same path | 543 |
| `supabase/functions/_shared/wh_testutil.ts` | same path (test helper; copied, not moved) | 206 |
| `supabase/functions/_shared/geo_write_fence.ts` | same path (not a `wh_*` file; runtime-required by wh_entrypoint + shell) | 342 |

Total runtime island: 4,972 LOC (10 `wh_*` = 4,630 + fence 342).

### Tests + fixtures (verbatim)

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

The `db/migrations/` vs `db/shard-migrations/` split is load-bearing (0015's header: the project-side runner globs `db/migrations/*.sql`; shard files must never be globbed onto the engine host). The W1–W5 template files and `manifest.json` are **sha256-pinned bodies-of-record** — never reformat.

## Ledger — NEW (no FM provenance)

| File | Note |
|---|---|
| `supabase/functions/_shared/whe_store.ts` | consumer-store seam; replaces FM `_shared/supabase-client.ts` (not copied: FM config-JWT/audit/quota baggage). `db()` semantics preserved. |
| `supabase/functions/_shared/whe_store_test.ts` | recording-fake battery pinning the 6 store query shapes. |
| `README.md`, `API.md`, `DESIGN.md`, `DEPLOY.md`, `CHANGELOG.md`, `PROVENANCE.md` | docs (this round). |
| `LICENSE` | MIT text; FM copyright line preserved + new line. |
| `.github/workflows/test.yml` | modeled on FM's workflow @ `ca4d280`; typecheck job ADDS the engine (FM deliberately excluded it). |
| `Makefile` | FM has none. |
| `scripts/run-tests.mjs` | loud deno detector (new). |
| `deno.json` | tasks only; FM has no deno.json anywhere (its `_shared/import_map.json` is unreferenced dead weight — not copied). |
| `supabase/config.toml` | minimal; adds the `[functions.warehouse-engine] verify_jwt=false` pin FM lacked. |
| `.gitignore` | carried from FM (proven safe). |

## Normative-source pointer map

Module/migration headers cite FM `research/` docs that do not exist in this repo. Map (do not edit the headers; map, per the verbatim-copy doctrine):

| Cited in headers as | Lives at (FM @ `ca4d280`) |
|---|---|
| `research/findings_wh_catalog_contract.md` | normative for 0013/0014/0016 + the catalog contract |
| `research/design_wh_query_rpc.md` | normative for shard 0015 (signature, registry, hash law, envelope) |
| `research/findings_wh_scatter_gather.md` | fan-out/merge contract background |
| `research/findings_geo_failover_design.md` | geo gate ladder G1..G6 / G-W1..W5 background |
| `research/design_r53_wh_ryw_lsn_poll.md` | `WH_RYW_V1` lever design (cited in wh_entrypoint.ts) |
| `research/design_r69_shard_channel.md` | shard service-key channel D-decisions (cited in index.ts) |

## Not copied (deliberate)

- `_shared/supabase-client.ts` + `_shared/types.ts` (FM fleet surface) — replaced by `whe_store.ts`.
- `_shared/import_map.json` — unreferenced in FM (dead weight).
- `_shared/http.ts` and everything else outside the engine closure (fleet-api, fleet-cron, watchdogs, deploy.sh, migrate.sh — migrate.sh lands v0.1.1).
- FM migrations `0017_geo_control_plane.sql`, `0022`/`0023` (geo/RYW) — geo gated fail-closed this round.
- FM research docs, `.env.example` (FM-shaped; a whe-shaped one may land later), Git history (no filter-repo graft round 1).
