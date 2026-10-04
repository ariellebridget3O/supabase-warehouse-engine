# DEPLOY — step-by-step

Deno is **not** required on the deploying machine (`--use-api` bundles server-side; no Docker either). You need: a project ref, a Management API PAT, and `npx`.

**Token discipline (read first).** Three different tokens, three different jobs:

| Token | Shape | Used for |
|---|---|---|
| `SUPABASE_ACCESS_TOKEN` | `sbp_…` (Management API PAT, dashboard → account/tokens) | deploy CLI + Mgmt API calls in this doc |
| `WHE_BEARER_TOKEN` | any high-entropy string | the engine's own fn-level bearer (`/query`) |
| shard service keys | the shard projects' `SUPABASE_SERVICE_ROLE_KEY` | engine → shard plane auth (`WH_SHARD_KEYS`), never documented here |

There is **no fallback** between them in this repo (FM's deploy-script fallback that offered the bearer as a PAT was deliberately dropped). Never reuse `WHE_BEARER_TOKEN` as `WH_SNAPSHOT_KEY`.

## 1. Preflight (project paused?)

```bash
export SUPABASE_ACCESS_TOKEN=sbp_…        # a real PAT — never the bearer token
export WHE_PROJECT_REF=<your-project-ref>
curl -s -H "Authorization: Bearer $SUPABASE_ACCESS_TOKEN" \
  "https://api.supabase.com/v1/projects/$WHE_PROJECT_REF" | jq '.status'
```

Expect `"ACTIVE_HEALTHY"`. A free-tier project that auto-paused returns `"PAUSED"` — restore it from the dashboard, wait for restore, re-run.

## 2. Apply engine-host migrations (order is hand-verified: 0013 → 0014 → 0016)

Apply to the **engine host** project, in order:

1. `db/migrations/0013_warehouse_catalog.sql` — `warehouse_tables`, `warehouse_placements` (two partial unique indexes), `v_warehouse_directory`, `warehouse_cold_objects`, `load_jobs`/`load_partitions`, `config.warehouse_directory_version` + `wh_bump_directory_version()`. **/health 500s until this is applied.**
2. `db/migrations/0014_loader_rpc.sql` — `fm_loader_bookkeep` / `fm_loader_finalize` (the ONE idempotent ledger RPC).
3. `db/migrations/0016_rolloff_seal.sql` — the v1 seal-only roll-off FM-side half (`roll_off_threshold_pct = 0.80`, `fm_rolloff_finalize`).

Either: **SQL editor** (dashboard → SQL editor → paste each file) or **psql via the session pooler**:

```bash
psql "postgresql://postgres.$WHE_PROJECT_REF:<db-password>@aws-0-<region>.pooler.supabase.com:5432/postgres" \
  -f db/migrations/0013_warehouse_catalog.sql
psql … -f db/migrations/0014_loader_rpc.sql
psql … -f db/migrations/0016_rolloff_seal.sql
```

> **v0.1.0 gap:** the Management-API SQL runner (`migrate.sh`, one statement per call, fail-fast) lands in v0.1.1 — apply via psql/SQL editor for now.

## 3. Apply shard migrations + seed templates to each SHARD project

For **each serving shard** (in the single-project shape, the engine host itself is the only shard — apply to the same project):

1. `db/shard-migrations/0015_wh_query_rpc.sql` — the `wh_query` RPC + registry + hash law (`WH403 wh_hash_mismatch`). 26 statements, splittable on top-level semicolons. **APPLY TARGET: serving shards only** — it lives in `db/shard-migrations/` (not `db/migrations/`) precisely so a project-side runner globbing `db/migrations/*.sql` never applies it to the wrong host.
2. `db/shard-migrations/0016_seal_roll_off.sql` — shard-side `facts_blocks`, `unpack_block`, `facts_events`, seal functions.
3. Seed the W1–W5 query templates **per `db/shard-templates/manifest.json`** (the body-of-record with pinned sha256 `template_hash`es — do not reformat those files). Lint first, then render the seed wave:

```bash
python3 scripts/lint_shard_templates.py          # must report: 5/5 templates PASS
python3 scripts/render_wh_seed_wave.py \
  --templates-dir db/shard-templates --out seed_wave.sql   # add --with-cold only if facts_blocks DDL (shard 0016) is applied
psql … -f seed_wave.sql                          # or paste via the SQL editor
```

## 4. Set secrets (`WHE_BEARER_TOKEN` required)

Via the Management API secrets endpoint (JSON-array body):

```bash
curl -s -X POST "https://api.supabase.com/v1/projects/$WHE_PROJECT_REF/secrets" \
  -H "Authorization: Bearer $SUPABASE_ACCESS_TOKEN" \
  -H "Content-Type: application/json" \
  -d '[{"name":"WHE_BEARER_TOKEN","value":"'"$(openssl rand -hex 32)"'"}]'
```

Optional, same endpoint (separate calls or one array):

- `WH_SNAPSHOT_KEY` — a **dedicated** HMAC key (`openssl rand -hex 32`); enables signed `directory_snapshot` mode. **Never** equal to `WHE_BEARER_TOKEN`.
- `WH_SHARD_KEYS` — multi-shard only: `[{"name":"WH_SHARD_KEYS","value":"{\"<project-ref>\":\"<shard-service-role-key>\"}"}]`. Absent/empty is a NORMAL state (the own-ref arm still works); a malformed value fails closed to an empty map with one defect-class boot log.

`SUPABASE_URL` + `SUPABASE_SERVICE_ROLE_KEY` are **auto-injected** by the Supabase edge runtime — do not set them. (CLI equivalent for single secrets: `npx supabase secrets set WHE_BEARER_TOKEN=… --project-ref $WHE_PROJECT_REF`.)

## 5. Deploy

```bash
SUPABASE_ACCESS_TOKEN="$SUPABASE_ACCESS_TOKEN" \
npx -y supabase functions deploy warehouse-engine \
  --project-ref "$WHE_PROJECT_REF" --no-verify-jwt --use-api
```

- `--no-verify-jwt` is the operative flag (auth is the fn-level bearer check, not Supabase JWTs). `supabase/config.toml` also pins `[functions.warehouse-engine] verify_jwt = false` as belt-and-braces.
- `--use-api` bundles **server-side**: neither Docker nor a local Deno install is needed.
- **config.toml law:** the config ships minimal (`project_id` + the one `[functions.*]` block). **No `[edge_runtime]` section** — an `[edge_runtime]` block in config.toml breaks deploys (`ProjectConfigParseError`). Do not add one.
- Redeploys bump the isolate; re-run the smoke below after every deploy.

## 6. Smoke

```bash
# 6a. /health — the first 200 (needs only migration 0013 applied):
curl -fsS "https://$WHE_PROJECT_REF.supabase.co/functions/v1/warehouse-engine/health"
#    → 200 {"v":1,"ok":true,"directory_version":1}

# 6b. authed /query — expect the PINNED pre-flip 500 (ships disabled):
curl -s -X POST "https://$WHE_PROJECT_REF.supabase.co/functions/v1/warehouse-engine/query" \
  -H "Authorization: Bearer $WHE_BEARER_TOKEN" -H "apikey: $WHE_PROJECT_REF.anon-key-or-any-value" \
  -H "Content-Type: application/json" -d '{"qid":"smoke-1","table":"wh_probe_agg","query":{"select":[{"op":"count"}]}}'
#    → 500 {"v":1,"qid":"smoke-1","error":{"code":"internal",
#         "message":"real shard fetcher lands after live probes #1/#2 …"}}
```

6b is the *good* outcome: it proves bearer auth + secret wiring end-to-end (a wrong token gives `401 … auth_kind:"invalid_token"` instead; a missing secret gives `500 "server has no WHE_BEARER_TOKEN secret set"`).

## Troubleshooting

| Symptom | Cause → fix |
|---|---|
| Bare `401` **without** `auth_kind` in the body | Platform JWT verification is ON (rejected at the gateway before the handler). Deploy with `--no-verify-jwt` and check `config.toml` pins `verify_jwt = false`. |
| `401` with `"auth_kind":"invalid_token"` | Client bearer ≠ `WHE_BEARER_TOKEN` secret. Re-set the secret (§4) and retry. |
| `500 "server has no WHE_BEARER_TOKEN secret set"` | Secret not set on the deployment — §4. |
| `GET /health` → `500 {"v":1,"ok":false,…}` | Migration `0013` not applied (version probe fails) — migrations-before-smoke, §2. |
| Mgmt API 401 / "no organizations found" | `SUPABASE_ACCESS_TOKEN` holds the wrong token (bearer ≠ PAT) — §Token discipline. |
| Deploy tries a local bundle / Docker | Missing `--use-api` — §5. |
| `POST /query` → 404 `tier_warm` (post-flip era) | Directory has no serving rows for the table — check placements + `v_warehouse_directory`. |
| `WH_SHARD_KEYS` malformed | Boot logs ONE defect line (`WH_SHARD_KEYS defect class …`), then every remote shard warns `shard_key_missing`. Value shape: `{"<project-ref>":"<service-role-key>"}` — §4. |
| `400 … WH_RYW_V1=on` in the message | A `min_lsn`-pinned request with the RYW lever OFF — planner-honest fail-closed; set `WH_RYW_V1=on` only when the stamp feed exists. |
