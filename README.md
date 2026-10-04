# supabase-warehouse-engine

Standalone **sharded-warehouse query engine** for one Supabase free-tier project. A single Deno edge function (`supabase/functions/warehouse-engine/`) over a pure library island (`supabase/functions/_shared/wh_*.ts`), extracted from [`supabase-fleet-manager`](https://github.com/ariellebridget3O/supabase-fleet-manager) @ `ca4d280` (MIT — see `LICENSE` and `PROVENANCE.md` for the per-file byte-diff ledger).

What it does:

- **Directory fan-out** — `POST /query` plans an aggregate read (sum/count/min/max/avg), resolves serving shards from `v_warehouse_directory` (routing truth), and fans the plan out to per-shard `wh_query` RPCs, merging partials under coverage/quorum laws.
- **Write fence** — write-plan requests (raw-body `idempotency_key`/write-batch markers) hit the geo write fence **before** parse: fenced fleets answer `503 read_only_mode`; an unwired fence fails closed `500` (a write can never slip ungated).
- **Snapshot HMAC** — with `WH_SNAPSHOT_KEY` set, full-embed responses carry a signed `directory_snapshot`; a valid replay (sig → TTL → version) skips the directory read entirely.
- **Shard handshake** — engine ⇄ shard plane auth (apikey + Bearer = shard service key) with engine-derived plan template hashes (never client-supplied).

**Single-project shape:** the engine's own project is the only shard (the own-ref arm resolves `SUPABASE_URL`'s subdomain as shard) — zero extra config. Add remote shards later via `WH_SHARD_KEYS`.

## Ships disabled (MEGA-loop posture)

`POST /query` returns a pinned **500** — `{"v":1,"error":{"code":"internal","message":"real shard fetcher lands after live probes #1/#2 (PAT-gated design freeze for the QC2 compile)"}}` — **before any directory work**, until the real-fetcher gate flips (`FLIP_hasRealFetcher = false` in `_shared/wh_entrypoint.ts:102`; flipping is a reviewed **code** change, never an env). A deployed stub must never serve plausible-looking empty 200s. `GET /health` is fully functional once migration `0013` is applied — **that is the smoke target**.

The lever set (all default **OFF**/absent):

| Lever | Activation | Effect |
|---|---|---|
| `WH_REAL_FETCHER` | exact string `on` (anything else — `'ON'`, `'1'`, `' true'`, absent — is OFF) | `rpcMode`: the fan-out speaks the `wh_query` RPC shape. Inert pre-flip (the deploy gate keeps precedence). |
| `WH_RYW_V1` | exact string `on` (never best-effort parse) | Read-your-writes `min_lsn` adjudication. OFF ⇒ a `min_lsn`-pinned `/query` is a `400 malformed` naming the lever (planner-honest, fail-closed); every request without `min_lsn` is byte-identical in both states. |
| `WH_SHARD_KEYS` | JSON object, **fail-closed parse** | Remote shard service keys `{"<project-ref>":"<service-role-key>"}`. Absent/empty = a NORMAL state (own-ref arm still works). Unparseable JSON / non-object (incl. arrays, null) / non-string value ⇒ **whole-map reject**: empty map + ONE defect-class boot log (env name + class, never values); every remote shard then warns `shard_key_missing`. |
| `WH_SNAPSHOT_KEY` | presence enables it | Dedicated HMAC key for signed `directory_snapshot` mode; absent ⇒ full-embed only. **Never reuse `WHE_BEARER_TOKEN`** — a client knowing the key could forge snapshots. |

## Quickstart

```bash
git clone https://github.com/ariellebridget3O/supabase-warehouse-engine
cd supabase-warehouse-engine

# 1. Offline battery — zero network, no DB, no PAT.
make test                 # or: deno task test

# 2. Apply migrations with the Management-API runner (one statement per call,
#    fail-fast, idempotent re-runs — DEPLOY.md §2):
#    SUPABASE_ACCESS_TOKEN="$SB_PAT" WHE_PROJECT_REF="$REF" bash scripts/migrate.sh
#    Single-project shape: the engine host is also the only shard, so ALSO apply the
#    shard side and seed the W1–W5 templates (DEPLOY.md §3):
#    WHE_PROJECT_REF="$REF" bash scripts/migrate.sh --shard
#    (psql through the pooler / the dashboard SQL editor remain documented fallbacks.)

# 3. Set the bearer secret, deploy (no Docker, no local Deno needed):
#    SUPABASE_ACCESS_TOKEN="$SB_PAT" npx -y supabase functions deploy warehouse-engine \
#      --project-ref $REF --no-verify-jwt --use-api

# 4. Smoke (needs 0013 applied first — see rescue line 5):
curl -fsS "https://$REF.supabase.co/functions/v1/warehouse-engine/health"
#    → 200 {"v":1,"ok":true,"directory_version":N}
```

## Environment variables

| Var | Required | Semantics |
|---|---|---|
| `WHE_BEARER_TOKEN` | **yes** | The fn-level bearer for every route except `OPTIONS` and `GET /health` (timing-safe compare) + an `apikey` header must be present. Unset ⇒ every authed route answers `500 "server has no WHE_BEARER_TOKEN secret set"`. (FM's `FLEET_TOKEN` name is retired.) |
| `SUPABASE_URL` | auto-injected on Supabase | Own project URL; the subdomain **is** the own project ref (own-ref shard arm). Unparseable ⇒ one boot defect log + own-ref arm disabled. |
| `SUPABASE_SERVICE_ROLE_KEY` | auto-injected on Supabase | Consumer-store DB reads + own-ref shard key resolution. Empty ⇒ fail-closed `shard_key_missing`. |
| `WH_SNAPSHOT_KEY` | optional | Enables signed `directory_snapshot` mode. Never equal to `WHE_BEARER_TOKEN`. |
| `WH_SHARD_KEYS` | optional (multi-shard only) | JSON object `{"<project-ref>":"<service-role-key>", …}`; fail-closed whole-map parse (above). |
| `WH_RYW_V1` | optional, default **OFF** | Read-your-writes lever; single activation value the exact string `on`. |
| `WH_REAL_FETCHER` | optional, default **OFF** | rpcMode lever; exact `on`. Pre-flip it is inert. |

Deploy-time (never a runtime fn secret): `SUPABASE_ACCESS_TOKEN` — a Management API PAT (`sbp_…`). It is **not** `WHE_BEARER_TOKEN`, and no fallback between them exists in this repo.

## Routes

Full wire contract: [API.md](API.md).

| Route | Auth | Response |
|---|---|---|
| `OPTIONS *` | exempt | `204` + CORS headers |
| `GET /health` | none | `200 {"v":1,"ok":true,"directory_version":N}` (probe failure ⇒ `500 {"v":1,"ok":false,…}`) |
| `POST /query` | Bearer `WHE_BEARER_TOKEN` + `apikey` | `200` engine envelope / mapped error |
| anything else | Bearer + `apikey` | `400 {"v":1,"error":{"code":"malformed","message":"no route <METHOD> <path>"}}` |

## Onboarding rescue lines

1. **`deno` missing** — `make test` runs `scripts/run-tests.mjs` (a node-run wrapper — stock `node` executes it; `deno task test` bypasses the wrapper), which prints a loud banner with the exact install command (`curl -fsSL https://deno.land/x/install/install.sh | sh`) and **exits 1**. It never silently skips (green-when-unverified is banned). Deno is needed for the battery and `deno check` — *not* for deploy (`--use-api` bundles server-side).
2. **Supabase CLI auth** — export `SUPABASE_ACCESS_TOKEN=sbp_…` (a real PAT from dashboard → account → tokens). **Explicit, never a fallback**: this repo deliberately drops FM's `deploy.sh` fallback that offered the bearer token as a PAT — that silent swap just 401s confusingly. Three tokens, three jobs: PAT (Management API) ≠ `WHE_BEARER_TOKEN` (this function) ≠ shard service keys.
3. **Project paused (free tier)** — preflight before anything: `curl -s -H "Authorization: Bearer $SUPABASE_ACCESS_TOKEN" https://api.supabase.com/v1/projects/$REF | jq .status` → expect `ACTIVE_HEALTHY` (requires `jq` on PATH); `PAUSED` ⇒ dashboard → restore project, then re-run.
4. **`verify_jwt` left on** — `supabase/config.toml` pins `[functions.warehouse-engine] verify_jwt = false`, and deploys pass `--no-verify-jwt`. Discrimination: a **bare gateway 401** (no `auth_kind` field) = platform JWT check rejected the call before the handler; the fn's **own 401** always says `auth rejected before route dispatch (<kind>)` with an `auth_kind` field (see API.md).
5. **Migrations before smoke** — `GET /health` answers `500 {"v":1,"ok":false,…}` until `0013_warehouse_catalog.sql` is applied (the version probe reads `config.warehouse_directory_version` / the directory view). Apply 0013 → 0014 → 0016 first, then smoke.

## Known gaps (v0.1.1)

- **Base-schema prerequisite (engine host):** migration `0013` references `public.projects(id)` / `public.orgs(id)` / `public.config`, which come from the **platform base schema** (applied when the project was provisioned for the fleet-manager family of engines). On a truly fresh project where those objects never existed, `0013` fails with `42P01` (undefined table) unless the base schema is applied first — `scripts/migrate.sh`'s header documents this, and the runner's `verify_migrations` assumes the base objects exist too.
- **Geo legs fail-closed** until the geo control-plane migration (`0017`) lands: absent geo rows ⇒ write plans `503 read_only_mode`, replica-plane deps fail closed to primary, unwired fence ⇒ `500` on write plans. No geo migrations ship in 0.1.0.
- `POST /query` is the pinned pre-flip 500 (ships disabled, above) — the real shard fetcher lands after live probes #1/#2.

## Repo map

```
supabase/functions/warehouse-engine/index.ts   # thin Deno.serve shell (env + dep wiring)
supabase/functions/_shared/wh_*.ts             # pure library island (env-free except checkAuth)
supabase/functions/_shared/whe_store.ts        # consumer-store seam (NEW; replaces FM supabase-client.ts)
supabase/functions/_shared/geo_write_fence.ts  # write fence + FENCE_CONFIG_KEYS read
db/migrations/0013,0014,0016                   # ENGINE-HOST schema (catalog, loader RPC, roll-off seal)
db/shard-migrations/0015,0016                  # SHARD-side wh_query RPC + seal/roll-off
db/shard-templates/W1..W5 + manifest.json      # query template bodies-of-record (sha256-pinned)
scripts/run-tests.mjs | lint_shard_templates.py | render_wh_seed_wave.py
scripts/migrate.sh + scripts/sql_split.awk     # Management-API migration runner (engine + --shard)
```

See [DESIGN.md](DESIGN.md) for the architecture, [DEPLOY.md](DEPLOY.md) for the step-by-step deploy, [PROVENANCE.md](PROVENANCE.md) for the verbatim-copy ledger.
