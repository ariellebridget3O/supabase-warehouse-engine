.PHONY: test lint-templates seed-wave check migrate-dry ci stamp deploy-gate deploy

# The offline battery (pinned verbatim repo-root cwd command; zero network).
# scripts/run-tests.mjs is a LOUD deno detector (a node-run wrapper — a .mjs
# file needs node; deno runs the battery itself inside the wrapper):
# deno present => run `deno test --no-check -q --allow-env --allow-read` and
# propagate the exit code; deno absent => banner + the exact install command +
# exit 1 (never a silent skip, never a green-when-unverified).
test:
	node scripts/run-tests.mjs

# Offline L3 lint over db/shard-templates/ (manifest.json is the
# body-of-record with pinned sha256 template hashes). Zero network.
lint-templates:
	python3 scripts/lint_shard_templates.py

# Render the per-shard wh_query seed-wave SQL from db/shard-templates/.
# Bare `make seed-wave` prints usage (--help || true); a real render needs
# --templates-dir db/shard-templates [--with-cold] --out <file>.
seed-wave:
	python3 scripts/render_wh_seed_wave.py --help || true

# Type-check the engine entrypoint + HTTP handler (the FM P1 closure: the
# engine was never type-checked in supabase-fleet-manager). Loud-fails when
# deno is absent — same law as `test`.
check: stamp
	@if command -v deno >/dev/null 2>&1; then \
	  deno check supabase/functions/warehouse-engine/index.ts supabase/functions/_shared/wh_entrypoint.ts; \
	else \
	  echo "ERROR: deno not found on PATH — install Deno 2.x: curl -fsSL https://deno.land/x/install/install.sh | sh" >&2; exit 1; \
	fi

# Dry-run the migration runner (zero network, zero env — lists the migration
# files + statement counts; the runner's own exit code propagates, so a broken
# splitter or a missing tree FAILS the target — fail-never-skip).
migrate-dry:
	bash scripts/migrate.sh --dry-run

# Full offline CI gate: type-check + battery + template lint + migrate dry-run.
ci: stamp check test lint-templates migrate-dry

# r124 A8 (design_r124_opt3_a8.md §2, audit B ➋B-1): the GENERATED build
# stamp — writes supabase/functions/_shared/engine_build.ts
# (`export const ENGINE_BUILD = "<sha7>";`) from the HEAD sha7. The file is
# GITIGNORED and NEVER committed (committing = sha self-reference regress).
# `check`/`ci` bootstrap it (a fresh clone's deno check fails on the missing
# import otherwise). Idempotent + offline (git rev-parse only).
stamp:
	@sha7="$$(git rev-parse --short=7 HEAD 2>/dev/null)" || { echo "ERROR: make stamp could not resolve HEAD (git rev-parse --short=7 failed) — run inside the git worktree" >&2; exit 1; }; \
	printf 'export const ENGINE_BUILD = "%s";\n' "$$sha7" > supabase/functions/_shared/engine_build.ts; \
	echo "stamped supabase/functions/_shared/engine_build.ts @ $$sha7"

# r124 A8 deploy gate (design §2 ➋B-2): FIVE ordered checks — on-branch main /
# clean tree / HEAD==origin/main after fetch / stamp fresh / required env non-empty
# — each dying with ONE fixed message (fm deploy.sh fail-fast style; the
# token guard lives in-script, NEVER ${:?} raw-bash — r118 T-3).
deploy-gate:
	bash scripts/deploy_gate.sh

# r124 A8: deploy = gate && the exact DEPLOY.md §5 CLI line. The gate runs
# AGAIN as this target's prerequisite (belt-and-braces — the design leg
# `make stamp && make deploy-gate && make deploy` runs it twice).
# SUPABASE_ACCESS_TOKEN + WHE_PROJECT_REF come from the caller's env (no
# project ref is ever hardcoded — no secrets in the repo).
deploy: deploy-gate
	SUPABASE_ACCESS_TOKEN="$${SUPABASE_ACCESS_TOKEN}" npx -y supabase functions deploy warehouse-engine --project-ref "$${WHE_PROJECT_REF}" --no-verify-jwt --use-api
