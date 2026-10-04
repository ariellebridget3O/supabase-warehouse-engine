.PHONY: test lint-templates seed-wave check migrate-dry ci

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
check:
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
ci: check test lint-templates migrate-dry
