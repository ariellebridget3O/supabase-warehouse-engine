#!/usr/bin/env bash
# =============================================================================
# scripts/migrate.sh — warehouse-engine (WHE) DB migration runner (Management API).
# =============================================================================
# Applies the engine's SQL waves to a Supabase project via the Management API,
# in lexicographic NNNN_ order, one statement per
#     POST /v1/projects/{ref}/database/query
# call (split by scripts/sql_split.awk — the same '…'/"…"/--/ /* */ /$tag$
# quoting semantics as _shared/management-api.ts::runSql, which documents that
# the Management API accepts only ONE statement per call). Fail-fast on the
# first server error, printing file, statement index, a snippet, and the
# server message. Transient failures (429/5xx/network) are retried 3x with
# backoff — honoring a numeric Retry-After header on 429 (capped at 30s),
# fixed 2s/4s backoff otherwise (same policy as
# _shared/management-api.ts::fetchWithRetry).
#
# TWO MIGRATION TREES, TWO ORDERS (pick with --shard):
#   ENGINE (default — db/migrations/):  0013_warehouse_catalog.sql
#                                    → 0014_loader_rpc.sql
#                                    → 0016_rolloff_seal.sql
#     The engine project's own waves: unified warehouse catalog → loader
#     ledger-write RPC → seal-only roll-off. After applying, verify_migrations
#     sweeps the Postgres catalogs (see the verification section below).
#   SHARD (--shard — db/shard-migrations/): 0015_wh_query_rpc.sql
#                                    → 0016_seal_roll_off.sql
#     Targets each SERVING SHARD — see the 0015 header ("APPLY TARGET: each
#     SERVING SHARD …"). verify_migrations is SKIPPED entirely in shard mode:
#     it is engine-scoped, and the shard migrations carry their own in-SQL
#     verify-gate DO blocks (0015's $verify_wh_registry$ fail-loudly gates)
#     instead of a runner-side catalog sweep.
#   SEED WAVE: db/shard-templates/ + scripts/render_wh_seed_wave.py render the
#     per-shard wh_query seed statements; apply the rendered file to a shard
#     with --file <rendered seed_wave.sql>.
#
# IDEMPOTENCY LEDGER (re-run safety — the runner simply re-applies ALL files
# in order on every run; no migration-tracking table):
#   0013: create table/index if not exists, create or replace view/function,
#         drop trigger if exists + create trigger, config seed on conflict do
#         nothing, alter table enable row level security (no-op once on),
#         revoke (no-op when no grants remain). Pure DDL — never mutates rows.
#         One EXCEPTION: the INITIAL creation of the two cross-writer partial
#         unique indexes FAILS LOUDLY if a dirty directory pre-exists
#         (impossible on a fresh catalog; resolve duplicate rows by hand
#         first, then re-run). One nuance: create or replace view RESETS
#         reloptions, so the adjacent alter view … set (security_invoker =
#         true) is LOAD-BEARING on every re-run — both ship adjacent in 0013.
#   0014: create or replace function (fm_loader_bookkeep — the ONE idempotent
#         loader ledger-write RPC) + revoke all on function … from public,
#         anon, authenticated + comment on function — pure DDL; a revoke is a
#         no-op when no grants remain, so re-applying never mutates rows.
#         NEVER introduce a DROP+CREATE of fm_loader_bookkeep in a later
#         migration: DROP resets ACLs and the PUBLIC EXECUTE default returns
#         (revision is CREATE OR REPLACE only, which preserves ACLs). The
#         ledger tables/columns it writes are 0013's — 0014 only adds the
#         server-side state machine.
#   0015 (shard): create table if not exists + RLS/policy/grant statements +
#         idempotent DO blocks, ending in in-SQL verify gates that FAIL
#         LOUDLY on drift — the shard-side self-verification.
#   0016 (engine): create or replace function/view + config seed on conflict
#         do nothing + revokes — pure DDL; re-applying never mutates rows.
#   0016_seal (shard): alter table add column if not exists + create or
#         replace function — pure DDL.
#
# BASE-SCHEMA PREREQUISITE (engine mode): 0013 references public.projects(id),
#   public.orgs(id) and public.config, which come from the PLATFORM BASE
#   schema (the fm 0001_init.sql class — applied when the project was
#   provisioned for the fleet-manager family of engines). On a TRULY FRESH
#   project where those objects never existed, 0013 fails with 42P01
#   (undefined table) unless the base schema is applied first;
#   verify_migrations assumes the base objects exist too.
#
# ENV CONTRACT (network modes): SUPABASE_ACCESS_TOKEN (a Supabase personal
#   access token, PAT) must be set, plus the project ref via --project-ref
#   REF or WHE_PROJECT_REF. --dry-run needs NO env (no network calls).
#
# --SHARD MODE SEMANTICS: --shard retargets the migration glob from
#   db/migrations/ to db/shard-migrations/ (a pre-exported MIGRATIONS_DIR
#   still wins over the --shard default), makes the collect_migrations()
#   directory guard accept the shard tree, and SKIPS verify_migrations. Point
#   --project-ref at a SERVING SHARD ref, never the engine project. Combines
#   freely with --dry-run (list only) and --file.
#
# Usage (standalone):
#   scripts/migrate.sh --dry-run                           # no network: list
#                                                          # files + stmt counts
#   scripts/migrate.sh --dry-run --shard                   # same, shard tree
#   source .env && scripts/migrate.sh --project-ref $WHE_PROJECT_REF
#   scripts/migrate.sh --project-ref REF --verify-only     # engine catalogs check
#   scripts/migrate.sh --project-ref REF --file <path.sql> # run any SQL file
#                                                          # (e.g. a rendered
#                                                          #  seed_wave.sql)
# As a library (source-safe: the log helpers are installed only when the
# caller has not already defined them — declare -F guard — and no top-level
# code runs besides the constants):
#   source scripts/migrate.sh
#   apply_all_migrations "$REF"; verify_migrations "$REF"; run_sql_file "$REF" f.sql
#   # shard side: export WHE_SHARD_MODE=1 (and optionally MIGRATIONS_DIR), or
#   # use the standalone entry's --shard.
#
# Requires: bash, curl, jq, awk.  Never touches live infra unless you run it.
# =============================================================================

# ---------- log helpers: reuse the caller's if sourced from setup/deploy -----
if ! declare -F log >/dev/null 2>&1; then
  c_red() { printf '\033[31m%s\033[0m' "$1"; }
  c_grn() { printf '\033[32m%s\033[0m' "$1"; }
  c_ylw() { printf '\033[33m%s\033[0m' "$1"; }
  c_blu() { printf '\033[34m%s\033[0m' "$1"; }
  log()  { printf '[%s] %s\n' "$(c_blu migrat)" "$1"; }
  ok()   { printf '[%s] %s\n' "$(c_grn ok     )" "$1"; }
  warn() { printf '[%s] %s\n' "$(c_ylw warn   )" "$1" >&2; }
  die()  { printf '[%s] %s\n' "$(c_red fail   )" "$1" >&2; exit 1; }
fi

MIGRATE_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd -P)"
# DEFAULT glob target: db/migrations/ — the ENGINE project's own catalog
# waves (0013→0014→0016). db/shard-migrations/ (0015_wh_query_rpc.sql +
# 0016_seal_roll_off.sql) targets SERVING SHARDS; its files share the
# NNNN_ prefix convention, so a mis-set MIGRATIONS_DIR would silently
# apply one tree to the wrong project. --shard retargets the glob to
# db/shard-migrations/ (a pre-exported MIGRATIONS_DIR still wins); the
# collect_migrations() guard makes an out-of-mode override die loudly.
WHE_MIGRATIONS_DIR_PRESET="${MIGRATIONS_DIR:-}"
MIGRATIONS_DIR="${MIGRATIONS_DIR:-$MIGRATE_SCRIPT_DIR/../db/migrations}"
MGMT_API_BASE="https://api.supabase.com"
MGMT_LAST_ERROR=""
MGMT_RESPONSE_FILE=""
MGMT_HEADERS_FILE=""
# Shard mode flag: set to 1 by the standalone entry's --shard (or by a library
# caller before collect_migrations/apply_all_migrations). It (a) lets the
# collect_migrations() directory guard accept db/shard-migrations/ and (b)
# marks the standalone apply flow to SKIP verify_migrations (engine-scoped).
WHE_SHARD_MODE="${WHE_SHARD_MODE:-0}"
# ---------- splitter ----------------------------------------------------------
# sql_split <file> — stdout: statements separated by ASCII 0x1e (see .awk).
sql_split() {
  awk -f "$MIGRATE_SCRIPT_DIR/sql_split.awk" "$1"
}

# ---------- one statement → Management API ------------------------------------
# mgmt_query <project-ref> <single-sql-statement>
# Writes the response body to $MGMT_RESPONSE_FILE (caller must rm it) and
# returns 0/1, setting MGMT_LAST_ERROR. Response headers go to
# $MGMT_HEADERS_FILE (owned + rm'd here) so a 429 Retry-After can be honored.
# Runs in the CURRENT shell (no command
# substitution) so the error text survives to the caller's failure message.
# One statement per call: POST /v1/projects/{ref}/database/query accepts
# exactly one statement.
mgmt_query() {
  local ref="$1" sql="$2"
  local payload code curl_rc attempt ok=0 err delay ra
  [[ -n "${SUPABASE_ACCESS_TOKEN:-}" ]] || { MGMT_LAST_ERROR="SUPABASE_ACCESS_TOKEN not set (source .env)"; return 1; }
  command -v curl >/dev/null || { MGMT_LAST_ERROR="curl not found"; return 1; }
  command -v jq   >/dev/null || { MGMT_LAST_ERROR="jq not found";   return 1; }

  MGMT_RESPONSE_FILE="$(mktemp "${TMPDIR:-/tmp}/whe-migrate.XXXXXX")"
  MGMT_HEADERS_FILE="$(mktemp "${TMPDIR:-/tmp}/whe-headers.XXXXXX")"
  payload="$(jq -Rs '{query: .}' <<< "$sql")"

  for attempt in 1 2 3; do
    MGMT_LAST_ERROR=""
    curl_rc=0
    code="$(curl -sS -o "$MGMT_RESPONSE_FILE" -D "$MGMT_HEADERS_FILE" -w '%{http_code}' \
      -X POST "$MGMT_API_BASE/v1/projects/$ref/database/query" \
      -H "Authorization: Bearer $SUPABASE_ACCESS_TOKEN" \
      -H "Content-Type: application/json" \
      -d "$payload")" || curl_rc=$?
    if [[ $curl_rc -eq 0 && "$code" == 2* ]]; then ok=1; break; fi
    # Retry transient failures only (mirrors fetchWithRetry: 429 + 5xx + network).
    if [[ $attempt -lt 3 && ( $curl_rc -ne 0 || "$code" == "429" || "$code" == 5* ) ]]; then
      # Honor a numeric Retry-After header on 429 when present (capped at 30s,
      # like fetchWithRetry); an HTTP-date value or absence falls back to the
      # fixed 2s/4s backoff. P3 (r5 audit): the old awk matched $1=="retry-after:"
      # field-wise, so a header emitted WITHOUT the space (`Retry-After:5`) was
      # one single field and silently missed (empirically confirmed; fails safe
      # to the fixed backoff). Now: case-insensitive whole-line prefix match,
      # then strip the 12-char prefix + leading blanks + CR; the numeric
      # validation below still rejects HTTP-date values and junk.
      delay=$((attempt * 2))
      if [[ "$code" == "429" ]]; then
        ra="$(awk 'tolower($0) ~ /^retry-after:/ { v = substr($0, 13); gsub(/^[ \t]+/, "", v); gsub(/\r/, "", v); print v; exit }' "$MGMT_HEADERS_FILE" 2>/dev/null || true)"
        if [[ "$ra" =~ ^[0-9]+$ ]]; then
          ra=$((10#$ra))
          if (( ra >= 1 )); then delay=$(( ra < 30 ? ra : 30 )); fi
        fi
      fi
      sleep "$delay"
      continue
    fi
    break
  done
  rm -f "$MGMT_HEADERS_FILE"

  if [[ $ok -ne 1 ]]; then
    err="$(jq -rj '(.error.message // .message // .error // .) | tostring' "$MGMT_RESPONSE_FILE" 2>/dev/null || true)"
    [[ -n "$err" && "$err" != "null" ]] || err="$(head -c 400 "$MGMT_RESPONSE_FILE" 2>/dev/null)"
    MGMT_LAST_ERROR="HTTP ${code:-none}${curl_rc:+ (curl rc $curl_rc)}: ${err:-<empty response>}"
    rm -f "$MGMT_RESPONSE_FILE"
    return 1
  fi
  return 0
}

# ---------- splitter, with a checked exit status ---------------------------------
# split_to_file <file.sql> — runs sql_split into the temp file $SPLIT_TMP_FILE
# (caller must rm -f it) so the awk EXIT STATUS is observable and CHECKED.
# WHY: the old `< <(sql_split …)` process substitution swallowed awk failures —
# a crashed splitter looked like "no statements (skipped)" (review P3-5).
# Note: empty OUTPUT is still legal (a comment-only file yields zero
# statements, verified empirically); only the exit status is asserted.
SPLIT_TMP_FILE=""
split_to_file() {
  local file="$1" rc=0
  SPLIT_TMP_FILE="$(mktemp "${TMPDIR:-/tmp}/whe-split.XXXXXX")"
  sql_split "$file" > "$SPLIT_TMP_FILE" || rc=$?
  if (( rc != 0 )); then
    rm -f "$SPLIT_TMP_FILE"; SPLIT_TMP_FILE=""
    printf '[fail] sql_split.awk failed on %s (awk exit %d)\n' "$(basename "$file")" "$rc" >&2
    return 1
  fi
}

# ---------- run one SQL file ---------------------------------------------------
# run_sql_file <project-ref> <file.sql> — split + apply statement-by-statement.
# Fail-fast: stops at the first statement the server rejects, prints file,
# statement index, snippet and server error to stderr, and RETURNS 1 (does not
# exit) so callers choose fatal (migrations) vs non-fatal (template seed).
run_sql_file() {
  local ref="$1" file="$2"
  local stmt stmts=() active=() i n
  command -v awk >/dev/null || die "awk not found."
  [[ -f "$file" ]] || die "SQL file not found: $file"

  split_to_file "$file" || return 1
  while IFS= read -r -d $'\x1e' stmt; do stmts+=("$stmt"); done < "$SPLIT_TMP_FILE"
  rm -f "$SPLIT_TMP_FILE"; SPLIT_TMP_FILE=""
  if [[ ${#stmts[@]} -gt 0 ]]; then
    for stmt in "${stmts[@]}"; do
      [[ -z "${stmt//[[:space:]]/}" ]] && continue          # skip blank fragments
      active+=("$stmt")
    done
  fi
  n=${#active[@]}
  if [[ $n -eq 0 ]]; then ok "$(basename "$file"): no statements (skipped)"; return 0; fi

  log "applying $(basename "$file") ($n statements)"
  for ((i = 0; i < n; i++)); do
    if ! mgmt_query "$ref" "${active[$i]}"; then
      local snip="${active[$i]}"
      snip="${snip//$'\r'/ }"; snip="${snip//$'\n'/ }"
      {
        printf '\n'
        printf '[fail] %s failed at statement %d/%d:\n' "$(basename "$file")" "$((i + 1))" "$n"
        printf '  statement: %.200s\n' "$snip"
        printf '  server error: %s\n' "$MGMT_LAST_ERROR"
        printf '\n  Fix the issue and re-run — all migration files are idempotent, so re-applying\n  from the start is always safe (see scripts/migrate.sh header).\n'
      } >&2
      return 1
    fi
    rm -f "${MGMT_RESPONSE_FILE:-}"
    printf '\r  [%d/%d] statements applied' "$((i + 1))" "$n"
  done
  printf '\n'
  ok "$(basename "$file") applied"
}

# ---------- enumerate migrations -----------------------------------------------
# collect_migrations — fills the global MIGRATION_FILES array with
# db/migrations/*.sql in LC_ALL=C lexicographic order. The zero-padded NNNN_
# prefix convention makes lexicographic == chronological; a file breaking the
# convention triggers a loud warning rather than a silent mis-ordering.
MIGRATION_FILES=()
collect_migrations() {
  local f raw=() shard_dir_probe=""
  local f raw=() shard_dir_probe=""
  # DIRECTORY SAFETY GUARD: this runner globs ONE migration tree per run.
  # Engine mode (default) applies the ENGINE project's own migrations from
  # db/migrations/ — db/shard-migrations/ (0015_wh_query_rpc.sql +
  # 0016_seal_roll_off.sql) targets SERVING SHARDS via its manifest-driven
  # path — see the 0015 header ("APPLY TARGET: each SERVING SHARD …").
  # --shard flips the expected tree, so the guard below is MODE-CONDITIONAL:
  # it dies loudly on a shard-migrations override ONLY in engine mode
  # (WHE_SHARD_MODE != 1). The files share the NNNN_ prefix convention, so
  # a filename check cannot discriminate; the guard matches the DIRECTORY
  # instead (raw name or resolved realpath, so symlinks/.. cannot sneak
  # past).
  if [[ "${WHE_SHARD_MODE:-0}" != "1" ]]; then
    shard_dir_probe="$(basename "${MIGRATIONS_DIR%/}")"
    [[ "$shard_dir_probe" != "shard-migrations" ]] || \
      die "MIGRATIONS_DIR points at a shard-migrations tree ($MIGRATIONS_DIR) — engine mode applies ONLY db/migrations/ (the engine project). Shard migrations target the shard databases and MUST NOT be applied here; pass --shard (or set WHE_SHARD_MODE=1) to apply them."
    shard_dir_probe="$(cd "$MIGRATIONS_DIR" >/dev/null 2>&1 && pwd -P || true)"
    [[ -z "$shard_dir_probe" || "$(basename "$shard_dir_probe")" != "shard-migrations" ]] || \
      die "MIGRATIONS_DIR resolves into a shard-migrations tree ($shard_dir_probe) — engine mode applies ONLY db/migrations/ (the engine project). Shard migrations target the shard databases and MUST NOT be applied here; pass --shard (or set WHE_SHARD_MODE=1) to apply them."
  fi
  for f in "$MIGRATIONS_DIR"/*.sql; do
    if [[ -f "$f" ]]; then raw+=("$f"); fi
  done
  [[ ${#raw[@]} -gt 0 ]] || die "no migration files found in $MIGRATIONS_DIR"
  MIGRATION_FILES=()
  while IFS= read -r f; do MIGRATION_FILES+=("$f"); done < <(printf '%s\n' "${raw[@]}" | LC_ALL=C sort)
  for f in "${MIGRATION_FILES[@]}"; do
    if [[ ! "$(basename "$f")" =~ ^[0-9]{4}_ ]]; then
      warn "$(basename "$f"): does not start with a NNNN_ prefix — lexicographic order may not match the intended migration order"
    fi
  done
}

# ---------- apply all migrations ------------------------------------------------
apply_all_migrations() {
  local ref="$1" f
  collect_migrations
  command -v awk >/dev/null || die "awk not found."
  log "applying ${#MIGRATION_FILES[@]} migration(s) from ${MIGRATIONS_DIR%/}/ in lexicographic order:"
  for f in "${MIGRATION_FILES[@]}"; do log "  - $(basename "$f")"; done
  for f in "${MIGRATION_FILES[@]}"; do
    run_sql_file "$ref" "$f" || \
      die "$(basename "$f") failed (see error above). Fix the cause and re-run — all migration files are idempotent, so re-applying from the start is safe."
  done
  ok "all ${#MIGRATION_FILES[@]} migration file(s) applied"
}

# ---------- verification ---------------------------------------------------------
# verify_migrations <project-ref> — checks the 0013/0014/0016 engine artifacts
# in the Postgres catalogs. Every checked artifact must genuinely
# DISCRIMINATE its migration (a check that also passes on a pre-catalog DB
# is a false-pass). Engine-scoped ON PURPOSE: --shard mode skips this
# function entirely — the shard migrations carry their own in-SQL
# verify-gate DO blocks (0015), so no runner-side sweep exists for them.
# The kept gates:
#   0013 → the unified warehouse catalog (r38). to_regclass existence for the
#          five new tables + wh_bump_directory_version; the TWO cross-writer
#          partial unique indexes get pg_index gates (indisunique AND non-
#          empty indpred AND the pg_get_expr deparse carries the predicate
#          tokens — 'NOT is_reference' on one_serving_per_span vs 'is_reference'
#          WITHOUT 'NOT' on one_serving_ref_per_shard, plus the
#          serving/draining state tokens — token-level like 0010 because
#          pg_get_expr normalizes the IN-list to = ANY (ARRAY[...])); the
#          directory view rides the 0007 idiom (security_invoker reloption +
#          anon AND authenticated privilege revokes — Supabase default
#          privileges grant both SELECT, so both probes discriminate; the
#          reloption probe also catches the create-or-replace-view RESETS-
#          reloptions hazard on re-runs); the config row pins the r37 P1-3
#          SCALAR encoding law (jsonb_typeof='number' + a ^[0-9]+$ text guard
#          — a string '1', an object {"v":1}, a float or a negative FAILS,
#          and the guarded CASE returns false INSTEAD of erroring the whole
#          verify query the way a bare ::bigint cast would); relrowsecurity
#          on all five tables (bool_and — a missing table is caught by its
#          own existence probe first); and all FOUR bump triggers counted
#          per-table (=2 each) WITH tgfoid = wh_bump_directory_version —
#          tables-without-triggers is a DEAD version counter (engine caches
#          never invalidate) that only this probe sees.
#   0014 → the loader ledger-write RPC fm_loader_bookkeep (r41). 0014 ships
#          NO verify-gate DO blocks of its own (the r43 verify-gate DO-block
#          pattern arrives in the SHARD migration 0015, which is NOT applied
#          here), so verify_migrations mirrors the post-conditions 0014's
#          lockdown section (r41 audit F4) and the 0012 triple-revoke
#          doctrine pin:
#            (a) to_regproc('public.fm_loader_bookkeep') — the name is
#                0014-only and 0014 creates EXACTLY ONE identity (the
#                17-argument fm_loader_bookkeep(text,uuid,uuid,text,bigint,
#                text,text,text,uuid,integer,text,text,integer,text,bigint,
#                uuid[],text[]) — NOT a (text,int,text,jsonb,jsonb) shape),
#                so a NULL oid ⇒ 0014 absent;
#            (b) BODY gates on prosrc (r5 S4 discipline — exact raise texts
#                and the r41-F2 FIXED predicate, not bare tokens): the P1-6
#                overlap-gate raise 'finalize: % overlapping serving/draining
#                span(s) on table % (P1-6 overlap gate)', the §2.5
#                migrate-first raise 'ensure_job: schema_version % <> table
#                % (migrate-first gate)', the lease missing-partition raise
#                'lease: partition row %/%/% does not exist
#                (ensure_partitions first)', the NULL-bounds overlap
#                predicate '(a.key_min is null or b.key_max is null or
#                a.key_min < b.key_max)' (the pre-F2 body used IS NOT NULL
#                guards and CANNOT contain it — a bounded finalize
#                co-existing with a serving unbounded span is the exact
#                wrong-SUM class P1-6 exists to close), and the §2.2
#                stateless-loader lease-decision reasons 'lease_held' /
#                'not_leaseable';
#            (c) OWNERSHIP + ACL posture: proowner must be postgres (the
#                Management-API identity that applied 0001-0014) and the
#                function must stay INVOKER (0014 ships no SECURITY DEFINER;
#                the loader calls as service_role, which holds bypassrls);
#                EXECUTE must be gone for anon AND authenticated while
#                service_role keeps the lifeline (the 0012 trio). Each
#                not-revoked probe ALSO sees PUBLIC's aclitem (aclmask
#                matches the PUBLIC entry for every role), so the default
#                PUBLIC EXECUTE grant or a DROP+CREATE ACL reset fails the
#                anon probe. The probes resolve the function by OID
#                (to_regproc → strict NULL → coalesce), NOT by the
#                17-argument text identity — a PRE-0014 DB then reaches the
#                precise 0014 die below instead of the whole verify query
#                dying on an unresolvable name (the 0012 idiom's failure
#                mode);
#            (d) LEDGER contract: load_jobs + load_partitions RLS on (their
#                EXISTENCE rides the 0013 probes, die-ordered first) and the
#                EXACT state-machine columns the RPC's actions read/write —
#                load_partitions {state, lease_until, attempts, last_error,
#                rows_sent, checksum, shard_checksum, updated_at} and
#                load_jobs {schema_version, finished_at} — counted via
#                information_schema (the 0004 idiom): a dropped/renamed
#                column breaks only the action that touches it and only this
#                probe sees it.
#   0016 → the seal-only roll-off (contract §2.3): the ONE watermark pair is
#          pinned at the ENCODING level (jsonb-SCALAR number — a string
#          "0.80", an object, a float or a negative all FAIL; the guarded
#          CASE returns false INSTEAD of erroring the whole verify query,
#          the same death-proofing the 0013 config gate pins); the derived
#          threshold fn is probed STRUCTURALLY (a CALL here would let a
#          missing config row kill the whole verify SELECT — the value
#          behavior is battery-pinned, not verify-pinned); the selection
#          view rides the 0013 view idiom (security_invoker reloption +
#          anon/authenticated revokes — the create-or-replace-view RESETS-
#          reloptions hazard applies on every re-run); fm_rolloff_finalize
#          is pinned by BODY tokens (kind/codec/state + the fail-closed
#          from_blocks provenance, overlap-daterange and advisory-lock
#          guards) plus the triple-revoke ACL posture with the
#          service_role lifeline.
verify_migrations() {
  local ref="$1" row \
    ropct16 rusable16 rtf16 rov16 rosi16 roan16 roau16 rofn16 robod16 roan16f roau16f rosr16f
  log "verifying applied migration state (pg catalogs)"
  if ! mgmt_query "$ref" "
select
  -- 0013 (warehouse catalog): the five new tables. Each to_regclass is NULL
  -- on a pre-0013 DB, so existence alone discriminates the migration.
  to_regclass('public.warehouse_tables')::text        as warehouse_tables_0013,
  to_regclass('public.warehouse_placements')::text    as warehouse_placements_0013,
  to_regclass('public.warehouse_cold_objects')::text  as warehouse_cold_objects_0013,
  to_regclass('public.load_jobs')::text               as load_jobs_0013,
  to_regclass('public.load_partitions')::text         as load_partitions_0013,
  -- 0013 (SKILL §10 cross-writer invariants): the TWO partial unique indexes
  -- must exist, be UNIQUE, and be PARTIAL with the right predicate — a plain
  -- (non-unique) or non-partial impostor, or a swapped predicate, fails.
  to_regclass('public.one_serving_per_span')::text      as one_serving_per_span_0013,
  to_regclass('public.one_serving_ref_per_shard')::text as one_serving_ref_per_shard_0013,
  (select pi.indisunique
     and pi.indpred is not null
     and position('NOT is_reference' in pg_get_expr(pi.indpred, pi.indrelid)) > 0
     and position('serving' in pg_get_expr(pi.indpred, pi.indrelid)) > 0
     and position('draining' in pg_get_expr(pi.indpred, pi.indrelid)) > 0
     from pg_index pi
    where pi.indexrelid = to_regclass('public.one_serving_per_span'))       as one_serving_per_span_pred_0013,
  (select pi.indisunique
     and pi.indpred is not null
     and position('is_reference' in pg_get_expr(pi.indpred, pi.indrelid)) > 0
     and position('NOT is_reference' in pg_get_expr(pi.indpred, pi.indrelid)) = 0
     and position('serving' in pg_get_expr(pi.indpred, pi.indrelid)) > 0
     and position('draining' in pg_get_expr(pi.indpred, pi.indrelid)) > 0
     from pg_index pi
    where pi.indexrelid = to_regclass('public.one_serving_ref_per_shard'))  as one_serving_ref_per_shard_pred_0013,
  -- 0013 (v_warehouse_directory): the 0007 idiom — security_invoker AND the
  -- anon/authenticated revokes. Supabase default privileges grant SELECT to
  -- BOTH roles, so each probe discriminates its revoke; the reloption probe
  -- also catches the create-or-replace-view RESETS-reloptions re-run hazard
  -- (the adjacent alter view in 0013 is load-bearing).
  (select coalesce(
     position('security_invoker=on' in coalesce(array_to_string(c.reloptions, ','), '')) > 0 or
     position('security_invoker=true' in coalesce(array_to_string(c.reloptions, ','), '')) > 0,
     false)
     from pg_class c
    where c.oid = to_regclass('public.v_warehouse_directory'))              as wh_view_security_invoker_0013,
  (select not coalesce(has_table_privilege(to_regrole('anon'), 'public.v_warehouse_directory', 'SELECT'), false))
                                                                             as wh_view_anon_revoked_0013,
  (select not coalesce(has_table_privilege(to_regrole('authenticated'), 'public.v_warehouse_directory', 'SELECT'), false))
                                                                             as wh_view_authenticated_revoked_0013,
  -- 0013 (r37 P1-3 SCALAR encoding law): jsonb_typeof pins scalar-number (a
  -- string '1' or an object {"v":1} FAILS — on an object the bump function's
  -- own (value #>> '{}')::bigint would throw on EVERY catalog write), and
  -- the ^[0-9]+$ text guard makes a float/negative return false INSTEAD of
  -- erroring the whole verify query (a bare ::bigint cast would 42703/22P02
  -- the SELECT into an opaque 'verification query failed').
  (select case
            when jsonb_typeof(value) = 'number' and (value #>> '{}') ~ '^[0-9]+$'
            then (value #>> '{}')::bigint >= 0
            else false
          end
     from public.config
    where key = 'warehouse_directory_version')                              as wh_dir_version_scalar_0013,
  -- 0013 (0001 doctrine enable+no-policies): relrowsecurity on ALL five new
  -- tables. bool_and over the five to_regclass oids — a table MISSING from
  -- the catalogs is caught by its own existence probe above (die-ordered
  -- first), a table present with RLS OFF fails here.
  (select bool_and(c.relrowsecurity)
     from pg_class c
    where c.oid in (to_regclass('public.warehouse_tables'),
                    to_regclass('public.warehouse_placements'),
                    to_regclass('public.warehouse_cold_objects'),
                    to_regclass('public.load_jobs'),
                    to_regclass('public.load_partitions')))                as wh_tables_rls_0013,
  -- 0013 (version-counter triggers): all FOUR must exist, non-internal, AND
  -- be bound to wh_bump_directory_version — counted per-table (=2 each) so a
  -- wrong name on the wrong table cannot reach 2. Tables-without-triggers is
  -- a dead version counter: engine caches never invalidate.
  (select (select count(*) from pg_trigger t
            where t.tgrelid = to_regclass('public.warehouse_placements')
              and not t.tgisinternal
              and t.tgname in ('wh_dir_bump_ins_del', 'wh_dir_bump_upd')
              and t.tgfoid = to_regproc('public.wh_bump_directory_version')) = 2
     and (select count(*) from pg_trigger t
            where t.tgrelid = to_regclass('public.warehouse_cold_objects')
              and not t.tgisinternal
              and t.tgname in ('wh_cold_bump_ins_del', 'wh_cold_bump_upd')
              and t.tgfoid = to_regproc('public.wh_bump_directory_version')) = 2) as wh_bump_triggers_0013,
  to_regproc('public.wh_bump_directory_version')::text                       as wh_bump_fn_0013,
  -- 0014 (r41): fm_loader_bookkeep — the ONE idempotent loader ledger-write
  -- endpoint. The name is 0014-only and 0014 creates EXACTLY ONE identity
  -- (the 17-argument (text,uuid,uuid,text,bigint,text,text,text,uuid,
  -- integer,text,text,integer,text,bigint,uuid[],text[]) function, no
  -- overloads), so to_regproc resolves it unambiguously; NULL oid ⇒ 0014
  -- absent.
  to_regproc('public.fm_loader_bookkeep')::text                  as fm_loader_bookkeep_0014,
  -- 0014 BODY discriminators (r5 S4 discipline: exact raise texts and the
  -- r41-F2 FIXED predicate, not bare tokens — a stale/pre-r41 body or a
  -- hand-stripped body fails): the P1-6 overlap-gate raise, the §2.5
  -- migrate-first raise, the lease missing-partition raise, the NULL-bounds
  -- overlap predicate (the pre-F2 body used IS NOT NULL guards and cannot
  -- contain it), and the §2.2 stateless-loader lease-decision reasons.
  (select position('finalize: % overlapping serving/draining span(s) on table % (P1-6 overlap gate)' in l.prosrc) > 0
    and position('ensure_job: schema_version % <> table % (migrate-first gate)' in l.prosrc) > 0
    and position('lease: partition row %/%/% does not exist (ensure_partitions first)' in l.prosrc) > 0
    and position('(a.key_min is null or b.key_max is null or a.key_min < b.key_max)' in l.prosrc) > 0
    and position('lease_held' in l.prosrc) > 0
    and position('not_leaseable' in l.prosrc) > 0
     from pg_proc l
    where l.oid = to_regproc('public.fm_loader_bookkeep'))     as fm_loader_bookkeep_body_0014,
  -- 0014 ownership posture: owned by postgres (the Management-API identity
  -- that applied every 0001-0014 object) and INVOKER-rights (0014 ships no
  -- SECURITY DEFINER — the loader calls as service_role with bypassrls; a
  -- definer flip is drift, not hardening).
  (select p.proowner = to_regrole('postgres')
    and not p.prosecdef
     from pg_proc p
    where p.oid = to_regproc('public.fm_loader_bookkeep'))     as fm_loader_bookkeep_owner_0014,
  -- 0014 ACL lockdown (r41 audit F4 / the 0012 triple-revoke doctrine):
  -- EXECUTE absent for anon AND authenticated AND PUBLIC, service_role
  -- lifeline intact (every loader call runs as service_role). Each
  -- not-revoked probe ALSO sees PUBLIC's aclitem — aclmask matches the
  -- PUBLIC entry for every role — so a lingering PUBLIC grant (or the
  -- default-privilege ACL a DROP+CREATE reset re-opens) fails the anon
  -- probe. The function is resolved by OID (to_regproc → strict NULL →
  -- coalesce), NOT by the 17-argument text identity, so a PRE-0014 DB gets
  -- the precise 0014 die below instead of an unresolvable-name error from
  -- the whole verify query (the 0012 idiom's failure mode).
  (select not coalesce(has_function_privilege(to_regrole('anon'),
      to_regproc('public.fm_loader_bookkeep')::oid, 'EXECUTE'), false))
                                                                 as fm_loader_bookkeep_anon_revoked_0014,
  (select not coalesce(has_function_privilege(to_regrole('authenticated'),
      to_regproc('public.fm_loader_bookkeep')::oid, 'EXECUTE'), false))
                                                                 as fm_loader_bookkeep_authenticated_revoked_0014,
  (select coalesce(has_function_privilege(to_regrole('service_role'),
      to_regproc('public.fm_loader_bookkeep')::oid, 'EXECUTE'), true))
                                                                 as fm_loader_bookkeep_service_role_lifeline_0014,
  -- 0014 ledger contract: load_jobs + load_partitions are 0013's tables
  -- (their EXISTENCE rides the 0013 probes, die-ordered first) — here: RLS
  -- on BOTH (0001 doctrine: enable + no policies; bool_and — a table present
  -- with RLS OFF fails; a table MISSING yields bool_and NULL → read false).
  (select bool_and(c.relrowsecurity)
     from pg_class c
    where c.oid in (to_regclass('public.load_jobs'),
                    to_regclass('public.load_partitions')))    as loader_ledger_rls_0014,
  -- 0014 ledger state-machine columns: the EXACT columns fm_loader_bookkeep's
  -- actions read/write, counted via information_schema (the 0004 idiom). A
  -- dropped/renamed column breaks only the action that touches it —
  -- invisible to every existence probe — and the RPC would 42703 mid-load.
  (select count(*) = 8 from information_schema.columns
     where table_schema = 'public' and table_name = 'load_partitions'
       and column_name in ('state', 'lease_until', 'attempts', 'last_error',
                           'rows_sent', 'checksum', 'shard_checksum', 'updated_at'))
                                                                 as load_partitions_lease_cols_0014,
  (select count(*) = 2 from information_schema.columns
     where table_schema = 'public' and table_name = 'load_jobs'
       and column_name in ('schema_version', 'finished_at'))
                                                                 as load_jobs_state_cols_0014,
  -- 0016 seal-only roll-off: the ONE watermark constants (contract §2.3 —
  -- jsonb-SCALAR number encoding; a string "0.80", an object, a float or a
  -- negative all FAIL — the same self-inconsistency class the 0013 gate
  -- pins for warehouse_directory_version). A MISSING row makes the scalar
  -- subquery return NULL → the explicit-boolean jq read yields "" → the die
  -- below fires; a row present with a wrong encoding yields false.
  (select coalesce(jsonb_typeof(value) = 'number'
                   and (value #>> '{}')::text = '0.80', false)
     from public.config where key = 'roll_off_threshold_pct')        as rolloff_pct_scalar_0016,
  (select coalesce(jsonb_typeof(value) = 'number'
                   and (value #>> '{}')::bigint = 450000000, false)
     from public.config where key = 'warehouse_usable_bytes')        as warehouse_usable_scalar_0016,
  -- the derived threshold fn: STRUCTURAL body probe only (a CALL here would
  -- let a missing config row kill the whole verify SELECT — the 0013
  -- precedent keeps the verify query death-proof; the value behavior is
  -- battery-pinned, not verify-pinned).
  (select position('roll_off_threshold_pct' in l.prosrc) > 0
     and position('warehouse_usable_bytes' in l.prosrc) > 0
     and position('raise exception' in l.prosrc) > 0
    from pg_proc l
   where l.oid = to_regproc('public.warehouse_roll_off_threshold_bytes')) as rolloff_threshold_fn_0016,
  -- the selection view: existence + security_invoker + client-key revokes
  -- (the 0013 view idiom; create or replace view RESETS reloptions — the
  -- adjacent alter in 0016 is load-bearing on every re-run).
  to_regclass('public.v_warehouse_rolloff_candidates')::text          as rolloff_view_0016,
  (select coalesce(c.reloptions @> array['security_invoker=true'], false)
     from pg_class c
    where c.oid = to_regclass('public.v_warehouse_rolloff_candidates')) as rolloff_view_security_invoker_0016,
  (select not coalesce(has_table_privilege(to_regrole('anon'),
      'public.v_warehouse_rolloff_candidates', 'SELECT'), false))     as rolloff_view_anon_revoked_0016,
  (select not coalesce(has_table_privilege(to_regrole('authenticated'),
      'public.v_warehouse_rolloff_candidates', 'SELECT'), false))     as rolloff_view_authenticated_revoked_0016,
  -- fm_rolloff_finalize: existence + body pins (kind/codec/state tokens) +
  -- the 0012 ACL triple (anon/authenticated revoked; service_role lifeline —
  -- the orchestration cron invokes as service_role).
  to_regproc('public.fm_rolloff_finalize')::text                      as fm_rolloff_finalize_0016,
  (select position('packed_blocks' in l.prosrc) > 0
     and position('jsonb+lz4' in l.prosrc) > 0
     and position('promoted' in l.prosrc) > 0
     and position('state = ''serving''' in l.prosrc) > 0
     and position('p_from_blocks is null or p_from_blocks' in l.prosrc) > 0
     and position('from_blocks provenance flag must be surfaced and false' in l.prosrc) > 0
     and position('pg_advisory_xact_lock(hashtextextended(' in l.prosrc) > 0
     and position('daterange(' in l.prosrc) > 0
     and position('object_path is distinct from p_object_path' in l.prosrc) > 0
     and position('unparseable/empty day range' in l.prosrc) > 0
     and position('is not a serving placement for table' in l.prosrc) > 0
     and position('pass p_placement_id' in l.prosrc) > 0
    from pg_proc l
   where l.oid = to_regproc('public.fm_rolloff_finalize'))            as fm_rolloff_finalize_body_0016,
  (select not coalesce(has_function_privilege(to_regrole('anon'),
      to_regproc('public.fm_rolloff_finalize')::oid, 'EXECUTE'), false)) as fm_rolloff_finalize_anon_revoked_0016,
  (select not coalesce(has_function_privilege(to_regrole('authenticated'),
      to_regproc('public.fm_rolloff_finalize')::oid, 'EXECUTE'), false)) as fm_rolloff_finalize_authenticated_revoked_0016,
  (select coalesce(has_function_privilege(to_regrole('service_role'),
      to_regproc('public.fm_rolloff_finalize')::oid, 'EXECUTE'), true))  as fm_rolloff_finalize_service_role_lifeline_0016,
"; then
    die "verification query failed: $MGMT_LAST_ERROR"
  fi
  row="$(cat "$MGMT_RESPONSE_FILE")"
  rm -f "$MGMT_RESPONSE_FILE"

  wt13="$(jq   -r '.[0].warehouse_tables_0013         // ""' <<<"$row")"
  wp13="$(jq   -r '.[0].warehouse_placements_0013     // ""' <<<"$row")"
  wc13="$(jq   -r '.[0].warehouse_cold_objects_0013   // ""' <<<"$row")"
  lj13="$(jq   -r '.[0].load_jobs_0013                // ""' <<<"$row")"
  lp13="$(jq   -r '.[0].load_partitions_0013          // ""' <<<"$row")"
  wsp13="$(jq  -r '.[0].one_serving_per_span_0013      // ""' <<<"$row")"
  wrs13="$(jq  -r '.[0].one_serving_ref_per_shard_0013 // ""' <<<"$row")"
  wspp13="$(jq -r '.[0].one_serving_per_span_pred_0013     | if . == null then "" else tostring end' <<<"$row")"
  wrsp13="$(jq -r '.[0].one_serving_ref_per_shard_pred_0013 | if . == null then "" else tostring end' <<<"$row")"
  wsi13="$(jq  -r '.[0].wh_view_security_invoker_0013       | if . == null then "" else tostring end' <<<"$row")"
  wan13="$(jq  -r '.[0].wh_view_anon_revoked_0013           | if . == null then "" else tostring end' <<<"$row")"
  wau13="$(jq  -r '.[0].wh_view_authenticated_revoked_0013  | if . == null then "" else tostring end' <<<"$row")"
  wdv13="$(jq  -r '.[0].wh_dir_version_scalar_0013          | if . == null then "" else tostring end' <<<"$row")"
  wrls13="$(jq -r '.[0].wh_tables_rls_0013                  | if . == null then "" else tostring end' <<<"$row")"
  wtrg13="$(jq -r '.[0].wh_bump_triggers_0013               | if . == null then "" else tostring end' <<<"$row")"
  wbf13="$(jq  -r '.[0].wh_bump_fn_0013                     // ""' <<<"$row")"
  flb14="$(jq   -r '.[0].fm_loader_bookkeep_0014                   // ""' <<<"$row")"
  flbb14="$(jq  -r '.[0].fm_loader_bookkeep_body_0014              | if . == null then "" else tostring end' <<<"$row")"
  flbo14="$(jq  -r '.[0].fm_loader_bookkeep_owner_0014             | if . == null then "" else tostring end' <<<"$row")"
  flban14="$(jq -r '.[0].fm_loader_bookkeep_anon_revoked_0014      | if . == null then "" else tostring end' <<<"$row")"
  flbau14="$(jq -r '.[0].fm_loader_bookkeep_authenticated_revoked_0014 | if . == null then "" else tostring end' <<<"$row")"
  flbsr14="$(jq -r '.[0].fm_loader_bookkeep_service_role_lifeline_0014 | if . == null then "" else tostring end' <<<"$row")"
  lrls14="$(jq  -r '.[0].loader_ledger_rls_0014                    | if . == null then "" else tostring end' <<<"$row")"
  lpc14="$(jq   -r '.[0].load_partitions_lease_cols_0014           | if . == null then "" else tostring end' <<<"$row")"
  ljc14="$(jq   -r '.[0].load_jobs_state_cols_0014                 | if . == null then "" else tostring end' <<<"$row")"
  ropct16="$(jq  -r '.[0].rolloff_pct_scalar_0016                        | if . == null then "" else tostring end' <<<"$row")"
  rusable16="$(jq  -r '.[0].warehouse_usable_scalar_0016                  | if . == null then "" else tostring end' <<<"$row")"
  rtf16="$(jq   -r '.[0].rolloff_threshold_fn_0016                    | if . == null then "" else tostring end' <<<"$row")"
  rov16="$(jq   -r '.[0].rolloff_view_0016                         // ""' <<<"$row")"
  rosi16="$(jq  -r '.[0].rolloff_view_security_invoker_0016           | if . == null then "" else tostring end' <<<"$row")"
  roan16="$(jq  -r '.[0].rolloff_view_anon_revoked_0016               | if . == null then "" else tostring end' <<<"$row")"
  roau16="$(jq  -r '.[0].rolloff_view_authenticated_revoked_0016      | if . == null then "" else tostring end' <<<"$row")"
  rofn16="$(jq  -r '.[0].fm_rolloff_finalize_0016                  // ""' <<<"$row")"
  robod16="$(jq -r '.[0].fm_rolloff_finalize_body_0016                | if . == null then "" else tostring end' <<<"$row")"
  roan16f="$(jq -r '.[0].fm_rolloff_finalize_anon_revoked_0016        | if . == null then "" else tostring end' <<<"$row")"
  roau16f="$(jq -r '.[0].fm_rolloff_finalize_authenticated_revoked_0016 | if . == null then "" else tostring end' <<<"$row")"
  rosr16f="$(jq -r '.[0].fm_rolloff_finalize_service_role_lifeline_0016 | if . == null then "" else tostring end' <<<"$row")"

  log "  warehouse_tables (0013):            ${wt13:-<MISSING>}"
  log "  warehouse_placements (0013):        ${wp13:-<MISSING>}"
  log "  warehouse_cold_objects (0013):      ${wc13:-<MISSING>}"
  log "  load_jobs (0013):                   ${lj13:-<MISSING>}"
  log "  load_partitions (0013):             ${lp13:-<MISSING>}"
  log "  one_serving_per_span (0013):        ${wsp13:-<MISSING>}"
  log "  one_serving_ref_per_shard (0013):   ${wrs13:-<MISSING>}"
  case "$wspp13" in
    true)  log "  one_serving_per_span gate (0013):   unique + partial (NOT is_reference, serving/draining)" ;;
    false) log "  one_serving_per_span gate (0013):   <DEGRADED — non-unique/non-partial impostor or predicate lost>" ;;
    *)     log "  one_serving_per_span gate (0013):   <UNKNOWN>" ;;
  esac
  case "$wrsp13" in
    true)  log "  one_serving_ref_per_shard gate (0013): unique + partial (is_reference, serving/draining)" ;;
    false) log "  one_serving_ref_per_shard gate (0013): <DEGRADED — non-unique/non-partial impostor or predicate lost>" ;;
    *)     log "  one_serving_ref_per_shard gate (0013): <UNKNOWN>" ;;
  esac
  case "$wsi13" in
    true)  log "  v_warehouse_directory security_invoker (0013): on (caller-privilege view)" ;;
    false) log "  v_warehouse_directory security_invoker (0013): <OFF — owner-privilege view bypasses the RLS 0001 doctrine>" ;;
    *)     log "  v_warehouse_directory security_invoker (0013): <MISSING view>" ;;
  esac
  case "$wan13" in
    true)  log "  v_warehouse_directory anon privilege (0013):          absent (revoked)" ;;
    false) log "  v_warehouse_directory anon privilege (0013):          <STILL GRANTED — anon can read the warehouse directory>" ;;
    *)     log "  v_warehouse_directory anon privilege (0013):          <UNKNOWN>" ;;
  esac
  case "$wau13" in
    true)  log "  v_warehouse_directory authenticated privilege (0013): absent (revoked)" ;;
    false) log "  v_warehouse_directory authenticated privilege (0013): <STILL GRANTED>" ;;
    *)     log "  v_warehouse_directory authenticated privilege (0013): <UNKNOWN>" ;;
  esac
  case "$wdv13" in
    true)  log "  config warehouse_directory_version (0013): scalar-number encoding (r37 P1-3 law)" ;;
    false) log "  config warehouse_directory_version (0013): <WRONG ENCODING — not a jsonb scalar number; every bump would throw>" ;;
    *)     log "  config warehouse_directory_version (0013): <MISSING>" ;;
  esac
  case "$wrls13" in
    true)  log "  warehouse catalog RLS (0013): enabled on all 5 tables (no policies)" ;;
    false) log "  warehouse catalog RLS (0013): <OFF on at least one of the 5 tables>" ;;
    *)     log "  warehouse catalog RLS (0013): <UNKNOWN>" ;;
  esac
  case "$wtrg13" in
    true)  log "  version-counter triggers (0013): all 4 present, bound to wh_bump_directory_version" ;;
    false) log "  version-counter triggers (0013): <INCOMPLETE — dead version counter: engine caches never invalidate>" ;;
    *)     log "  version-counter triggers (0013): <UNKNOWN>" ;;
  esac
  log "  wh_bump_directory_version (0013):   ${wbf13:-<MISSING>}"
  log "  fm_loader_bookkeep (0014):          ${flb14:-<MISSING>}"
  log "  roll-off watermark (0016):          pct=${ropct16:-<MISSING>} usable=${rusable16:-<MISSING>} fn=${rtf16:-<MISSING>}"
  log "  roll-off selection view (0016):     ${rov16:-<MISSING>} (security_invoker=${rosi16:-<UNKNOWN>})"
  log "  fm_rolloff_finalize (0016):         ${rofn16:-<MISSING>}"
  case "$flbb14" in
    true)  log "  fm_loader_bookkeep body (0014):     r41 gates present (P1-6 overlap + migrate-first + lease raises)" ;;
    false) log "  fm_loader_bookkeep body (0014):     <PRESENT but STALE/PRE-r41 BODY (gate raises missing)>" ;;
    *)     log "  fm_loader_bookkeep body (0014):     <UNKNOWN>" ;;
  esac
  case "$flbo14" in
    true)  log "  fm_loader_bookkeep owner (0014):    postgres + invoker-rights" ;;
    false) log "  fm_loader_bookkeep owner (0014):    <WRONG — not owned by postgres, or a SECURITY DEFINER flip>" ;;
    *)     log "  fm_loader_bookkeep owner (0014):    <UNKNOWN>" ;;
  esac
  case "$flban14" in
    true)  log "  fm_loader_bookkeep anon EXECUTE (0014):          revoked" ;;
    false) log "  fm_loader_bookkeep anon EXECUTE (0014):          <STILL GRANTED — the anon key can drive the loader ledger RPC>" ;;
    *)     log "  fm_loader_bookkeep anon EXECUTE (0014):          <UNKNOWN>" ;;
  esac
  case "$flbau14" in
    true)  log "  fm_loader_bookkeep authenticated EXECUTE (0014): revoked" ;;
    false) log "  fm_loader_bookkeep authenticated EXECUTE (0014): <STILL GRANTED>" ;;
    *)     log "  fm_loader_bookkeep authenticated EXECUTE (0014): <UNKNOWN>" ;;
  esac
  case "$flbsr14" in
    true)  log "  fm_loader_bookkeep service_role EXECUTE (0014):  lifeline intact" ;;
    false) log "  fm_loader_bookkeep service_role EXECUTE (0014):  <LIFELINE BROKEN — every loader call runs as service_role>" ;;
    *)     log "  fm_loader_bookkeep service_role EXECUTE (0014):  <UNKNOWN>" ;;
  esac
  case "$lrls14" in
    true)  log "  loader ledger RLS (0014):           enabled on load_jobs + load_partitions" ;;
    false) log "  loader ledger RLS (0014):           <OFF on at least one ledger table (or table missing)>" ;;
    *)     log "  loader ledger RLS (0014):           <UNKNOWN>" ;;
  esac
  case "$lpc14" in
    true)  log "  load_partitions lease columns (0014): all 8 state-machine columns present" ;;
    false) log "  load_partitions lease columns (0014): <INCOMPLETE — an RPC action would 42703 mid-load>" ;;
    *)     log "  load_partitions lease columns (0014): <UNKNOWN>" ;;
  esac
  case "$ljc14" in
    true)  log "  load_jobs columns (0014):           schema_version + finished_at present" ;;
    false) log "  load_jobs columns (0014):           <INCOMPLETE — migrate-first gate / finalize stamp would 42703>" ;;
    *)     log "  load_jobs columns (0014):           <UNKNOWN>" ;;
  esac

  if [[ -z "$wt13" || -z "$wp13" || -z "$wc13" || -z "$lj13" || -z "$lp13" ]]; then
    local missing13t=""
    [[ -z "$wt13" ]] && missing13t+="public.warehouse_tables "
    [[ -z "$wp13" ]] && missing13t+="public.warehouse_placements "
    [[ -z "$wc13" ]] && missing13t+="public.warehouse_cold_objects "
    [[ -z "$lj13" ]] && missing13t+="public.load_jobs "
    [[ -z "$lp13" ]] && missing13t+="public.load_partitions "
    die "migration 0013 (unified warehouse catalog) is NOT applied — the engine has NO routing truth (every warehouse query 4xxs at plan time) and the loader ledger has no catalog anchor (missing: ${missing13t}).
  Re-run: scripts/migrate.sh --project-ref $ref   (idempotent, safe to re-apply)"
  fi
  if [[ -z "$wsp13" || -z "$wrs13" || "$wspp13" != "true" || "$wrsp13" != "true" ]]; then
    local missing13i=""
    [[ -z "$wsp13" ]]         && missing13i+="one_serving_per_span(missing) "
    [[ -z "$wrs13" ]]         && missing13i+="one_serving_ref_per_shard(missing) "
    [[ "$wspp13" != "true" ]] && missing13i+="one_serving_per_span(not unique+partial NOT-is_reference-serving/draining) "
    [[ "$wrsp13" != "true" ]] && missing13i+="one_serving_ref_per_shard(not unique+partial is_reference-serving/draining) "
    die "migration 0013 (unified warehouse catalog) is NOT applied correctly — the TWO cross-writer partial unique indexes (SKILL §10: storage constraints, not app locks) are missing or degraded; concurrent loader-finalize + reshard flips could double-serve a key span or double-serve a reference shard (missing: ${missing13i}).
  Re-run: scripts/migrate.sh --project-ref $ref   (idempotent — create unique index if not exists; if the INITIAL creation fails loudly on a dirty directory, resolve the duplicate rows per the engine catalog's dedupe doctrine first — hand-fix the directory to one serving span / one reference shard per key, then re-run)"
  fi
  if [[ "$wsi13" != "true" || "$wan13" != "true" || "$wau13" != "true" ]]; then
    local missing13v=""
    [[ "$wsi13" != "true" ]] && missing13v+="security_invoker(on) "
    [[ "$wan13" != "true" ]] && missing13v+="anon-revoke "
    [[ "$wau13" != "true" ]] && missing13v+="authenticated-revoke "
    die "migration 0013 (v_warehouse_directory hardening) is NOT fully applied — the engine's routing view bypasses table RLS (owner-privilege view) or leaks directory rows to client keys (missing: ${missing13v}).
  Re-run: scripts/migrate.sh --project-ref $ref   (idempotent — NOTE: create or replace view RESETS reloptions, so the adjacent alter view … set (security_invoker = true) is LOAD-BEARING on every re-run; both ship in 0013)"
  fi
  if [[ "$wdv13" != "true" ]]; then
    die "migration 0013 (directory version counter) is NOT applied correctly — the config row 'warehouse_directory_version' is MISSING or its value is NOT the pinned jsonb SCALAR-number encoding (r37 P1-3 law: seed to_jsonb(1::bigint), read (value #>> '{}')::bigint). A string '1', an object {\"v\":1}, a float or a negative all FAIL this gate — on an object the bump function's own (value #>> '{}')::bigint would throw on EVERY catalog write.
  Self-healing: re-run scripts/migrate.sh --project-ref $ref (the seed is on conflict (key) do nothing, so a WRONG-ENCODING row pre-existing from a hand-edit survives re-runs — fix it explicitly: update public.config set value = to_jsonb(1::bigint) where key = 'warehouse_directory_version';)"
  fi
  if [[ "$wrls13" != "true" ]]; then
    die "migration 0013 (warehouse catalog RLS) is NOT fully applied — relrowsecurity is OFF on at least one of the 5 catalog tables (0001 doctrine: enable + no policies ⇒ no access via anon/authenticated keys).
  Re-run: scripts/migrate.sh --project-ref $ref   (idempotent — the five alter table … enable row level security statements are no-ops once on)"
  fi
  if [[ "$wtrg13" != "true" || -z "$wbf13" ]]; then
    local missing13g=""
    [[ -z "$wbf13" ]]         && missing13g+="public.wh_bump_directory_version() "
    [[ "$wtrg13" != "true" ]] && missing13g+="bump triggers (need wh_dir_bump_ins_del + wh_dir_bump_upd on warehouse_placements AND wh_cold_bump_ins_del + wh_cold_bump_upd on warehouse_cold_objects, all bound to wh_bump_directory_version) "
    die "migration 0013 (version-counter triggers) is NOT fully applied — a DEAD version counter: catalog writes stop bumping warehouse_directory_version, so engine consumers cache a stale directory FOREVER (caches never invalidate), invisible to every other probe here (missing: ${missing13g}).
  Re-run: scripts/migrate.sh --project-ref $ref   (idempotent — drop trigger if exists + create trigger; the ins/del triggers carry NO WHEN clause because an INSERT trigger's WHEN cannot reference OLD values, the upd triggers pin WHEN (old.* is distinct from new.*))"
  fi
  if [[ -z "$flb14" ]]; then
    die "migration 0014 (loader ledger RPC) is NOT applied — public.fm_loader_bookkeep is MISSING entirely: the loader's ONE idempotent ledger-write endpoint does not exist, so every loader bookkeep/lease/verify/finalize call fails and load_partitions never advances (the loader is stateless — the ledger is sole truth).
  Re-run: scripts/migrate.sh --project-ref $ref   (idempotent, safe to re-apply)"
  fi
  if [[ "$flbb14" != "true" ]]; then
    die "migration 0014 (loader ledger RPC) is NOT applied correctly — fm_loader_bookkeep exists but its body is STALE/PRE-r41: prosrc lacks the gate raises the r41 body carries (the P1-6 overlap-gate raise, the §2.5 migrate-first raise, the lease missing-partition raise, and the r41-F2 NULL-bounds overlap predicate '(a.key_min is null or b.key_max is null or a.key_min < b.key_max)' — the pre-F2 body used IS NOT NULL guards and let a bounded finalize co-exist with a serving unbounded span, the exact wrong-SUM class P1-6 exists to close).
  Re-run: scripts/migrate.sh --project-ref $ref   (idempotent — create or replace rewrites the body in place and preserves ACLs)"
  fi
  if [[ "$flbo14" != "true" ]]; then
    die "migration 0014 (loader ledger RPC) owner posture is WRONG — fm_loader_bookkeep must be owned by postgres (the Management-API identity that applied every 0001-0014 object) and stay INVOKER-rights (0014 ships no SECURITY DEFINER; the loader calls as service_role with bypassrls, so a definer flip is drift, not hardening).
  Fix: alter function public.fm_loader_bookkeep owner to postgres; (and re-apply 0014 to reset the definer flag), then re-run --verify-only"
  fi
  if [[ "$flban14" != "true" || "$flbau14" != "true" || "$flbsr14" != "true" ]]; then
    local missing14=""
    [[ "$flban14" != "true" ]] && missing14+="anon-EXECUTE-still-granted "
    [[ "$flbau14" != "true" ]] && missing14+="authenticated-EXECUTE-still-granted "
    [[ "$flbsr14" != "true" ]] && missing14+="service_role-LIFELINE-BROKEN-EXECUTE-lost "
    die "migration 0014 (loader ledger RPC lockdown, r41 audit F4) is NOT applied correctly — anon/authenticated can still drive the ledger-write RPC (lease/loaded/verify/fail/FINALIZE = full ledger + placement control from a client key), or the service_role lifeline broke (missing: ${missing14}).
  Re-run: scripts/migrate.sh --project-ref $ref   (idempotent — the revoke is a no-op once the desired state holds). A still-granted probe usually means the function was DROP+CREATE'd out-of-band (DROP resets ACLs → the PUBLIC EXECUTE default returns; the anon probe sees PUBLIC's aclitem because has_function_privilege matches the PUBLIC entry for every role — the engine loader-RPC lockdown doctrine). If the service_role lifeline broke, re-grant it explicitly before anything else: every loader call runs as service_role."
  fi
  if [[ "$lrls14" != "true" || "$lpc14" != "true" || "$ljc14" != "true" ]]; then
    local missing14l=""
    [[ "$lrls14" != "true" ]] && missing14l+="ledger-RLS-off-or-table-missing "
    [[ "$lpc14" != "true" ]]  && missing14l+="load_partitions state-machine columns incomplete (need state, lease_until, attempts, last_error, rows_sent, checksum, shard_checksum, updated_at) "
    [[ "$ljc14" != "true" ]]  && missing14l+="load_jobs columns incomplete (need schema_version, finished_at) "
    die "migration 0014 (loader ledger contract) is NOT fully satisfied — the ledger tables fm_loader_bookkeep writes are missing RLS or the exact state-machine columns its actions read/write; a dropped/renamed column makes the RPC 42703 mid-load and only this probe sees it (missing: ${missing14l}).
  Re-run: scripts/migrate.sh --project-ref $ref   (idempotent — 0013's create table if not exists statements restore the schema; ledger-table EXISTENCE itself is die-ordered under the 0013 gates above)"
  fi
  if [[ -z "$ropct16" || -z "$rusable16" ]]; then
    die "migration 0016 (seal-only roll-off watermark) is NOT applied — the config rows 'roll_off_threshold_pct' / 'warehouse_usable_bytes' are MISSING (probe returned empty). The engine roll-off orchestration cannot decide WHEN to seal a shard off.
  Re-run: scripts/migrate.sh --project-ref $ref   (idempotent — BUT the seeds are on conflict (key) do nothing, so a WRONG-ENCODING row pre-existing from a hand-edit survives re-runs; fix explicitly: update public.config set value = to_jsonb(0.80::numeric) where key='roll_off_threshold_pct'; update public.config set value = to_jsonb(450000000::bigint) where key='warehouse_usable_bytes';)"
  fi
  if [[ "$ropct16" != "true" || "$rusable16" != "true" ]]; then
    local missing16w=""
    [[ "$ropct16" != "true" ]] && missing16w+="roll_off_threshold_pct(not-jsonb-number-'0.80') "
    [[ "$rusable16" != "true" ]] && missing16w+="warehouse_usable_bytes(not-jsonb-number-450000000) "
    die "migration 0016 (seal-only roll-off watermark) is NOT applied correctly — the config rows exist but violate the pinned jsonb-SCALAR number encoding (contract §2.3 ONE-watermark doctrine; same self-inconsistency class the 0013 gate pins for warehouse_directory_version): ${missing16w}. On a string "0.80" the derived threshold fn would throw on EVERY invocation.
  Fix explicitly: update public.config set value = to_jsonb(0.80::numeric) where key='roll_off_threshold_pct'; update public.config set value = to_jsonb(450000000::bigint) where key='warehouse_usable_bytes';"
  fi
  if [[ -z "$rtf16" ]]; then
    die "migration 0016 (seal-only roll-off threshold fn) is NOT applied — public.warehouse_roll_off_threshold_bytes is MISSING entirely: the selection view cannot compute the 360000000 fire line (0.80 × 450000000).
  Re-run: scripts/migrate.sh --project-ref $ref   (idempotent — create or replace rewrites the body in place)"
  fi
  if [[ "$rtf16" != "true" ]]; then
    die "migration 0016 (roll-off threshold fn) body is STALE/FOREIGN — prosrc lacks the two config-key reads and/or the missing-config raise (structural probe; a fn that silently returns NULL on missing config would make the view select NOTHING, or worse EVERYTHING, with no error).
  Re-run: scripts/migrate.sh --project-ref $ref   (idempotent)"
  fi
  if [[ -z "$rov16" ]]; then
    die "migration 0016 (roll-off selection view) is NOT applied — public.v_warehouse_rolloff_candidates is MISSING: no selection surface exists for the roll-off cron, so no shard ever seals and 500MB shards fill to the hard wall.
  Re-run: scripts/migrate.sh --project-ref $ref   (idempotent)"
  fi
  if [[ "$rosi16" != "true" || "$roan16" != "true" || "$roau16" != "true" ]]; then
    local missing16v=""
    [[ "$rosi16" != "true" ]] && missing16v+="security_invoker(on) "
    [[ "$roan16" != "true" ]] && missing16v+="anon-revoke "
    [[ "$roau16" != "true" ]] && missing16v+="authenticated-revoke "
    die "migration 0016 (roll-off selection view hardening) is NOT fully applied — the view runs with owner privileges (bypassing its guards' assumptions) or leaks roll-off candidates to client keys (missing: ${missing16v}).
  Re-run: scripts/migrate.sh --project-ref $ref   (idempotent — NOTE: create or replace view RESETS reloptions, so the adjacent alter view … set (security_invoker = true) is LOAD-BEARING on every re-run; both ship in 0016)"
  fi
  if [[ -z "$rofn16" ]]; then
    die "migration 0016 (roll-off finalize RPC) is NOT applied — public.fm_rolloff_finalize is MISSING entirely: a completed in-shard seal_roll_off_day cannot land in the catalog (no cold_objects row, no bytes_used reset), so the directory keeps routing reads to data that was just deleted from the shard.
  Re-run: scripts/migrate.sh --project-ref $ref   (idempotent, safe to re-apply)"
  fi
  if [[ "$robod16" != "true" ]]; then
    die "migration 0016 (roll-off finalize RPC) body is STALE/FOREIGN — prosrc lacks the packed_blocks/jsonb+lz4/'promoted' tokens, the serving-placement gate 'state = ''serving''', or the r45 shape-pass guards (from_blocks fail-closed provenance, advisory-lock serialization, daterange overlap guard with the is-distinct carve-out, unparseable-day-range raise, p_placement_id disambiguation): a finalize that accepts non-serving placements, from_blocks:true checksums, or overlapping day ranges would land cold_objects rows the directory double-counts.
  Re-run: scripts/migrate.sh --project-ref $ref   (idempotent — create or replace rewrites the body in place and preserves ACLs)"
  fi
  if [[ "$roan16f" != "true" || "$roau16f" != "true" || "$rosr16f" != "true" ]]; then
    local missing16f=""
    [[ "$roan16f" != "true" ]] && missing16f+="anon-EXECUTE-still-granted "
    [[ "$roau16f" != "true" ]] && missing16f+="authenticated-EXECUTE-still-granted "
    [[ "$rosr16f" != "true" ]] && missing16f+="service_role-LIFELINE-BROKEN-EXECUTE-lost "
    die "migration 0016 (roll-off finalize lockdown, 0012/0014 doctrine) is NOT applied correctly — anon/authenticated can still finalize roll-offs (catalog + placement control from a client key), or the service_role lifeline broke (missing: ${missing16f}).
  Re-run: scripts/migrate.sh --project-ref $ref   (idempotent — the revoke is a no-op once the desired state holds). A still-granted probe usually means the function was DROP+CREATE'd out-of-band (DROP resets ACLs → the PUBLIC EXECUTE default returns — the engine RPC lockdown doctrine). If the service_role lifeline broke, re-grant it explicitly: every engine roll-off call runs as service_role."
  fi
  ok "migration state verified: 0013+0014+0016 engine artifacts present (warehouse catalog + cross-writer partial-unique invariants + hardened directory view + bump triggers; loader ledger RPC fm_loader_bookkeep + body/owner/ACL gates + ledger columns; roll-off seal watermark + threshold fn + hardened selection view + fm_rolloff_finalize)"
}

# ---------- dry run (no network) ---------------------------------------------------
dry_run_all() {
  local f stmt n total=0
  command -v awk >/dev/null || die "awk not found."
  collect_migrations
  log "DRY RUN — would apply ${#MIGRATION_FILES[@]} migration file(s) in this order:"
  for f in "${MIGRATION_FILES[@]}"; do
    n=0
    split_to_file "$f" || return 1
    while IFS= read -r -d $'\x1e' stmt; do
      [[ -z "${stmt//[[:space:]]/}" ]] && continue
      n=$((n + 1))
    done < "$SPLIT_TMP_FILE"
    rm -f "$SPLIT_TMP_FILE"; SPLIT_TMP_FILE=""
    total=$((total + n))
    log "  $(basename "$f"): $n statement(s)"
  done
  ok "dry run complete: $total statement(s) total (no network calls made)"
}

# ---------- standalone entry point ---------------------------------------------------
if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  set -euo pipefail
  REF="${WHE_PROJECT_REF:-}"
  MODE="apply"
  SHARD=0
  EXTRA_FILE=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --project-ref)    REF="${2:?--project-ref needs a value}"; shift 2 ;;
      --project-ref=*)  REF="${1#*=}"; shift ;;
      --verify-only)    MODE="verify"; shift ;;
      --dry-run)        MODE="dry"; shift ;;
      --shard)          SHARD=1; shift ;;
      --file)           EXTRA_FILE="${2:?--file needs a path}"; MODE="file"; shift 2 ;;
      --file=*)         EXTRA_FILE="${1#*=}"; MODE="file"; shift ;;
      -h|--help)        grep '^#' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
      *) die "unknown argument: $1 (try --help)" ;;
    esac
  done
  if [[ "$SHARD" == "1" ]]; then
    WHE_SHARD_MODE=1
    # Shard mode: retarget the glob to db/shard-migrations/ — a pre-exported
    # MIGRATIONS_DIR still wins over the --shard default.
    [[ -n "${WHE_MIGRATIONS_DIR_PRESET:-}" ]] || \
      MIGRATIONS_DIR="$MIGRATE_SCRIPT_DIR/../db/shard-migrations"
  fi
  case "$MODE" in
    dry)
      dry_run_all
      ;;
    verify)
      if [[ "$SHARD" == "1" ]]; then
        die "--shard mode has NO runner-side catalog verify — verify_migrations is engine-scoped and the shard migrations carry their own in-SQL verify-gate DO blocks (0015); re-run without --verify-only"
      fi
      [[ -n "$REF" ]] || die "no project ref: pass --project-ref REF or set WHE_PROJECT_REF in .env"
      verify_migrations "$REF"
      ;;
    file)
      [[ -n "$REF" ]] || die "no project ref: pass --project-ref REF or set WHE_PROJECT_REF in .env"
      [[ -n "$EXTRA_FILE" ]] || die "--file needs a path"
      run_sql_file "$REF" "$EXTRA_FILE" || die "SQL file failed (see error above)"
      ;;
    apply)
      [[ -n "$REF" ]] || die "no project ref: pass --project-ref REF or set WHE_PROJECT_REF in .env"
      [[ -n "${SUPABASE_ACCESS_TOKEN:-}" ]] || die "SUPABASE_ACCESS_TOKEN not set (source .env first)"
      apply_all_migrations "$REF"
      # Shard mode SKIPS verify_migrations entirely (engine-scoped; the shard
      # migrations carry their own in-SQL verify-gate DO blocks).
      if [[ "$SHARD" != "1" ]]; then verify_migrations "$REF"; fi
      ;;
  esac
fi
