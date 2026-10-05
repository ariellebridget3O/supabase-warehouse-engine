// =============================================================================
// _shared/templates_test.ts — offline pins for the whe v0.1.1 migrate surface
// (the adapted scripts/migrate.sh + scripts/sql_split.awk + shard templates).
// =============================================================================
// Harness ported from supabase-fleet-manager's _shared/templates_test.ts
// (module-level passed/failed/failures[] ledger + eq() JSON-compare + a
// terminal __report__ Deno.test that throws when failed > 0). fm's t1/t3/t4/
// t5/t6 groups are deliberately NOT ported: they pin fm-only surfaces (the
// 0011 edge-proxy templates, ../fleet-api/index.ts, ./rotation.ts, the
// management-api.ts splitter) that do not exist in this repo — and fm's t7
// ok()-summary pin rode fm's 0001-0017 gate set, which whe's pruned
// 0013/0014/0016 engine subset replaces.
//
// Seams under test (all offline; migrate.sh is imported as TEXT and never
// executed — module-graph text imports need no filesystem permission, the
// same mechanism as the wh_*_test.ts source pins; only the manifest/W1–W5
// recompute touches disk via the --allow-read the battery grants — the
// wh_handshake_test.ts:501 readTextFileSync precedent):
//   * scripts/migrate.sh — the LETHAL PINS: the verify-gate probes for the
//     engine verify subset (0013/0014/0016), the NEW ok() summary, BOTH
//     collect_migrations shard-guard dies, the engine-default glob +
//     shard-migrations site confinement, and the env contract
//     (WHE_PROJECT_REF / SUPABASE_ACCESS_TOKEN in; the fm token/rotation/
//     probe set negatively pinned out).
//   * db/shard-templates/manifest.json + W1..W6 — RECOMPUTE each row's
//     sha256 from the file bytes and eq it to row.template_hash: this pulls
//     scripts/lint_shard_templates.py's body-of-record invariant inside CI,
//     so a file/manifest drift fails the battery too, not just the lint.
//   * db/migrations/0013|0014|0016 + db/shard-migrations/0015|0016 — one
//     identity fragment per tree (plus cross-tree negative pins) so a
//     wrong-dir copy goes red.
//   * scripts/sql_split.awk — the RS=0x1e emission + dollar-tag state
//     machine, matched against migrate.sh's `read -d $'\x1e'` consumption
//     (the two halves of the splitter contract).
//
// KNOWN LIMIT (fm r15 audit F5 class): these are STATIC source pins — they
// prove the probe/die TEXT exists in the file, but a mutant that deletes a
// die CONDITION while retaining its text (fm's F5 class) passes a substring
// pin. The conditions themselves need a live Management API to exercise,
// which is out of scope for the offline battery by design. The pins raise
// the cost of drift; they do not eliminate it.
// =============================================================================

import migrateShSrc from '../../../scripts/migrate.sh' with { type: 'text' };
import sqlSplitAwkSrc from '../../../scripts/sql_split.awk' with { type: 'text' };
import migration0013 from '../../../db/migrations/0013_warehouse_catalog.sql' with { type: 'text' };
import migration0014 from '../../../db/migrations/0014_loader_rpc.sql' with { type: 'text' };
import migration0016Engine from '../../../db/migrations/0016_rolloff_seal.sql' with { type: 'text' };
import shardMigration0015 from '../../../db/shard-migrations/0015_wh_query_rpc.sql' with { type: 'text' };
import shardMigration0016 from '../../../db/shard-migrations/0016_seal_roll_off.sql' with { type: 'text' };

let passed = 0;
let failed = 0;
const failures: string[] = [];

function eq(name: string, actual: unknown, expected: unknown): void {
  const a = JSON.stringify(actual);
  const e = JSON.stringify(expected);
  if (a === e) {
    passed++;
    console.log(`  ok  ${name}`);
  } else {
    failed++;
    failures.push(`${name}\n      expected: ${e}\n      actual:   ${a}`);
    console.error(`FAIL  ${name}\n      expected: ${e}\n      actual:   ${a}`);
  }
}

// -----------------------------------------------------------------------------
// t1 — VERIFY-GATE PROBES (engine verify subset 0013/0014/0016). Probe strings
// pinned verbatim (fm t7/t8 precedent) so a gate cannot silently drift.
// -----------------------------------------------------------------------------
Deno.test('migrate.sh source — engine verify-gate probes: 0013 catalog tables, the 42703 killer (pi.indrelid), explicit-boolean jq reads, loader + rolloff-finalize probes', () => {
  eq('the 0013 table probes are wired verbatim (5 to_regclass existence columns — NULL on a pre-0013 DB)',
    migrateShSrc.includes("to_regclass('public.warehouse_tables')::text")
    && migrateShSrc.includes("to_regclass('public.warehouse_placements')::text")
    && migrateShSrc.includes("to_regclass('public.warehouse_cold_objects')::text")
    && migrateShSrc.includes("to_regclass('public.load_jobs')::text")
    && migrateShSrc.includes("to_regclass('public.load_partitions')::text"), true);
  eq('the partial-unique-index gates use the REAL pg_index column indrelid (fm r9 shipped pg_get_expr(pi.indpred, pi.indrel) — pi.indrel does not exist, 42703, and the WHOLE verify SELECT died before reaching any gate; the mutant fragment must stay out)',
    migrateShSrc.includes('pg_get_expr(pi.indpred, pi.indrelid)')
    && !migrateShSrc.includes('pi.indrel)'), true);
  eq('the boolean gates are read with the EXPLICIT-boolean jq idiom — exactly 26 occurrences on the landed verify subset (the plain // "" idiom swallows a JSON false and would false-pass a degraded gate)',
    migrateShSrc.split('| if . == null then "" else tostring end').length - 1, 26);
  eq('the 0014 loader-RPC probe is wired (to_regproc fm_loader_bookkeep — 0014 creates EXACTLY ONE identity) with the r41 body gate pinned (P1-6 overlap-gate raise in prosrc)',
    migrateShSrc.includes("to_regproc('public.fm_loader_bookkeep')::text")
    && migrateShSrc.includes("position('finalize: % overlapping serving/draining span(s) on table % (P1-6 overlap gate)' in l.prosrc) > 0"), true);
  eq('the 0016 rolloff-finalize probe is wired (to_regproc fm_rolloff_finalize) with the shape-pass body pinned (per-table advisory xact lock)',
    migrateShSrc.includes("to_regproc('public.fm_rolloff_finalize')::text")
    && migrateShSrc.includes("position('pg_advisory_xact_lock(hashtextextended(' in l.prosrc) > 0"), true);
  eq('oid/text probes ride the // "" idiom (the fm-lineage jq read of the 0013 anchor column)',
    migrateShSrc.includes('.[0].warehouse_tables_0013         // ""'), true);
});

// -----------------------------------------------------------------------------
// t2 — THE NEW ok() SUMMARY (:884). Rewritten for the pruned engine subset —
// the pin follows the NEW bytes (fm r33 R-9 precedent) and its negative twin
// (0001-0017) lives in the t5 negative set.
// -----------------------------------------------------------------------------
Deno.test('migrate.sh source — the ok() summary rides the 0013+0014+0016 engine subset (RED vs a lazy full-port regression to fm gate-set wording)', () => {
  eq('ok() summary names the engine verify subset (the deterministic fragment of the :884 line)',
    migrateShSrc.includes('0013+0014+0016 engine artifacts present'), true);
  eq('ok() summary still carries the full 0016 tail (watermark + threshold fn + hardened selection view + fm_rolloff_finalize — the summary covers the subset, not a stub)',
    migrateShSrc.includes('roll-off seal watermark + threshold fn + hardened selection view + fm_rolloff_finalize'), true);
});

// -----------------------------------------------------------------------------
// t3 — BOTH SHARD-GUARD DIES (collect_migrations). The directory guard is
// MODE-CONDITIONAL: it dies on a shard-migrations override ONLY in engine
// mode — pin both the two die texts and the guard condition itself.
// -----------------------------------------------------------------------------
Deno.test('migrate.sh source — BOTH collect_migrations shard-guard die texts + the mode-conditional guard condition', () => {
  eq('guard die #1: the raw-name probe dies on a shard-migrations MIGRATIONS_DIR in engine mode',
    migrateShSrc.includes('MIGRATIONS_DIR points at a shard-migrations tree'), true);
  eq('guard die #2: the resolved-realpath probe also dies (symlinks/.. cannot sneak past)',
    migrateShSrc.includes('MIGRATIONS_DIR resolves into a shard-migrations tree'), true);
  eq('the guard is MODE-CONDITIONAL (engine mode = WHE_SHARD_MODE != 1) — pinning the condition is the closest a static pin gets to the r15 F5 limit',
    migrateShSrc.includes('if [[ "${WHE_SHARD_MODE:-0}" != "1" ]]; then'), true);
});

// -----------------------------------------------------------------------------
// t4 — SHARPENED STATIC ASSERTIONS: the engine-mode default glob stays
// db/migrations/, and every shard-migrations site is a LEGIT site (guard text,
// the --shard retarget assignment, the header/docs lines).
// -----------------------------------------------------------------------------
Deno.test('migrate.sh source — engine default dir is db/migrations (NOT shard-migrations); shard-migrations sites confined to legit sites', () => {
  eq('the engine-mode default assignment is present verbatim (the constants-block glob default)',
    migrateShSrc.includes('MIGRATIONS_DIR="${MIGRATIONS_DIR:-$MIGRATE_SCRIPT_DIR/../db/migrations}"'), true);
  eq('that default line does NOT point at shard-migrations (extract the assignment line and inspect it — RED vs retargeting the default)',
    (() => {
      const line = migrateShSrc.split('\n').find((l) => l.startsWith('MIGRATIONS_DIR="${MIGRATIONS_DIR:-'));
      return typeof line === 'string'
        && line.includes('$MIGRATE_SCRIPT_DIR/../db/migrations')
        && !line.includes('shard-migrations');
    })(), true);
  eq('shard-migrations occurrences confined to legit sites (≥4: guard text + the --shard dir assignment + header docs; 13 on the landed file — split length = occurrences + 1)',
    migrateShSrc.split('shard-migrations').length >= 4, true);
  eq('the --shard retarget assignment is present (a pre-exported MIGRATIONS_DIR still wins over the --shard default)',
    migrateShSrc.includes('MIGRATIONS_DIR="$MIGRATE_SCRIPT_DIR/../db/shard-migrations"'), true);
});

// -----------------------------------------------------------------------------
// t5 — ENV-NAME PINS + the NEGATIVE SET. The negatives kill the lazy
// full-port regression: an unadapted copy of fm's migrate.sh carries
// FLEET_PROJECT_REF / FLEET_TOKEN / try_start_rotation / fleet_promote /
// edge_proxy_sql_0011 and the 0001-0017 summary — every one must be GONE.
// -----------------------------------------------------------------------------
Deno.test('migrate.sh source — env contract: WHE_PROJECT_REF + SUPABASE_ACCESS_TOKEN in; the fm token/rotation/probe set OUT', () => {
  eq('WHE_PROJECT_REF present (≥2 occurrences: env contract doc + the standalone REF default + the die texts; 6 on the landed file)',
    migrateShSrc.split('WHE_PROJECT_REF').length - 1 >= 2, true);
  eq('SUPABASE_ACCESS_TOKEN present (PAT-only mgmt_query: env contract + the mgmt_query guard + the standalone apply gate)',
    migrateShSrc.includes('SUPABASE_ACCESS_TOKEN'), true);
  eq('NEGATIVE pins — the fm gate set is fully retired (FLEET_PROJECT_REF env, FLEET_TOKEN, the rotation hook, the fleet_promote RPC probes, the 0011 edge-proxy gates, and the 0001-0017 summary: a lazy full-port fails here)',
    !migrateShSrc.includes('FLEET_PROJECT_REF')
    && !migrateShSrc.includes('FLEET_TOKEN')
    && !migrateShSrc.includes('try_start_rotation')
    && !migrateShSrc.includes('fleet_promote')
    && !migrateShSrc.includes('edge_proxy_sql_0011')
    && !migrateShSrc.includes('0001-0017 artifacts present'), true);
});

// -----------------------------------------------------------------------------
// t6 — MANIFEST RECOMPUTE: sha256(W1..W5 file bytes) == manifest.template_hash.
// manifest.json is the sha256-pinned body-of-record (never reformat); this
// recomputes each row's digest from the ACTUAL file bytes so file+manifest
// drift fails the battery even where the python lint does not run.
// -----------------------------------------------------------------------------
Deno.test('manifest recompute — sha256(W1..W6 file bytes) == manifest.template_hash (lint-templates invariant inside CI)', async () => {
  const manifestBytes = await Deno.readFile(new URL('../../../db/shard-templates/manifest.json', import.meta.url));
  const manifest = JSON.parse(new TextDecoder().decode(manifestBytes)) as Array<{ slug: string; file: string; template_hash: string }>;
  // r129 re-pin (design_r128_joinplans.md §2.3 — the W6 provenance cell this
  // census law requires; census agent-ctx/r129-pincensus.md §3.3
  // templates_test row): W6_colocated_join_agg APPENDED at index 5 — the
  // append-only law (W1..W5 keep their pinned order) — and the per-row
  // sha256 recompute loop below AUTO-covers W6 (lethal, never weakened).
  eq('manifest has exactly 6 template rows (W1..W5 + r129 W6_colocated_join_agg)', manifest.length, 6);
  eq('manifest slugs are the pinned six, in order (W6 appended at index 5)', manifest.map((r) => r.slug),
    ['W1_grouped_sum_count', 'W2_scalar_minmax', 'W3_scalar_avg_pair', 'W4_topk', 'W5_cold_agg', 'W6_colocated_join_agg']);
  eq('every row\'s file is the slug + .sql (no stray file/slug drift inside the manifest)',
    manifest.every((r) => r.file === `${r.slug}.sql`), true);
  for (const row of manifest) {
    const bytes = await Deno.readFile(new URL(`../../../db/shard-templates/${row.file}`, import.meta.url));
    const digest = await crypto.subtle.digest('SHA-256', bytes);
    const hex = [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, '0')).join('');
    eq(`sha256(file bytes) == manifest.template_hash for ${row.slug} (a reformatted/re-exported template goes RED here too)`,
      hex, row.template_hash);
  }
});

// -----------------------------------------------------------------------------
// t7 — MIGRATION-FILE IDENTITY PINS: one distinctive fragment per tree, plus
// cross-tree negative pins so a wrong-dir copy goes red.
// -----------------------------------------------------------------------------
Deno.test('migration files — per-tree identity fragments (engine 0013/0014/0016 vs shard 0015/0016; wrong-dir copy goes red)', () => {
  eq('0013 (engine) creates the warehouse catalog anchor table',
    migration0013.includes('create table if not exists public.warehouse_tables'), true);
  eq('0014 (engine) creates the loader ledger-write RPC fm_loader_bookkeep',
    migration0014.includes('fm_loader_bookkeep'), true);
  eq('0015 (shard) carries its in-SQL verify-gate DO block (runner-side verify is SKIPPED in shard mode — the shard self-verifies via $verify_wh_registry$)',
    shardMigration0015.includes('do $verify_wh_registry$'), true);
  eq('engine 0016 creates the roll-off selection view (distinctive to db/migrations/)',
    migration0016Engine.includes('create or replace view public.v_warehouse_rolloff_candidates'), true);
  eq('shard 0016 creates the in-shard seal_roll_off_day function (distinctive to db/shard-migrations/)',
    shardMigration0016.includes('create or replace function seal_roll_off_day(p_day date)'), true);
  eq('the engine-0016 view create is ABSENT from shard 0016 (a wrong-dir copy would double-apply the catalog half onto a shard)',
    shardMigration0016.includes('create or replace view public.v_warehouse_rolloff_candidates'), false);
  eq('the shard seal-fn create is ABSENT from engine 0016 (comments/raise lore mentioning seal_roll_off_day are fine — the create is not)',
    migration0016Engine.includes('create or replace function seal_roll_off_day(p_day date)'), false);
});

// -----------------------------------------------------------------------------
// t8 — SQL_SPLIT.AWK: the splitter halves must agree. The awk emits statements
// separated by ASCII 0x1e (RS); migrate.sh consumes exactly that delimiter.
// -----------------------------------------------------------------------------
Deno.test('sql_split.awk — RS 0x1e emission + dollar-tag state machine, consumed by migrate.sh via read -d', () => {
  eq('the awk emits statements separated by ASCII 30 = 0x1e (the documented RS contract)',
    sqlSplitAwkSrc.includes('printf "%s%c", cur, 30'), true);
  eq('the $tag$ dollar-quote state machine is present (function bodies survive intact)',
    sqlSplitAwkSrc.includes('if (dollar != "")'), true);
  eq('migrate.sh consumes the SAME delimiter (read -r -d $\'\\x1e\' — both halves of the splitter contract in lockstep)',
    migrateShSrc.includes("read -r -d $'\\x1e'"), true);
});

// -----------------------------------------------------------------------------
// Harness report (hand-rolled runner, no external deps).
// -----------------------------------------------------------------------------
Deno.test('__report__', () => {
  console.log(`\ntemplates_test: ${passed} assertions passed, ${failed} failed`);
  if (failed > 0) throw new Error(`${failed} assertion(s) failed:\n${failures.join('\n---\n')}`);
});
