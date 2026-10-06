// =============================================================================
// _shared/wh_join_test.ts — r129 JOIN-PLAN battery (design_r128_joinplans §6).
// =============================================================================
// Lethal, hand-computed, offline. The oracle is the CANONICAL artifact
// audit/r128_join_oracle/join_oracle.json (sha256
// e5959512546b9d00591af265c944911101314c5ce8cdf9470618d09b7f298172, audit-B
// independently recomputed) ported verbatim into ./wh_fixtures/E15_join.json —
// E15's numbers ARE the expectations; nothing here is derived from a run.
// Corpus law (design §4): 14k rows, ids 0..13999; the replicated dim
// wh_probe_dim omits g47 — the g47 omission IS the join discriminator (a
// join-skipper emits 50 regions → EXACT row-set mismatch → RED).
//
// Arms (§6.1-§6.7): parse (absent byte-identical re-pin / valid / each
// malformed variant fixed-string / no-echo), plan gate (colocation ✓,
// is_reference false/absent, missing + duplicate dim placement, undefined
// dim population, template-class, key binding M-J3, D2 ownership of empty
// derivation), envelope/merge (E15 through the REAL merge path — monoid
// closure + finalize EXACT 49 + dim-labeled envelope_invalid), manifest
// statics (W6 row deep-equality + rows-kind body TEXT pins, lint parity),
// E15 provenance, rpcParams ({} + optional limit — no new params), perShard
// r133 (design_r132_w7_family §5/§6.4): E16_tier2 — the W7 tier2 oracle = the
// sha-gated PROJECTION of the same banked join_oracle.json (17 rows;
// scripts/r133_tier2_oracle.py). Arms: fixture statics + SET-INCLUSION
// (E16 ⊂ E15, keys AND values), 17-row monoid closure + full-pipeline W7
// wave (finalize EXACT 17 + perShard 6/6/5) + the W7 dim-labeled
// envelope_invalid twin.
// additive ok-arm presence + exact-shape pins (the R-3 law on the new
// surface), the entrypoint :464-478 dispatch-population split, and the R-B3
// rpcMode decision pin (see the block comment there).
// r144 R9 W8 (agent-ctx/r140-w7-census.md §6): the FIRST grouped-avg join
// template — derivation census e2e (tier2-avg⇒[W8H]/sum⇒[W7H]/mixed+base
// avg⇒D2 plan_untemplated), E16 avg-pair monoid closure + 3-shard fanout
// (partials 17×3, exact rationals), the twin-consumption pin (avg fusion +
// direct count off the SAME c encoding), the K-W8 fusion-gate lethal arm,
// and the fetch_rows grouped p_params={} arm.
//
// Offline + pure: every transport is an injected recording fake — zero
// sockets, no --allow-net (the battery never grants it; a real fetch would
// throw PermissionDenied and fail closed). The only disk reads are the E15
// fixture + the W6 template body (the --allow-read the battery grants).
// =============================================================================

import {
  adaptWireEnvelope,
  buildMergePlan,
  executeWhQuery,
  parseWhEngineRequest,
  rpcParams,
  WhEngineError,
} from './wh_engine_core.ts';
import type {
  WhDirectoryRow,
  WhEngineTimers,
  WhMergePlan,
  WhRpcSpec,
  WhShardFetcher,
} from './wh_engine_core.ts';
import { deriveTemplateHashes, ENGINE_TEMPLATE_MANIFEST, manifestRowByHash } from './wh_handshake.ts';
import { finalizeGroups, mergeGroupedPartials, rankComparator } from './wh_merge.ts';
import type { WhGroupFinal } from './wh_merge.ts';
import { WhMergeError } from './wh_types.ts';
import type { WhPartialEnvelope } from './wh_types.ts';
import { handleWhEngineRequest } from './wh_entrypoint.ts';
import type { WhEngineDeps } from './wh_entrypoint.ts';
import { deepEq, show } from './wh_testutil.ts';
import type { WhEngineRequest } from './wh_engine_core.ts';

let passed = 0;
let failed = 0;

function eq(name: string, actual: unknown, expected: unknown): void {
  if (deepEq(actual, expected)) {
    passed++;
    console.log(`  ok  ${name}`);
  } else {
    failed++;
    console.error(`FAIL  ${name}\n      expected: ${show(expected)}\n      actual:   ${show(actual)}`);
  }
}

function eqTrue(name: string, actual: boolean): void {
  eq(name, actual, true);
}

function throwsEngine(
  name: string,
  fn: () => unknown,
  code: WhEngineError['code'],
  msgPart?: string,
): void {
  try {
    fn();
    failed++;
    console.error(`FAIL  ${name} — expected WhEngineError(${code}), got no throw`);
  } catch (err) {
    if (
      err instanceof WhEngineError && err.code === code &&
      (msgPart === undefined || (err as Error).message.includes(msgPart))
    ) {
      passed++;
      console.log(`  ok  ${name} (WhEngineError ${code}${msgPart ? ', message names it' : ''})`);
    } else {
      failed++;
      console.error(`FAIL  ${name} — wrong throw: ${err}`);
    }
  }
}

async function rejectsEngine(
  name: string,
  fn: () => Promise<unknown>,
  code: WhEngineError['code'],
  msgPart?: string,
): Promise<void> {
  try {
    await fn();
    failed++;
    console.error(`FAIL  ${name} — expected WhEngineError(${code}), got no throw`);
  } catch (err) {
    if (
      err instanceof WhEngineError && err.code === code &&
      (msgPart === undefined || (err as Error).message.includes(msgPart))
    ) {
      passed++;
      console.log(`  ok  ${name} (WhEngineError ${code}${msgPart ? ', message names it' : ''})`);
    } else {
      failed++;
      console.error(`FAIL  ${name} — wrong throw: ${err}`);
    }
  }
}

function throwsMerge(name: string, fn: () => unknown, code: WhMergeError['code']): void {
  try {
    fn();
    failed++;
    console.error(`FAIL  ${name} — expected WhMergeError(${code}), got no throw`);
  } catch (err) {
    if (err instanceof WhMergeError && err.code === code) {
      passed++;
      console.log(`  ok  ${name} (WhMergeError ${code})`);
    } else {
      failed++;
      console.error(`FAIL  ${name} — wrong throw: ${err}`);
    }
  }
}

// -----------------------------------------------------------------------------
// Fixtures — hashes pinned from db/shard-templates/manifest.json.
// -----------------------------------------------------------------------------
const W1H = 'a934e7e062f59cff5a856afdc7aa743ec9be11c068c7e861ea856c36b40bdbfd';
// r129: W6 = sha256 of db/shard-templates/W6_colocated_join_agg.sql (the body
// IS the contract) — the join-class row APPENDED at manifest index 5
// (provenance: design_r128_joinplans.md §2.3; never widened silently).
const W6H = '7004f44de62a8e998ce1915348be0f0fc299ac1aae901966ae7080f7c2cc9576';
// r133 (design_r132_w7_family.md §2.2): W7 = sha256 of
// db/shard-templates/W7_dim_tier_join_agg.sql (W6's body + the ONE delta
// line `and d.tier = 2` on the ON clause; 482 bytes LF-only no-trailing-NL)
// — the tier2 join-class row APPENDED at manifest index 6 (provenance:
// design_r132_w7_family.md §2.2; never widened silently).
const W7H = 'e6d40cbe1d5da2587492c07076b97ec1e716deaf5cefa2b3098038bee98b79bb';
// r144 (agent-ctx/r140-w7-census.md §6): W8 = sha256 of
// db/shard-templates/W8_dim_tier_join_avg.sql (W7's body with the ONE delta
// BYTE `x`→`s` on the row_json wire key; 482 bytes LF-only no-trailing-NL)
// — the tier2 GROUPED-AVG join-class row APPENDED at manifest index 7
// (provenance: agent-ctx/r140-w7-census.md §6; never widened silently).
const W8H = 'bed23e35a457c534824e634e3f863742415d8077db36828bca2f630131968e86';

/** E15 — the canonical join oracle ported verbatim (§6.5). The numbers are
 *  the audit-B-confirmed hand constants; the fixture's meta.source pins the
 *  port provenance (path + sha256). */
interface E15Row { region: string; x: number; c: number; n: number }
interface E15Fixture {
  id: string;
  meta: { fixture: string; source: string; doc: string };
  rows: E15Row[];
  excluded: Record<string, { x: number; c: number; n: number }>;
  globals: { rows: number; sum_c: number; sum_n: number; sum_x: number };
}
const E15 = JSON.parse(
  Deno.readTextFileSync(new URL('./wh_fixtures/E15_join.json', import.meta.url)),
) as E15Fixture;

// r133 (design_r132_w7_family §5): E16 — the W7 tier2 oracle = the sha-gated
// PROJECTION of the same banked join_oracle.json (17 tier-2 rows; generator
// scripts/r133_tier2_oracle.py, ALL hand-pin gates green pre-emit). Same
// provenance law as E15: the fixture's numbers ARE the expectations.
interface E16Fixture {
  id: string;
  meta: { fixture: string; source: string; doc: string };
  rows: E15Row[];
  excluded: { note: string };
  globals: { rows: number; sum_c: number; sum_n: number; sum_x: number };
  per_band: Record<'A' | 'B' | 'C', { n: number; sum_x: number }>;
}
const E16 = JSON.parse(
  Deno.readTextFileSync(new URL('./wh_fixtures/E16_tier2.json', import.meta.url)),
) as E16Fixture;

const COLS: Record<string, string> = { region: 'text', amount: 'numeric' };
const SCALES: Record<string, number> = { amount: 0 };

/** Never-firing injected timers (counter clock — deterministic test ids). */
function neverTimeoutTimers(): WhEngineTimers {
  let n = 100;
  return {
    nowMs: () => ++n,
    startTimeout: (_ms: number) => ({ promise: new Promise<'timeout'>(() => {}), dispose: () => {} }),
  };
}

/** Fact-side directory row (wh_probe_agg placement). */
function factRow(shard: string): WhDirectoryRow {
  return {
    shard,
    key_min: null,
    key_max: null,
    hash_slot: null,
    state: 'serving',
    platform_status: 'ACTIVE_HEALTHY',
    schema_version: 1,
    last_health_at: '2026-09-28T00:00:00.000000Z',
    logical_name: 'wh_probe_agg',
    shard_key_type: 'none',
    shard_key_column: null,
    table_schema_version: 1,
  };
}

/** Dim-side directory row (wh_probe_dim broadcast placement). is_reference
 *  is the FAIL-CLOSED gate input — absent ≠ true (design §2.2). */
function dimRow(shard: string, opts: { isReference?: boolean; state?: 'serving' | 'draining' } = {}): WhDirectoryRow {
  return {
    shard,
    key_min: null,
    key_max: null,
    hash_slot: null,
    state: opts.state ?? 'serving',
    platform_status: 'ACTIVE_HEALTHY',
    schema_version: 1,
    last_health_at: '2026-09-28T00:00:00.000000Z',
    logical_name: 'wh_probe_dim',
    shard_key_type: 'none',
    shard_key_column: null,
    table_schema_version: 9, // deliberately ≠ the fact's 1: if the dim ever displaced the fact head, the schema gate would trip (the split's head-stays-fact discriminator)
    ...(opts.isReference !== undefined ? { is_reference: opts.isReference } : {}),
  };
}

/** The §2.1 join request through the REAL parse: sum(amount)→x,
 *  count(amount)→c (the count-col select law), count(*)→n, grouped by
 *  region, joining wh_probe_dim on region=region. */
function joinReqBody(overrides: { query?: Record<string, unknown>; table?: string } = {}): Record<string, unknown> {
  return {
    v: 1,
    qid: '01J9Q1ZZZZZZZZZZZZZZZZZZZZ',
    table: overrides.table ?? 'wh_probe_agg',
    query: {
      select: [
        { op: 'sum', col: 'amount', alias: 'x' },
        { op: 'count', col: 'amount', alias: 'c' },
        { op: 'count', alias: 'n' },
      ],
      groupBy: ['region'],
      join: { table: 'wh_probe_dim', type: 'inner', on: { left: 'region', right: 'region' } },
      ...(overrides.query ?? {}),
    },
  };
}

/** W6-shaped grouped partial (the exact per-shard shape the W6 template
 *  emits post-adaptation: k:[region], a:{x: fixed-point TEXT, c, n}). */
function w6env(shard: string, rows: E15Row[]): WhPartialEnvelope {
  return {
    v: 1,
    shard,
    table: 'wh_probe_agg',
    schema_version: 1,
    partial: {
      kind: 'grouped',
      groupKeys: ['region'],
      aggs: { x: { op: 'sum', col: 'amount' }, c: { op: 'count', col: 'amount' }, n: { op: 'count' } },
      rows: rows.map((r) => ({ k: [r.region], a: { x: String(r.x), c: r.c, n: r.n } })),
      rowCount: rows.length,
      more: false,
    },
  };
}

/** The FLAT §6.2 wire the seeded wh_query fn returns on the rpc plane
 *  (0015 shape: qc_class/kind/truncated/encoding at the top level; the wire
 *  carries NO shard — §6.2 declared deviation, the call site stamps it). */
function w6Wire(rows: E15Row[]): Record<string, unknown> {
  return {
    v: 1,
    table: 'wh_probe_agg',
    schema_version: 1,
    qc_class: 'QC6',
    kind: 'grouped',
    groupKeys: ['region'],
    aggs: { x: { op: 'sum', col: 'amount' }, c: { op: 'count', col: 'amount' }, n: { op: 'count' } },
    rows: rows.map((r) => ({ k: [r.region], a: { x: String(r.x), c: r.c, n: r.n } })),
    rowCount: rows.length,
    truncated: false,
    encoding: { x: 'text', c: 'number', n: 'number' },
    template_hash: W6H,
    template_timeout_ms: 8000,
    latencyMs: 5,
  };
}

/** Small hand-computed 2-region row sets (the 2-shard arms):
 *  merged = g00 {x:140, c:4, n:5}, g01 {x:110, c:5, n:5}. */
const SMALL_A: E15Row[] = [{ region: 'g00', x: 100, c: 3, n: 4 }, { region: 'g01', x: 50, c: 2, n: 2 }];
const SMALL_B: E15Row[] = [{ region: 'g00', x: 40, c: 1, n: 1 }, { region: 'g01', x: 60, c: 3, n: 3 }];
const smallEnvA = w6env('shard-a', SMALL_A);
const smallEnvB = w6env('shard-b', SMALL_B);

function bytes(partial: unknown): number {
  return new TextEncoder().encode(JSON.stringify(partial)).length;
}

interface SeenCall { shard: string; url: string; rpc?: WhRpcSpec }

/** Recording fetcher fake — pushes every call (shard + url + rpc?) and
 *  serves shard-keyed envelopes. The zero-socket law: this is the ONLY
 *  transport in the file, and it records. */
function recordingFetch(byShard: Record<string, WhPartialEnvelope | Record<string, unknown>>, seen: SeenCall[]): WhShardFetcher {
  return ((shard: string, url: string, rpc?: WhRpcSpec) => {
    seen.push({ shard, url, ...(rpc !== undefined ? { rpc } : {}) });
    const env = byShard[shard];
    if (env === undefined) return Promise.resolve({ ok: false as const, warning: { code: 'template_missing' }, estRows: 0 });
    return Promise.resolve({ ok: true as const, envelope: env as WhPartialEnvelope, estRows: 10 });
  }) as WhShardFetcher;
}

/** Full join ExecuteArgs over a colocated fleet. joinDimRows feeds the
 *  colocation gate ONLY (never directoryRows — the dispatch population stays
 *  the fact rows). */
function joinExecArgs(opts: {
  facts?: WhDirectoryRow[];
  dims?: WhDirectoryRow[];
  fetcher: WhShardFetcher;
  templateHashes?: readonly string[];
  rpcMode?: boolean;
  omitJoinDimRows?: boolean;
  reqOverride?: WhEngineRequest;
}): Parameters<typeof executeWhQuery>[0] {
  return {
    req: opts.reqOverride ?? parseWhEngineRequest(joinReqBody()),
    columnTypes: COLS,
    columnScales: SCALES,
    directoryRows: opts.facts ?? [factRow('shard-a'), factRow('shard-b')],
    ...(opts.omitJoinDimRows ? {} : { joinDimRows: opts.dims ?? [dimRow('shard-a', { isReference: true }), dimRow('shard-b', { isReference: true })] }),
    shardKeyColumn: '',
    shardKeyType: 'none',
    directoryVersion: 42,
    timers: neverTimeoutTimers(),
    fetcher: opts.fetcher,
    tableSchemaVersion: 1,
    templateHashes: opts.templateHashes ?? [W6H],
    ...(opts.rpcMode !== undefined ? { rpcMode: opts.rpcMode } : {}),
  };
}

const JOIN_PLAN: WhMergePlan = buildMergePlan(parseWhEngineRequest(joinReqBody()), { columnTypes: COLS, columnScales: SCALES });

// =============================================================================
// §6.1 — parse arms.
// =============================================================================
Deno.test('r129 join parse: absent byte-identical (re-pin), valid descriptor normalizes, null-lenient absent', () => {
  const plain = parseWhEngineRequest({ v: 1, qid: 'q', table: 'wh_probe_agg', query: { select: [{ op: 'count' }] } });
  // §6.1 arm 1 (the re-pin): a request WITHOUT join parses to EXACTLY the
  // pre-r129 shape — the normalized query carries no join key at all.
  eq('join absent: normalized query key set is EXACTLY the pre-r129 set (byte-identical re-pin)', Object.keys(plain.query).sort(), ['select']);
  const parsed = parseWhEngineRequest(joinReqBody());
  eq('valid join descriptor normalizes to the strict {table,type,on} shape', parsed.query.join, { table: 'wh_probe_dim', type: 'inner', on: { left: 'region', right: 'region' } });
  const nulled = parseWhEngineRequest({ ...joinReqBody(), query: { ...(joinReqBody().query as Record<string, unknown>), join: null } });
  eq('join:null => absent (the null-lenient precedent)', nulled.query.join, undefined);
  // r133 (design_r132_w7_family §2.1): variant normalizes additively —
  // absent stays absent (the W6 path byte-identical), present rides as the
  // literal "tier2".
  const tier2 = parseWhEngineRequest({ ...joinReqBody(), query: { ...(joinReqBody().query as Record<string, unknown>), join: { table: 'wh_probe_dim', type: 'inner', on: { left: 'region', right: 'region' }, variant: 'tier2' } } });
  eq('valid variant "tier2" normalizes into the descriptor (additive-optional)', tier2.query.join, { table: 'wh_probe_dim', type: 'inner', on: { left: 'region', right: 'region' }, variant: 'tier2' });
  eqTrue('variant-absent descriptor carries NO variant key (deepEq is exact-key-set — the W6 emit shape unchanged)', parsed.query.join !== undefined && !('variant' in parsed.query.join));
});

Deno.test('r129 join parse: every malformed variant is a fixed-string malformed (strict descriptor law)', () => {
  const j = (join: unknown) => ({ v: 1, qid: 'q-join-parse', table: 'wh_probe_agg', query: { select: [{ op: 'count' }], join } });
  throwsEngine('bad IDENT join.table (wh probe dim) => malformed', () => parseWhEngineRequest(j({ table: 'wh probe dim', type: 'inner', on: { left: 'region', right: 'region' } })), 'malformed', 'query.join.table must be a plain identifier');
  throwsEngine('self-join (join.table === query.table) => malformed', () => parseWhEngineRequest(j({ table: 'wh_probe_agg', type: 'inner', on: { left: 'region', right: 'region' } })), 'malformed', 'query.join.table must differ from the query table');
  throwsEngine('unknown type (left) => malformed — inner is the ONLY v1 type', () => parseWhEngineRequest(j({ table: 'wh_probe_dim', type: 'left', on: { left: 'region', right: 'region' } })), 'malformed', 'query.join.type must be the literal "inner"');
  throwsEngine('unknown key INSIDE join (using) => malformed (strict from day one)', () => parseWhEngineRequest(j({ table: 'wh_probe_dim', type: 'inner', on: { left: 'region', right: 'region' }, using: 'region' })), 'malformed', 'query.join carries unknown keys');
  throwsEngine('missing on.* (left absent) => malformed', () => parseWhEngineRequest(j({ table: 'wh_probe_dim', type: 'inner', on: { right: 'region' } })), 'malformed', 'query.join.on.left must be a plain identifier');
  // r132 rider (F-A4, r129 P3 ledger): the 4 malformed variants the battery
  // never pinned — the engine already rejects each (wh_engine_core.ts:547-583,
  // design_r128_joinplans §2.1); these arms pin those rejects fail-loud.
  throwsEngine('non-object join (a string) => malformed — typeof guard fires before any key lookups', () => parseWhEngineRequest(j('wh_probe_dim')), 'malformed', 'query.join must be an object');
  throwsEngine('non-object join.on (a string) => malformed — the on object guard fires before on.* lookups', () => parseWhEngineRequest(j({ table: 'wh_probe_dim', type: 'inner', on: 'region=region' })), 'malformed', 'query.join.on must be an object');
  throwsEngine('unknown key INSIDE join.on (bogus) => malformed — on is strictly validated like join itself', () => parseWhEngineRequest(j({ table: 'wh_probe_dim', type: 'inner', on: { left: 'region', right: 'region', bogus: 1 } })), 'malformed', 'query.join.on carries unknown keys');
  throwsEngine('bad IDENT on.right ("region; DROP") => malformed — on.right is IDENT_RE, no expression surface', () => parseWhEngineRequest(j({ table: 'wh_probe_dim', type: 'inner', on: { left: 'region', right: 'region; DROP' } })), 'malformed', 'query.join.on.right must be a plain identifier');
  // r133 (design_r132_w7_family §2.1): the additive OPTIONAL variant
  // discriminator — present must be the literal "tier2"; ANY other value is
  // a fixed-string malformed (r57 law: names the var, never echoes the
  // value); the check is LAST in the block so every existing reject above
  // fires UNCHANGED.
  throwsEngine('variant "tier3" (unknown enum value) => malformed', () => parseWhEngineRequest(j({ table: 'wh_probe_dim', type: 'inner', on: { left: 'region', right: 'region' }, variant: 'tier3' })), 'malformed', 'query.join.variant must be "tier2"');
  throwsEngine('variant non-string (7) => malformed — the enum is the literal "tier2" ONLY', () => parseWhEngineRequest(j({ table: 'wh_probe_dim', type: 'inner', on: { left: 'region', right: 'region' }, variant: 7 })), 'malformed', 'query.join.variant must be "tier2"');
  throwsEngine('variant null => malformed (inner keys are strict — the on:null precedent)', () => parseWhEngineRequest(j({ table: 'wh_probe_dim', type: 'inner', on: { left: 'region', right: 'region' }, variant: null })), 'malformed', 'query.join.variant must be "tier2"');
  // validation-order law: a request that fails an EARLIER reject still
  // reports THAT reject when variant is also bad (variant is checked LAST).
  throwsEngine('validation order: unknown key + bad variant => the UNKNOWN-KEY reject fires first', () => parseWhEngineRequest(j({ table: 'wh_probe_dim', type: 'inner', on: { left: 'region', right: 'region' }, using: 'region', variant: 'tier3' })), 'malformed', 'query.join carries unknown keys');
  throwsEngine('validation order: bad type + bad variant => the TYPE reject fires first', () => parseWhEngineRequest(j({ table: 'wh_probe_dim', type: 'left', on: { left: 'region', right: 'region' }, variant: 'tier3' })), 'malformed', 'query.join.type must be the literal "inner"');
  // r57 no-echo law: the reject names the var/position only — never the value.
  let msg = '';
  try {
    parseWhEngineRequest(j({ table: 'wh_probe_agg', type: 'inner', on: { left: 'region', right: 'region' } }));
  } catch (err) {
    msg = (err as Error).message;
  }
  eqTrue('no-echo: the self-join reject never repeats the offending input value', !msg.includes('wh_probe_agg'));
});

// =============================================================================
// §6.2 — plan gate arms (colocation/template-class/key binding).
// =============================================================================

// The E15 3-shard colocated arm IS the "W6 selected" proof: the whole oracle
// rides the REAL engine pipeline (gate → fanout → merge → finalize) with the
// fact rows dispatched and the broadcast dim vouching every selected ref.
// Shard slices: A = g00..g15 (16 rows), B = g16..g32 (17), C = g33..g49 (16).
const E15_A = E15.rows.slice(0, 16);
const E15_B = E15.rows.slice(16, 33);
const E15_C = E15.rows.slice(33, 49);
const envA = w6env('shard-a', E15_A);
const envB = w6env('shard-b', E15_B);
const envC = w6env('shard-c', E15_C);

Deno.test('r129 join gate: colocated broadcast dim ⇒ W6 fanout — the FULL E15 oracle through the real pipeline (49 rows EXACT), perShard ok-arm pins', async () => {
  const seen: SeenCall[] = [];
  const res = await executeWhQuery(joinExecArgs({
    facts: [factRow('shard-a'), factRow('shard-b'), factRow('shard-c')],
    dims: [dimRow('shard-a', { isReference: true }), dimRow('shard-b', { isReference: true }), dimRow('shard-c', { isReference: true })],
    fetcher: recordingFetch({ 'shard-a': envA, 'shard-b': envB, 'shard-c': envC }, seen),
  }));
  eq('colocated join: coverage 3/3, partial false, warnings []', [res.coverage, res.partial, res.warnings], ['3/3', false, []]);
  eq('finalize output === E15 output_rows EXACT (49 rows, x as the scale-0 bigint carrier, canonical order)', res.rows, E15.rows.map((r) => ({ k: [r.region], aggs: { x: BigInt(r.x), c: r.c, n: r.n } })));
  // §6.7 R-3 exact-shape law on the NEW surface: the ok-arm key set is EXACTLY
  // {shard, ok, latencyMs, error, partial_rows, partial_bytes} (deepEq is an
  // exact-key-set pin by construction — never weakened to a projection).
  eq('perShard exact shape (R-3 law: exact ok-arm key set, partial_rows = consumed rowCount)', res.perShard, [
    { shard: 'shard-a', ok: true, latencyMs: res.perShard[0]?.latencyMs, error: null, partial_rows: 16, partial_bytes: bytes(envA.partial) },
    { shard: 'shard-b', ok: true, latencyMs: res.perShard[1]?.latencyMs, error: null, partial_rows: 17, partial_bytes: bytes(envB.partial) },
    { shard: 'shard-c', ok: true, latencyMs: res.perShard[2]?.latencyMs, error: null, partial_rows: 16, partial_bytes: bytes(envC.partial) },
  ]);
  eqTrue(
    'partial_rows === the CONSUMED partial\'s rowCount on every ok entry (49-class law: live waves pin 49/shard, r130 AB-P3)',
    res.perShard.length === 3 && res.perShard.every((p, i) => p.partial_rows === [16, 17, 16][i]),
  );
  eqTrue(
    'partial_bytes byte-convention: UTF-8 byte length of JSON.stringify(the consumed partial), > 0',
    res.perShard[0]?.partial_bytes === bytes(envA.partial) && (res.perShard[0]?.partial_bytes ?? 0) > 0 &&
      res.perShard[1]?.partial_bytes === bytes(envB.partial) && res.perShard[2]?.partial_bytes === bytes(envC.partial),
  );
});

Deno.test('r129 join gate: every colocation violation fails CLOSED with join_not_colocated BEFORE any network (recording fake proves zero POSTs)', async () => {
  // A single recording fetcher shared by every rejecting arm below — the
  // gate throws before target construction, so it must record NOTHING.
  const seen: SeenCall[] = [];
  const fetcher = recordingFetch({ 'shard-a': envA, 'shard-b': envB }, seen);
  const noDims = (dims: WhDirectoryRow[] | undefined) =>
    joinExecArgs({ dims, fetcher, ...(dims === undefined ? { omitJoinDimRows: true } : {}) });

  await rejectsEngine('is_reference=false on one dim placement => join_not_colocated (undefined ≠ true)', () =>
    executeWhQuery(noDims([dimRow('shard-a', { isReference: true }), dimRow('shard-b', { isReference: false })])), 'join_not_colocated', 'not a reference placement');
  await rejectsEngine('is_reference ABSENT on one dim placement => join_not_colocated (fail-closed — the gate can never vouch what it cannot see)', () =>
    executeWhQuery(noDims([dimRow('shard-a', { isReference: true }), dimRow('shard-b')])), 'join_not_colocated', 'not a reference placement');
  await rejectsEngine('dim placement MISSING on one selected ref => join_not_colocated naming the ref (arity 0)', () =>
    executeWhQuery(noDims([dimRow('shard-a', { isReference: true })])), 'join_not_colocated', "selected ref 'shard-b'");
  await rejectsEngine('DUPLICATE dim placement on a ref (found 2) => join_not_colocated (the exactly-one arity invariant, r113 law)', () =>
    executeWhQuery(noDims([dimRow('shard-a', { isReference: true }), dimRow('shard-b', { isReference: true }), dimRow('shard-b', { isReference: true })])), 'join_not_colocated', '(found 2)');
  await rejectsEngine('joinDimRows undefined (join request with no dim population) => join_not_colocated fail-closed', () =>
    executeWhQuery(noDims(undefined)), 'join_not_colocated', 'dim placement population unavailable');
  eq('every gate reject fired BEFORE any network: the recording fetcher saw ZERO calls', seen, []);

  // The serving-or-draining law: a draining dim placement still vouches its
  // ref (rolling-deploy law) — the gate passes and the wave completes.
  const drainSeen: SeenCall[] = [];
  const res = await executeWhQuery(joinExecArgs({
    dims: [dimRow('shard-a', { isReference: true }), dimRow('shard-b', { isReference: true, state: 'draining' })],
    fetcher: recordingFetch({ 'shard-a': envA, 'shard-b': envB }, drainSeen),
  }));
  eq('draining counts as serving-or-draining: the gate passes and the wave completes 2/2', [res.coverage, res.partial], ['2/2', false]);
});

Deno.test('r129 join gate: template-class + key binding (M-J3) — a join request must select the join-class template with the EXACT manifest binding', async () => {
  const seen: SeenCall[] = [];
  const fetcher = recordingFetch({ 'shard-a': envA, 'shard-b': envB }, seen);
  // derived[0] = W1 (no join binding) => join_template_required.
  await rejectsEngine('join request vs non-join-class template (W1 derived) => join_template_required', () =>
    executeWhQuery(joinExecArgs({ fetcher, templateHashes: [W1H] })), 'join_template_required', 'the derived template is not join-class');
  // Key binding (§2.1 audit-B AB-P2 / M-J3 re-spec): on {left:region,
  // right:tier} ≠ the manifest binding {region,region} => join_key_mismatch.
  // Pre-fix RED = the mismatched request passes and returns the EXACT oracle
  // (silent wrong-bind); post-fix = 400.
  const mismatchArgs = joinExecArgs({ fetcher });
  mismatchArgs.req = parseWhEngineRequest({ ...joinReqBody(), query: { ...(joinReqBody().query as Record<string, unknown>), join: { table: 'wh_probe_dim', type: 'inner', on: { left: 'region', right: 'tier' } } } });
  await rejectsEngine('on right:"tier" vs the manifest join binding (region/region) => join_key_mismatch (M-J3 killer)', () =>
    executeWhQuery(mismatchArgs), 'join_key_mismatch', 'request join.on keys do not equal');
  eq('both rejects fired BEFORE any network: zero shard POSTs', seen, []);
});

Deno.test('r129 join gate: a ZERO-derivation join plan belongs to D2 (plan_untemplated), never to the join gates', async () => {
  // Join plans derive ONLY join-class rows — a scalar-kind join plan derives
  // NOTHING (W6 is rows-kind). The template-class gate deliberately skips
  // zero derivation (W1 can never mask it) and the rpcMode D2 gate owns the
  // outcome — the census law "ZERO hashes → plan_untemplated (not W1)".
  const scalarJoinReq = parseWhEngineRequest({
    v: 1,
    qid: '01J9Q1ZZZZZZZZZZZZZZZZZZZZ',
    table: 'wh_probe_agg',
    query: {
      select: [{ op: 'sum', col: 'amount', alias: 'x' }],
      join: { table: 'wh_probe_dim', type: 'inner', on: { left: 'region', right: 'region' } },
    },
  });
  const seen: SeenCall[] = [];
  await rejectsEngine('zero-derived (scalar-kind) join under rpcMode => plan_untemplated (D2 — NOT join_template_required)', () =>
    executeWhQuery(joinExecArgs({
      fetcher: recordingFetch({}, seen),
      templateHashes: [],
      rpcMode: true,
      reqOverride: scalarJoinReq,
    })), 'plan_untemplated', 'derives 0 template hash');
  eq('D2 fired before any network: zero POSTs', seen, []);
});

// r134 (review-b B-F2, design_r132_w7_family §6.2): the LITERAL tier2-flavored
// zero-derivation arm — the r129 D2 precedent above with the variant:'tier2'
// discriminator carried. Derivation is DIM-BLIND (join.table is NOT a
// derivation input — the (a2) gate solely owns it), so a zero-match tier2
// plan is built the precedent's way: a scalar-kind join (no groupBy — W7 is
// rows-kind) whose REAL manifest derivation is EMPTY. The join gates
// deliberately skip zero derivation (matchedJoinRow === null leaves
// (a)/(a2)/(b) inert) and the rpcMode D2 gate owns the outcome:
// plan_untemplated, NEVER join_template_required.
Deno.test('r134 B-F2 (design_r132_w7_family §6.2): the tier2-flavored ZERO-derivation join belongs to D2 (plan_untemplated), never to the join gates', async () => {
  const scalarTier2JoinReq = parseWhEngineRequest({
    v: 1,
    qid: '01J9Q1ZZZZZZZZZZZZZZZZZZZZ',
    table: 'wh_probe_agg',
    query: {
      select: [{ op: 'sum', col: 'amount', alias: 'x' }],
      join: { table: 'wh_probe_dim', type: 'inner', on: { left: 'region', right: 'region' }, variant: 'tier2' },
    },
  });
  // REAL derivation path (the wh_entrypoint.ts:486 plan-view shape — table +
  // groupKeys? + join? + aggs — against the REAL ENGINE_TEMPLATE_MANIFEST):
  // the tier2 scalar-kind join derives ZERO rows (kind mismatch — W7 is
  // rows-kind; no unit-fake, the [] IS the real derivation output, and it is
  // the exact set the engine consumes below).
  const derivedReal = deriveTemplateHashes(
    { table: scalarTier2JoinReq.table, join: scalarTier2JoinReq.query.join, aggs: { x: { op: 'sum', col: 'amount' } } },
    1,
  );
  eq('REAL manifest derivation of the tier2 scalar-kind join => [] (zero-match tier2 — the variant partition never matches a scalar plan)', derivedReal, []);
  const seen: SeenCall[] = [];
  await rejectsEngine('zero-derived tier2 join under rpcMode => plan_untemplated (D2 — NOT join_template_required)', () =>
    executeWhQuery(joinExecArgs({
      fetcher: recordingFetch({}, seen),
      templateHashes: derivedReal,
      rpcMode: true,
      reqOverride: scalarTier2JoinReq,
    })), 'plan_untemplated', 'derives 0 template hash');
  eq('D2 fired before any network: zero POSTs', seen, []);
});

Deno.test('r129 join-aware derivation partition: join plans derive ONLY the join class, non-join plans EXCLUDE it (W6 never co-derives with W1)', () => {
  const groupedView = {
    table: 'wh_probe_agg',
    groupKeys: [{ col: 'region', type: 'text' }],
    aggs: { x: { op: 'sum', col: 'amount' }, c: { op: 'count', col: 'amount' }, n: { op: 'count' } },
  };
  eq('join request (full {x,c,n} set) => [W6H] ONLY (a non-join plan over the SAME op-set derives [] — count_col+groupby is W6-class only)', deriveTemplateHashes({ ...groupedView, join: { table: 'wh_probe_dim', type: 'inner', on: { left: 'region', right: 'region' } } }, 1), [W6H]);
  // The sharpest partition discriminator: ONE op-set both classes could
  // serve ({groupby,sum,count}) — the join marker flips W1H ⇄ W6H, proving
  // the partition is the join marker (presence-only), not the subset rule.
  const w1Shaped = { table: 'wh_probe_agg', groupKeys: [{ col: 'region', type: 'text' }], aggs: { x: { op: 'sum', col: 'amount' }, n: { op: 'count' } } };
  eq('the {sum,count} op-set WITHOUT join => [W1H]; the SAME op-set WITH join => [W6H] (presence-only partition, never co-derivation)', [
    deriveTemplateHashes(w1Shaped, 1),
    deriveTemplateHashes({ ...w1Shaped, join: { table: 'wh_probe_dim', type: 'inner', on: { left: 'region', right: 'region' } } }, 1),
  ], [[W1H], [W6H]]);
  eqTrue('a join request lacking the count-col is still LEGAL (subset semantics): {x} => [W6H]', deepEq(deriveTemplateHashes({ table: 'wh_probe_agg', groupKeys: [{ col: 'region', type: 'text' }], aggs: { x: { op: 'sum', col: 'amount' } }, join: {} }, 1), [W6H]));
  // r133 W7 LEGS (design_r132_w7_family §6.2): the VARIANT discriminator —
  // W6/W7 merge_ops are IDENTICAL, so the variant (not the op-set) picks
  // the class member; W6 and W7 can NEVER co-derive.
  const tier2Join = { table: 'wh_probe_dim', type: 'inner', on: { left: 'region', right: 'region' }, variant: 'tier2' as const };
  eq('tier2-join (variant present) over the FULL {x,c,n} op-set => [W7H] ONLY (the variant partition — W6 is variant-absent)', deriveTemplateHashes({ ...groupedView, join: tier2Join }, 1), [W7H]);
  eq('base-join (variant absent) over the SAME op-set => [W6H] ONLY (absent ↔ absent — W7 is variant-tier2)', deriveTemplateHashes({ ...groupedView, join: { table: 'wh_probe_dim', type: 'inner', on: { left: 'region', right: 'region' } } }, 1), [W6H]);
  eq('tier2-join over the {sum,count} op-set => [W7H] (subset semantics ride the variant class too)', deriveTemplateHashes({ ...w1Shaped, join: tier2Join }, 1), [W7H]);
  eqTrue('non-join plans NEVER derive W7 (the join-class partition is unchanged)', deepEq(deriveTemplateHashes(groupedView, 1), []));
  // r144 W8 LEGS (agent-ctx/r140-w7-census.md §6): the tier2 variant now
  // ALSO op-set-partitions — a tier2 AVG plan derives ONLY W8 (W7 lacks
  // avg_pair), a tier2 SUM plan derives ONLY W7 (W8 lacks sum; pinned
  // above), and the mixed {avg_pair,sum} set derives ZERO (D2 territory).
  eq('tier2-join AVG (avg_pair) => [W8H] ONLY (r144 — the FIRST grouped-avg derivation; W7 lacks avg_pair)', deriveTemplateHashes({ table: 'wh_probe_agg', groupKeys: [{ col: 'region', type: 'text' }], aggs: { a: { op: 'avg', col: 'amount' } }, join: tier2Join }, 1), [W8H]);
  eq('tier2-join {avg_pair,sum} => ZERO derivation (no member serves the mixed set — plan_untemplated, the D2 datum)', deriveTemplateHashes({ table: 'wh_probe_agg', groupKeys: [{ col: 'region', type: 'text' }], aggs: { a: { op: 'avg', col: 'amount' }, x: { op: 'sum', col: 'amount' } }, join: tier2Join }, 1), []);
});

// =============================================================================
// §6.3 (r133) — the join.dim BINDING GATE (design_r132_w7_family §2.4, census
// P1 — K-W7c). The gate is the SOLE dim-binding site: derivation does the
// VARIANT (wh_handshake.ts), THIS gate does the dim. A join request whose
// join.table does not equal the matched (variant-partitioned) template's
// manifest join.dim is rejected join_template_required — a template WAS
// selected, it just serves a different dim (semantically distinct from D2's
// empty-derivation plan_untemplated). With ONE join class the mismatch was
// unreachable-in-practice; with TWO (W6 base / W7 tier2) it is the wrong-dim
// silent-execution hazard: the body executes its HARDCODED dim and a
// mismatched request would silently bind nothing and pass a WRONG oracle.
// K-W7c: the mutant removes exactly this comparison — the arms below are the
// pre-fix RED evidence (written BEFORE the gate; run RED; then the gate; run
// GREEN — commit-before-mutant law).
// =============================================================================
Deno.test('r133 join.dim binding gate (K-W7c, design_r132_w7_family §2.4): a wrong-dim join request is rejected join_template_required BEFORE any network', async () => {
  const seen: SeenCall[] = [];
  const fetcher = recordingFetch({ 'shard-a': envA, 'shard-b': envB }, seen);
  // Wrong-dim request: join.table 'wh_probe_dim2' is a plain identifier that
  // differs from the query table (parse-legal) but does NOT equal the
  // manifest join binding of the injected derived row (templateHashes
  // injection, wh_join_test.ts:442 precedent — :442/:448 era). The keys
  // still match (region/region) and the colocation population is provided —
  // WITHOUT the dim gate the request would silently execute W6's hardcoded
  // dim and fan out (the pre-fix RED: no throw, coverage 2/2).
  const wrongDimBase = parseWhEngineRequest({ ...joinReqBody(), query: { ...(joinReqBody().query as Record<string, unknown>), join: { table: 'wh_probe_dim2', type: 'inner', on: { left: 'region', right: 'region' } } } });
  await rejectsEngine('join.table wh_probe_dim2 vs the W6 manifest join.dim wh_probe_dim (injected [W6H]) => join_template_required (K-W7c)', () =>
    executeWhQuery(joinExecArgs({ fetcher, templateHashes: [W6H], reqOverride: wrongDimBase })), 'join_template_required', 'join.dim binding');
  // The tier2 twin: the VARIANT matches (the request IS W7-class) but the
  // dim binding still fails — variant-only derivation is deliberately NOT a
  // dim check (zero-derivation stays D2-owned); the gate solely owns the dim.
  const wrongDimTier2 = parseWhEngineRequest({ ...joinReqBody(), query: { ...(joinReqBody().query as Record<string, unknown>), join: { table: 'wh_probe_dim2', type: 'inner', on: { left: 'region', right: 'region' }, variant: 'tier2' } } });
  await rejectsEngine('tier2-variant join.table wh_probe_dim2 vs the W7 manifest join.dim wh_probe_dim (injected [W7H]) => join_template_required (the variant does NOT vouch the dim)', () =>
    executeWhQuery(joinExecArgs({ fetcher, templateHashes: [W7H], reqOverride: wrongDimTier2 })), 'join_template_required', 'join.dim binding');
  eq('both dim-gate rejects fired BEFORE any network: zero shard POSTs', seen, []);
  // Positive control: the RIGHT dim passes the gate and the wave completes —
  // the gate is not over-firing on the legal tier2 path.
  const rightDimTier2 = parseWhEngineRequest({ ...joinReqBody(), query: { ...(joinReqBody().query as Record<string, unknown>), join: { table: 'wh_probe_dim', type: 'inner', on: { left: 'region', right: 'region' }, variant: 'tier2' } } });
  const resOk = await executeWhQuery(joinExecArgs({ fetcher: recordingFetch({ 'shard-a': envA, 'shard-b': envB }, []), templateHashes: [W7H], reqOverride: rightDimTier2 }));
  eq('right-dim tier2 request passes the gate: coverage 2/2 (the tier2 path is UNCHANGED by the dim gate)', [resOk.coverage, resOk.partial], ['2/2', false]);
});

// =============================================================================
// §6.3 + §6.5 — envelope/merge arms over E15 (the REAL merge path).
// =============================================================================
Deno.test('r129 E15 monoid closure: 49 W6-shaped one-region partials merge + finalize through the REAL merge path to EXACTLY the oracle', () => {
  const envs = E15.rows.map((r) => w6env(`e15-${r.region}`, [r]));
  const merged = mergeGroupedPartials(JOIN_PLAN, envs);
  eq('merged partial keeps the rowCount invariant (49 groups from 49 one-row shard partials)', merged.partial.rowCount, 49);
  const fin = finalizeGroups(merged, JOIN_PLAN);
  eq('finalize(merged) === E15 output_rows EXACT (49 rows — g47 ABSENT by the inner-join law)', fin, E15.rows.map((r) => ({ k: [r.region], aggs: { x: BigInt(r.x), c: r.c, n: r.n } })));
  const finRev = finalizeGroups(mergeGroupedPartials(JOIN_PLAN, [...envs].reverse()), JOIN_PLAN);
  eq('monoid closure: reversed-arrival merge finalizes to the SAME 49 rows (assoc + comm spot pin)', finRev, fin);
  const sumX = fin.reduce((a, r) => a + (r.aggs.x as bigint), 0n);
  const sumC = fin.reduce((a, r) => a + (r.aggs.c as number), 0);
  const sumN = fin.reduce((a, r) => a + (r.aggs.n as number), 0);
  eq('hand-computed oracle globals: Σx 64,521,483 / Σc 13,578 / Σn 13,720 (audit-B confirmed)', [sumX.toString(), sumC, sumN], ['64521483', 13578, 13720]);
  eq('M-J1 killer datum: g47 ABSENT from the merged output AND the excluded mutant-catcher row is pinned', [fin.some((r) => r.k[0] === 'g47'), E15.excluded.g47], [false, { x: 1374335, c: 277, n: 280 }]);
});

Deno.test('r129 §2.4 contract pin: a W6-shaped partial labeled with the DIM table is envelope_invalid (the envelope rides the FACT table only)', () => {
  const dimLabeled = { ...w6env('s-dim', [E15.rows[0]]), table: 'wh_probe_dim' };
  throwsMerge('envelope.table wh_probe_dim !== plan.table wh_probe_agg => envelope_invalid', () => mergeGroupedPartials(JOIN_PLAN, [dimLabeled]), 'envelope_invalid');
});

Deno.test('r129 engine consumption: the dim-labeled partial is excluded LOUD and the merge-exclusion spread-rewrite PRESERVES the ok-arm measurement keys', async () => {
  // The ok-arm push happens BEFORE the merge loop; the :2014-style exclusion
  // rewrite { ...entry, ok: false, error: 'excluded' } spreads — so the
  // newly-failed entry KEEPS partial_rows/partial_bytes (census §2.0 note).
  const res = await executeWhQuery(joinExecArgs({
    fetcher: recordingFetch({ 'shard-a': { ...envA, table: 'wh_probe_dim' }, 'shard-b': envB }, []),
  }));
  eq('dim-labeled shard excluded, honest shard merged: coverage 1/2, partial', [res.coverage, res.partial], ['1/2', true]);
  eqTrue('the exclusion warning names the envelope.table law (WhMergeError detail rides)', (res.warnings[0]?.code === 'excluded') && (res.warnings[0]?.detail ?? '').includes('envelope.table'));
  eq('perShard: the rewritten-failed entry KEEPS partial_rows/partial_bytes (spread preserves); the honest entry stays ok', res.perShard, [
    { shard: 'shard-a', ok: false, latencyMs: res.perShard[0]?.latencyMs, error: 'excluded', partial_rows: 16, partial_bytes: bytes(envA.partial) },
    { shard: 'shard-b', ok: true, latencyMs: res.perShard[1]?.latencyMs, error: null, partial_rows: 17, partial_bytes: bytes(envB.partial) },
  ]);
});

// =============================================================================
// §6.4 — manifest statics: W6 row deep-equality + rows-kind body TEXT pins.
// =============================================================================
Deno.test('r129 manifest statics: the W6 row deep-equals the 16-field transcription (incl. the join binding) and is APPENDED at index 5', () => {
  const w6 = manifestRowByHash(W6H);
  eq('manifestRowByHash(W6H) deep-equals the hand-transcribed W6 row (16 fields incl. join {dim,left,right})', w6, {
    slug: 'W6_colocated_join_agg',
    file: 'W6_colocated_join_agg.sql',
    template_hash: W6H,
    logical_table: 'wh_probe_agg',
    qc_class: 'QC6',
    kind: 'rows',
    merge_ops: ['groupby', 'sum', 'count', 'count_col'],
    group_keys: ['region'],
    params_schema: {},
    timeout_ms: 8000,
    max_rows: 1000,
    schema_version: 1,
    state: 'active',
    aggs: { x: { op: 'sum', col: 'amount' }, c: { op: 'count', col: 'amount' }, n: { op: 'count' } },
    encoding: { x: 'text', c: 'number', n: 'number' },
    join: { dim: 'wh_probe_dim', left: 'region', right: 'region' },
  });
  // r133 re-pin (design_r132_w7_family §2.2): the join-row count 1 → 2 (W7
  // APPENDED at index 6, manifest length 6 → 7 — the append-only + partition
  // laws EXTENDED, never weakened).
  const w7 = manifestRowByHash(W7H);
  eq('manifestRowByHash(W7H) deep-equals the hand-transcribed W7 row (16 fields incl. join {dim,left,right,variant:"tier2"})', w7, {
    slug: 'W7_dim_tier_join_agg',
    file: 'W7_dim_tier_join_agg.sql',
    template_hash: W7H,
    logical_table: 'wh_probe_agg',
    qc_class: 'QC6',
    kind: 'rows',
    merge_ops: ['groupby', 'sum', 'count', 'count_col'],
    group_keys: ['region'],
    params_schema: {},
    timeout_ms: 8000,
    max_rows: 1000,
    schema_version: 1,
    state: 'active',
    aggs: { x: { op: 'sum', col: 'amount' }, c: { op: 'count', col: 'amount' }, n: { op: 'count' } },
    encoding: { x: 'text', c: 'number', n: 'number' },
    join: { dim: 'wh_probe_dim', left: 'region', right: 'region', variant: 'tier2' },
  });
  // r144 re-pin (agent-ctx/r140-w7-census.md §6): the join-row count 2 → 3
  // (W8 APPENDED at index 7, manifest length 7 → 8 — the append-only +
  // partition laws EXTENDED, never weakened).
  const w8 = manifestRowByHash(W8H);
  eq('manifestRowByHash(W8H) deep-equals the hand-transcribed W8 row (16 fields incl. join {dim,left,right,variant:"tier2"}; the FIRST grouped-avg row)', w8, {
    slug: 'W8_dim_tier_join_avg',
    file: 'W8_dim_tier_join_avg.sql',
    template_hash: W8H,
    logical_table: 'wh_probe_agg',
    qc_class: 'QC6',
    kind: 'rows',
    merge_ops: ['groupby', 'avg_pair', 'count', 'count_col'],
    group_keys: ['region'],
    params_schema: {},
    timeout_ms: 8000,
    max_rows: 1000,
    schema_version: 1,
    state: 'active',
    aggs: { s: { op: 'sum', col: 'amount' }, c: { op: 'count_col', col: 'amount' }, n: { op: 'count' } },
    encoding: { s: 'text', c: 'number', n: 'number' },
    join: { dim: 'wh_probe_dim', left: 'region', right: 'region', variant: 'tier2' },
  });
  eq('append-only + partition laws (r144: W6 index 5 unchanged, W7 index 6, W8 index 7, THREE join-carrying rows, EIGHT rows)', [ENGINE_TEMPLATE_MANIFEST[5] === w6, ENGINE_TEMPLATE_MANIFEST[6] === w7, ENGINE_TEMPLATE_MANIFEST[7] === w8, ENGINE_TEMPLATE_MANIFEST.filter((r) => r.join !== undefined).length, ENGINE_TEMPLATE_MANIFEST.length], [true, true, true, 3, 8]);
});

Deno.test('r129 W6 body compliance TEXT pins (design §2.3 rows-kind law, lint-templates parity) over db/shard-templates/W6_colocated_join_agg.sql', async () => {
  const bodyBytes = await Deno.readFile(new URL('../../../db/shard-templates/W6_colocated_join_agg.sql', import.meta.url));
  const body = new TextDecoder().decode(bodyBytes);
  eqTrue('effective text ends `limit $2` (final line, no trailing semicolon — the F2 sentinel interplay)', body.trimEnd().endsWith('limit $2'));
  eqTrue('_pre_trim window present (same shape as W1 — the sentinel re-cap interplay unchanged)', body.includes('count(*) over () as _pre_trim'));
  eqTrue('zero double-quote characters anywhere in the body (single quotes only — mirrors W1)', !body.includes('"'));
  eqTrue('no row-aggregate wrapper (jsonb_agg|array_agg|string_agg banned in rows-kind bodies — the lint ROWS_AGG_BAN parity)', !/\b(jsonb_agg|array_agg|string_agg)\s*\(/i.test(body));
  eqTrue('the hardcoded SQL join keys match the manifest join binding (t.region = d.region — what key binding protects)', body.includes('join public.wh_probe_dim d on t.region = d.region'));
  const digest = await crypto.subtle.digest('SHA-256', bodyBytes);
  const hex = [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, '0')).join('');
  eq('sha256(body bytes) === the manifest template_hash (the body IS the contract)', hex, W6H);
});

Deno.test('r133 W7 body compliance TEXT pins (design_r132_w7_family §2.3 rows-kind law + the K-W7b tier-predicate TEXT pin) over db/shard-templates/W7_dim_tier_join_agg.sql', async () => {
  const bodyBytes = await Deno.readFile(new URL('../../../db/shard-templates/W7_dim_tier_join_agg.sql', import.meta.url));
  const body = new TextDecoder().decode(bodyBytes);
  eqTrue('effective text ends `limit $2` (final line, no trailing semicolon — the F2 sentinel interplay)', body.trimEnd().endsWith('limit $2'));
  eqTrue('_pre_trim window present (same shape as W1/W6 — the sentinel re-cap interplay unchanged)', body.includes('count(*) over () as _pre_trim'));
  eqTrue('zero double-quote characters anywhere in the body (single quotes only — mirrors W6)', !body.includes('"'));
  eqTrue('no row-aggregate wrapper (jsonb_agg|array_agg|string_agg banned in rows-kind bodies — the lint ROWS_AGG_BAN parity)', !/\b(jsonb_agg|array_agg|string_agg)\s*\(/i.test(body));
  eqTrue('the hardcoded SQL join keys match the manifest join binding (t.region = d.region — what key binding protects)', body.includes('join public.wh_probe_dim d on t.region = d.region'));
  // K-W7b TEXT arm (design §6 K-W7b): the tier predicate is the body's ONE
  // delta line vs W6 — a mutant that drops `and d.tier = 2` emits the 49-row
  // W6 oracle against the 17-row tier2 expectation → this pin + the live
  // ladder RED. Byte law: W6 + 15 bytes = 482, LF-only, no trailing NL.
  eqTrue('the tier predicate `and d.tier = 2` is present on the ON clause (the K-W7b discriminator — tier filter is a template CONSTANT)', body.includes('on t.region = d.region and d.tier = 2'));
  eq('W7 byte law: EXACTLY 482 bytes = W6 (467) + the 15-byte ` and d.tier = 2` delta, single LF-only tail, no trailing newline', [bodyBytes.length, body.endsWith('limit $2'), body.includes('\r')], [482, true, false]);
  const digest = await crypto.subtle.digest('SHA-256', bodyBytes);
  const hex = [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, '0')).join('');
  eq('sha256(body bytes) === the manifest template_hash (the body IS the contract)', hex, W7H);
});

Deno.test('r144 W8 body compliance TEXT pins (agent-ctx/r140-w7-census.md §6 rows-kind law + the ONE-BYTE delta law) over db/shard-templates/W8_dim_tier_join_avg.sql', async () => {
  const bodyBytes = await Deno.readFile(new URL('../../../db/shard-templates/W8_dim_tier_join_avg.sql', import.meta.url));
  const body = new TextDecoder().decode(bodyBytes);
  eqTrue('effective text ends `limit $2` (final line, no trailing semicolon — the F2 sentinel interplay)', body.trimEnd().endsWith('limit $2'));
  eqTrue('_pre_trim window present (same shape as W1/W6/W7 — the sentinel re-cap interplay unchanged)', body.includes('count(*) over () as _pre_trim'));
  eqTrue('zero double-quote characters anywhere in the body (single quotes only — mirrors W7)', !body.includes('"'));
  eqTrue('no row-aggregate wrapper (jsonb_agg|array_agg|string_agg banned in rows-kind bodies — the lint ROWS_AGG_BAN parity)', !/\b(jsonb_agg|array_agg|string_agg)\s*\(/i.test(body));
  eqTrue('the hardcoded SQL join keys match the manifest join binding (t.region = d.region — what key binding protects)', body.includes('join public.wh_probe_dim d on t.region = d.region'));
  // The tier predicate rides UNCHANGED (W8 inherits W7's `and d.tier = 2` —
  // the variant is still tier2) and the wire key is the ONE delta: the s/c
  // avg-pair convention (W3 precedent) replaces W7's x/c keys.
  eqTrue('the tier predicate `and d.tier = 2` is present on the ON clause (inherited from W7 — the variant stays tier2)', body.includes('on t.region = d.region and d.tier = 2'));
  eqTrue("the row_json wire key is 's' (the W3 avg-pair convention — the manifest aggs/encoding keys match)", body.includes("jsonb_build_object('s', g.s::text, 'c', g.c, 'n', g.n)"));
  eqTrue("W7's 'x' wire key is GONE (the rename is the whole delta — no x-key residue)", !body.includes("'x'"));
  eq('W8 byte law: EXACTLY 482 bytes (identical to W7), single LF-only tail, no trailing newline, zero CR', [bodyBytes.length, body.endsWith('limit $2'), body.includes('\r')], [482, true, false]);
  // THE ONE-BYTE DELTA LAW (the census §6 template-only growth proof): W8's
  // bytes differ from W7's in EXACTLY ONE position, and that byte is
  // 'x' (0x78) → 's' (0x73). Any other drift (a reformatted body, a changed
  // predicate, a second wire-key edit) goes RED here.
  const w7Bytes = await Deno.readFile(new URL('../../../db/shard-templates/W7_dim_tier_join_agg.sql', import.meta.url));
  const diffPositions: { pos: number; w7: number; w8: number }[] = [];
  for (let i = 0; i < Math.max(w7Bytes.length, bodyBytes.length); i++) {
    if (w7Bytes[i] !== bodyBytes[i]) diffPositions.push({ pos: i, w7: w7Bytes[i], w8: bodyBytes[i] });
  }
  eq('the W8-vs-W7 byte diff is EXACTLY ONE position, x(0x78)→s(0x73)', diffPositions, [{ pos: 104, w7: 0x78, w8: 0x73 }]);
  const digest = await crypto.subtle.digest('SHA-256', bodyBytes);
  const hex = [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, '0')).join('');
  eq('sha256(body bytes) === the manifest template_hash (the body IS the contract)', hex, W8H);
});

Deno.test('r129 E15 fixture provenance: canonical oracle port (path + sha256 self-pinned), the hand constants ride the fixture', () => {
  eqTrue('meta.source names the canonical oracle file AND its sha256 (never a silent re-derivation)', E15.meta.source.includes('audit/r128_join_oracle/join_oracle.json') && E15.meta.source.includes('e5959512546b9d00591af265c944911101314c5ce8cdf9470618d09b7f298172'));
  eq('49 rows + globals + excluded are the audit-B-confirmed hand constants', [E15.rows.length, E15.globals, E15.excluded.g47, E15.meta.fixture], [49, { rows: 49, sum_c: 13578, sum_n: 13720, sum_x: 64521483 }, { x: 1374335, c: 277, n: 280 }, 'E15']);
});

// =============================================================================
// §5 + §6.4 (r133) — E16_tier2: the W7 oracle = sha-gated PROJECTION of the
// banked join oracle (17 tier-2 rows, generator scripts/r133_tier2_oracle.py,
// hand-pin gates ALL green pre-emit). Arms: fixture statics + the SET-
// INCLUSION identity (E16 ⊂ E15, keys AND per-region values), the monoid
// closure through the REAL merge path, the full-pipeline W7 wave, and the
// W7-shaped dim-labeled envelope_invalid twin (the §2.4 law is
// variant-independent).
// =============================================================================

/** The W7 tier2 request: the W6 descriptor + variant:'tier2' (the ONE field
 *  that flips derivation to W7 — design_r132_w7_family §2.2). */
function tier2JoinReqBody(): Record<string, unknown> {
  return joinReqBody({ query: { join: { table: 'wh_probe_dim', type: 'inner', on: { left: 'region', right: 'region' }, variant: 'tier2' } } });
}

const W7_PLAN: WhMergePlan = buildMergePlan(parseWhEngineRequest(tier2JoinReqBody()), { columnTypes: COLS, columnScales: SCALES });

/** W7-shaped grouped partial — the exact per-shard shape the W7 template
 *  emits post-adaptation. W7's body is W6 + the ONE tier-2 delta line, so
 *  the ENVELOPE is byte-identical to W6's (same aggs {x,c,n}, same
 *  groupKeys ['region'], same schema_version; the variant lives in the
 *  manifest/template, NEVER in the envelope) — only the ROW SET differs
 *  (17 tier-2 regions, n_r = 280 each). Same fixed-point TEXT convention
 *  as w6env (x rides as String). */
function w7env(shard: string, rows: E15Row[]): WhPartialEnvelope {
  return {
    v: 1,
    shard,
    table: 'wh_probe_agg',
    schema_version: 1,
    partial: {
      kind: 'grouped',
      groupKeys: ['region'],
      aggs: { x: { op: 'sum', col: 'amount' }, c: { op: 'count', col: 'amount' }, n: { op: 'count' } },
      rows: rows.map((r) => ({ k: [r.region], a: { x: String(r.x), c: r.c, n: r.n } })),
      rowCount: rows.length,
      more: false,
    },
  };
}

Deno.test('r133 E16 provenance + SET-INCLUSION: the tier2 oracle is a strict SUBSET of the E15 oracle (17 ⊂ 49 region keys, per-region values identical), fixture self-pinned', () => {
  eqTrue('E16 id/fixture are the §5 schema literals ("E16_tier2" ×2 — the audit-B P2-2 fold: the schema is fully determined)', E16.id === 'E16_tier2' && E16.meta.fixture === 'E16_tier2');
  eqTrue('meta.source names the canonical oracle file AND its sha256 (the E15 provenance convention — never a silent re-derivation)', E16.meta.source.includes('audit/r128_join_oracle/join_oracle.json') && E16.meta.source.includes('e5959512546b9d00591af265c944911101314c5ce8cdf9470618d09b7f298172'));
  eq('17 rows + globals + per_band are the hand pins (§5: Σx 22,264,985 / Σc 4,711 / Σn 4,760; per-band n 2040/1360/1360, Σx 9,506,717/6,438,027/6,320,241)', [E16.rows.length, E16.globals, E16.per_band], [17, { rows: 17, sum_c: 4711, sum_n: 4760, sum_x: 22264985 }, { A: { n: 2040, sum_x: 9506717 }, B: { n: 1360, sum_x: 6438027 }, C: { n: 1360, sum_x: 6320241 } }]);
  eq('the 17 region keys are EXACTLY the hand-derived tier-2 set (k ≡ 1 mod 3: g01…g49 — tier = (k % 3) + 1)', E16.rows.map((r) => r.region), ['g01', 'g04', 'g07', 'g10', 'g13', 'g16', 'g19', 'g22', 'g25', 'g28', 'g31', 'g34', 'g37', 'g40', 'g43', 'g46', 'g49']);
  eqTrue('region-ascending AND n_r = 280 ∀17 (uniform 120+80+80 by the coprime construction — Σn = 17 × 280 = 4,760)', E16.rows.every((r, i, a) => i === 0 || a[i - 1].region < r.region) && E16.rows.every((r) => r.n === 280));
  const E15RegionKeys = new Set(E15.rows.map((r) => r.region));
  eq('SET-INCLUSION (the §5 identity, pinned explicitly): every E16 region key ∈ the E15 region keys (17 ⊂ 49)', [E16.rows.every((r) => E15RegionKeys.has(r.region)), E15.rows.length], [true, 49]);
  eqTrue('per-region value identity: each E16 row {x,c,n} EQUALS its E15 twin (a strict subset of the 49-row oracle — a tier-predicate-drop mutant emits tier-1/3 rows → EXACT mismatch RED, K-W7b)', E16.rows.every((r) => {
    const twin = E15.rows.find((e) => e.region === r.region);
    return twin !== undefined && twin.x === r.x && twin.c === r.c && twin.n === r.n;
  }));
  eq('closure: the tier-1+3 complement (E15 − E16) is the hand-derived residual {Σx 42,256,498, Σc 8,867, Σn 8,960} (t1+t2+t3 = banked, ALL EXACT)', [E15.globals.sum_x - E16.globals.sum_x, E15.globals.sum_c - E16.globals.sum_c, E15.globals.sum_n - E16.globals.sum_n], [42256498, 8867, 8960]);
  eq('per-band closure: A+B+C reassembles the tier-2 globals (Σx 9,506,717 + 6,438,027 + 6,320,241 = 22,264,985; n 2040+1360+1360 = 4760)', [E16.per_band.A.sum_x + E16.per_band.B.sum_x + E16.per_band.C.sum_x, E16.per_band.A.n + E16.per_band.B.n + E16.per_band.C.n], [22264985, 4760]);
  eqTrue('excluded.note pins the g47-losslessness statement (tier-2 loses NO region to the omission — g47 is tier-3, excluded in the BANKED oracle)', typeof E16.excluded.note === 'string' && E16.excluded.note.includes('g47') && E16.excluded.note.includes('tier-3'));
});

Deno.test('r133 E16 monoid closure: 17 W7-shaped one-region partials merge + finalize through the REAL merge path to EXACTLY the tier2 oracle', () => {
  const envs = E16.rows.map((r) => w7env(`e16-${r.region}`, [r]));
  const merged = mergeGroupedPartials(W7_PLAN, envs);
  eq('merged partial keeps the rowCount invariant (17 groups from 17 one-region shard partials)', merged.partial.rowCount, 17);
  const fin = finalizeGroups(merged, W7_PLAN);
  eq('finalize(merged) === E16 rows EXACT (17 rows, x as the scale-0 bigint carrier, canonical order)', fin, E16.rows.map((r) => ({ k: [r.region], aggs: { x: BigInt(r.x), c: r.c, n: r.n } })));
  const finRev = finalizeGroups(mergeGroupedPartials(W7_PLAN, [...envs].reverse()), W7_PLAN);
  eq('monoid closure: reversed-arrival merge finalizes to the SAME 17 rows (assoc + comm spot pin)', finRev, fin);
  const sumX = fin.reduce((a, r) => a + (r.aggs.x as bigint), 0n);
  const sumC = fin.reduce((a, r) => a + (r.aggs.c as number), 0);
  const sumN = fin.reduce((a, r) => a + (r.aggs.n as number), 0);
  eq('hand-computed tier2 globals: Σx 22,264,985 / Σc 4,711 / Σn 4,760 (the §5 pins through the merge path)', [sumX.toString(), sumC, sumN], ['22264985', 4711, 4760]);
  eqTrue('every merged region is tier-2 by the (k % 3) + 1 rule AND g47 ABSENT (a dropped tier predicate emits tier-1/3 regions → row-count AND set-difference RED)', fin.every((r) => (parseInt(r.k[0].slice(1), 10) % 3) + 1 === 2) && !fin.some((r) => r.k[0] === 'g47'));
});

// Shard slices for the W7 wave: A = g01..g16 (6 rows), B = g19..g37 (6), C = g40..g49 (5). 6+6+5 = 17.
const E16_A = E16.rows.slice(0, 6);
const E16_B = E16.rows.slice(6, 12);
const E16_C = E16.rows.slice(12, 17);
const w7envA = w7env('shard-a', E16_A);
const w7envB = w7env('shard-b', E16_B);
const w7envC = w7env('shard-c', E16_C);

Deno.test('r133 W7 fanout: the FULL E16 tier2 oracle through the real pipeline (17 rows EXACT), perShard ok-arm pins + the W7-shaped dim-labeled envelope_invalid twin', async () => {
  const seen: SeenCall[] = [];
  const res = await executeWhQuery(joinExecArgs({
    reqOverride: parseWhEngineRequest(tier2JoinReqBody()),
    facts: [factRow('shard-a'), factRow('shard-b'), factRow('shard-c')],
    dims: [dimRow('shard-a', { isReference: true }), dimRow('shard-b', { isReference: true }), dimRow('shard-c', { isReference: true })],
    fetcher: recordingFetch({ 'shard-a': w7envA, 'shard-b': w7envB, 'shard-c': w7envC }, seen),
    templateHashes: [W7H],
  }));
  eq('tier2 colocated wave: coverage 3/3, partial false, warnings [], exactly 3 shard POSTs', [res.coverage, res.partial, res.warnings, seen.length], ['3/3', false, [], 3]);
  eq('finalize output === E16 rows EXACT (17 rows through gate → fanout → merge → finalize)', res.rows, E16.rows.map((r) => ({ k: [r.region], aggs: { x: BigInt(r.x), c: r.c, n: r.n } })));
  eq('perShard exact shape (R-3 law: exact ok-arm key set, partial_rows = consumed rowCount 6/6/5)', res.perShard, [
    { shard: 'shard-a', ok: true, latencyMs: res.perShard[0]?.latencyMs, error: null, partial_rows: 6, partial_bytes: bytes(w7envA.partial) },
    { shard: 'shard-b', ok: true, latencyMs: res.perShard[1]?.latencyMs, error: null, partial_rows: 6, partial_bytes: bytes(w7envB.partial) },
    { shard: 'shard-c', ok: true, latencyMs: res.perShard[2]?.latencyMs, error: null, partial_rows: 5, partial_bytes: bytes(w7envC.partial) },
  ]);
  const sumX = (res.rows ?? []).reduce((a, r) => a + (r.aggs.x as bigint), 0n);
  const sumC = (res.rows ?? []).reduce((a, r) => a + (r.aggs.c as number), 0);
  const sumN = (res.rows ?? []).reduce((a, r) => a + (r.aggs.n as number), 0);
  eqTrue('globals recompute from the ENGINE output: Σx 22,264,985 / Σc 4,711 / Σn 4,760 (the E16 globals ride the live path)', sumX.toString() === '22264985' && sumC === 4711 && sumN === 4760);
  const dimLabeledW7 = { ...w7env('s-dim', [E16.rows[0]]), table: 'wh_probe_dim' };
  throwsMerge('W7-shaped partial labeled with the DIM table => envelope_invalid vs the W7 plan (the §2.4 envelope law is variant-independent — the W6 twin is the r129 arm above)', () => mergeGroupedPartials(W7_PLAN, [dimLabeledW7]), 'envelope_invalid');
});

// =============================================================================
// r144 R9 W8 — the FIRST grouped-avg join template (agent-ctx/r140-w7-census.md
// §6 pre-registered deltas). W8 = W7's body with the ONE delta byte `x`→`s`;
// manifest row idx7 merge_ops [groupby,avg_pair,count,count_col]; the tier2
// variant partition now ALSO op-set-partitions (AVG ⇒ W8 only, SUM ⇒ W7 only,
// mixed ⇒ ZERO). The adapter fuses the plan's avg(amount) from the template's
// same-col s+c encodings (avgPairEncoding, gated on the avg_pair DECLARATION)
// while the plan's count(amount) maps DIRECT to the SAME `c` encoding (the
// sole count≡count_col equivalence, r130) — legal twin consumption of one
// encoding in one wave. W8's merged per-region avg is the EXACT rational pair
// {num: x_r, den: c_r} — never a float, never the mean-of-means (the E2 law
// rides the join plane; the 3-band folds below are the live discriminators).
// =============================================================================

/** The W8 tier2-avg request (census §6): avg(amount) unaliased (the
 *  buildMergePlan name 'avg(amount)'), count(amount) alias c, count(*) alias
 *  n, grouped by region, the tier2 join descriptor. */
function w8avgReqBody(): Record<string, unknown> {
  return {
    v: 1,
    qid: '01J9Q1ZZZZZZZZZZZZZZZZZZZZ',
    table: 'wh_probe_agg',
    query: {
      select: [
        { op: 'avg', col: 'amount' },
        { op: 'count', col: 'amount', alias: 'c' },
        { op: 'count', alias: 'n' },
      ],
      groupBy: ['region'],
      join: { table: 'wh_probe_dim', type: 'inner', on: { left: 'region', right: 'region' }, variant: 'tier2' },
    },
  };
}

const W8_PLAN: WhMergePlan = buildMergePlan(parseWhEngineRequest(w8avgReqBody()), { columnTypes: COLS, columnScales: SCALES });

/** W8-shaped grouped partial — the exact per-shard shape the W8 template
 *  emits POST-ADAPTATION (the flat wire's encoding keys s/c/n remap to the
 *  plan's own names: the fused avg pair under 'avg(amount)' plus the direct
 *  c/n). Same fixed-point TEXT convention as w6env/w7env. */
function w8env(shard: string, rows: E15Row[]): WhPartialEnvelope {
  return {
    v: 1,
    shard,
    table: 'wh_probe_agg',
    schema_version: 1,
    partial: {
      kind: 'grouped',
      groupKeys: ['region'],
      aggs: { 'avg(amount)': { op: 'avg', col: 'amount' }, c: { op: 'count', col: 'amount' }, n: { op: 'count' } },
      rows: rows.map((r) => ({ k: [r.region], a: { 'avg(amount)': { s: String(r.x), c: r.c }, c: r.c, n: r.n } })),
      rowCount: rows.length,
      more: false,
    },
  };
}

/** The FLAT §6.2 W8 wire (the seeded fn shape — encoding keys s/c/n, the W8
 *  body's row_json). The rpc-plane adapter consumes THIS shape. */
function w8wire(rows: E15Row[]): Record<string, unknown> {
  return {
    v: 1,
    table: 'wh_probe_agg',
    schema_version: 1,
    qc_class: 'QC6',
    kind: 'grouped',
    groupKeys: ['region'],
    aggs: { s: { op: 'sum', col: 'amount' }, c: { op: 'count_col', col: 'amount' }, n: { op: 'count' } },
    rows: rows.map((r) => ({ k: [r.region], a: { s: String(r.x), c: r.c, n: r.n } })),
    rowCount: rows.length,
    truncated: false,
    encoding: { s: 'text', c: 'number', n: 'number' },
    template_hash: W8H,
    template_timeout_ms: 8000,
    latencyMs: 5,
  };
}

/** Hand-derived E16 projection (census §6): W8's finalize row per region r is
 *  EXACTLY {k:[r], 'avg(amount)': {num: x_r, den: c_r}, c: c_r, n: 280} —
 *  lifted from the fixture's own numbers in-test (never hardcoded floats,
 *  never derived from a run). */
const E16_W8_FINAL = E16.rows.map((r) => ({
  k: [r.region],
  aggs: { 'avg(amount)': { num: BigInt(r.x), den: BigInt(r.c) }, c: r.c, n: r.n },
}));

/** The wh_entrypoint.ts:493-507 derivation plan-view shape, mirrored
 *  verbatim: table + groupKeys = query.groupBy (PRESENCE is the kind law —
 *  grouped↔'rows') + join? (the class/variant partition rides PRESENCE +
 *  variant) + aggs keyed alias ?? `${op}_${i}`. */
function entrypointPlanView(req: WhEngineRequest): Record<string, unknown> {
  return {
    table: req.table,
    ...(req.query.groupBy !== undefined ? { groupKeys: req.query.groupBy } : {}),
    ...(req.query.join !== undefined ? { join: req.query.join } : {}),
    aggs: Object.fromEntries(
      req.query.select.map((s, i) => [s.alias ?? `${s.op}_${i}`, { op: s.op, ...(s.col !== undefined ? { col: s.col } : {}) }]),
    ),
  };
}

Deno.test('r144 W8 derivation census (e2e): tier2-avg ⇒ [W8H] ONLY, tier2-sum ⇒ [W7H] ONLY; MIXED sum+avg and BASE non-join avg both derive ZERO ⇒ D2 plan_untemplated (planner-honest)', async () => {
  // REAL derivation (the wh_entrypoint.ts:486 plan-view shape — table +
  // groupKeys? + join? + aggs — against the REAL ENGINE_TEMPLATE_MANIFEST;
  // the r134 arm's convention). The unit-level op-set legs are pinned in the
  // r144 legs of the derivation-partition block above; these are the
  // plan-view shapes the ENGINE actually derives from the parsed requests.
  const avgPlan = parseWhEngineRequest(w8avgReqBody());
  eq('tier2-avg plan view derives [W8H] ONLY (W6/W7 lack avg_pair — the op-set partition completes the variant partition)', deriveTemplateHashes(entrypointPlanView(avgPlan) as never, 1), [W8H]);
  const sumPlan = parseWhEngineRequest(tier2JoinReqBody());
  eq('tier2-sum plan view derives [W7H] ONLY (W8 carries NO sum token — the census arm through the REAL view)', deriveTemplateHashes(entrypointPlanView(sumPlan) as never, 1), [W7H]);
  // MIXED sum+avg tier2 plan: NO template serves the union (W8 lacks sum,
  // W6/W7 lack avg_pair) ⇒ zero derivation ⇒ the D2 gate owns the outcome
  // (the join gates deliberately skip zero derivation — matchedJoinRow null).
  const mixedReq = parseWhEngineRequest({ ...w8avgReqBody(), query: { ...(w8avgReqBody().query as Record<string, unknown>), select: [{ op: 'avg', col: 'amount' }, { op: 'sum', col: 'amount', alias: 'x' }], groupBy: ['region'] } });
  eq('MIXED sum+avg tier2 plan derives ZERO (the pre-registered census arm — no member serves {groupby,avg_pair,sum})', deriveTemplateHashes(entrypointPlanView(mixedReq) as never, 1), []);
  const seenMixed: SeenCall[] = [];
  await rejectsEngine('mixed tier2 join under rpcMode => plan_untemplated (D2 — NOT a join-gate reject; planner-honest)', () =>
    executeWhQuery(joinExecArgs({ fetcher: recordingFetch({}, seenMixed), templateHashes: [], rpcMode: true, reqOverride: mixedReq })), 'plan_untemplated', 'derives 0 template hash');
  eq('mixed-arm D2 fired before any network: zero POSTs', seenMixed, []);
  // BASE (non-join) avg GROUPED plan: the join class is partition-EXCLUDED
  // and W3 is scalar-kind (the derivation kind law — grouped↔'rows') ⇒ zero.
  const baseAvgReq = parseWhEngineRequest({ v: 1, qid: '01J9Q1ZZZZZZZZZZZZZZZZZZZZ', table: 'wh_probe_agg', query: { select: [{ op: 'avg', col: 'amount' }], groupBy: ['region'] } });
  eq('BASE non-join avg GROUPED plan derives ZERO (W1 lacks avg_pair; the join class is partition-excluded; W3 is scalar-kind)', deriveTemplateHashes(entrypointPlanView(baseAvgReq) as never, 1), []);
  const seenBase: SeenCall[] = [];
  await rejectsEngine('base avg grouped plan under rpcMode => plan_untemplated (the FIRST grouped avg is join-class ONLY — zero POSTs)', () =>
    executeWhQuery(joinExecArgs({ fetcher: recordingFetch({}, seenBase), templateHashes: [], rpcMode: true, reqOverride: baseAvgReq })), 'plan_untemplated', 'derives 0 template hash');
  eq('base-arm D2 fired before any network: zero POSTs', seenBase, []);
});

Deno.test('r144 E16 avg-pair monoid closure: 17 W8-shaped one-region partials merge + finalize through the REAL merge path to EXACTLY the E16 projection (exact rationals, never floats)', () => {
  const envs = E16.rows.map((r) => w8env(`e16-w8-${r.region}`, [r]));
  const merged = mergeGroupedPartials(W8_PLAN, envs);
  eq('merged partial keeps the rowCount invariant (17 groups from 17 one-region shard partials)', merged.partial.rowCount, 17);
  const fin = finalizeGroups(merged, W8_PLAN);
  eq('finalize(merged) === the hand-derived E16 projection EXACT (17 rows region-ascending; per-region avg = the EXACT rational pair {num: x_r, den: c_r} lifted from the fixture)', fin, E16_W8_FINAL);
  const finRev = finalizeGroups(mergeGroupedPartials(W8_PLAN, [...envs].reverse()), W8_PLAN);
  eq('monoid closure: reversed-arrival merge finalizes to the SAME 17 rows (assoc + comm spot pin)', finRev, fin);
  const sumNum = fin.reduce((a, r) => a + (r.aggs['avg(amount)'] as { num: bigint; den: bigint }).num, 0n);
  const sumDen = fin.reduce((a, r) => a + (r.aggs['avg(amount)'] as { num: bigint; den: bigint }).den, 0n);
  const sumN = fin.reduce((a, r) => a + (r.aggs.n as number), 0);
  eq('hand-derived globals: Σnum 22,264,985 / Σden 4,711 / Σn 4,760 (the E16 globals through the avg-pair path)', [sumNum.toString(), sumDen.toString(), sumN], ['22264985', '4711', 4760]);
  eqTrue('every finalized pair is the region-ascending tier-2 set with den = c_r > 0 (no NULL avg rows in THIS corpus — the census §6 pre-registered non-arm; all-null finalize stays the scalar lane E2/E14)', fin.every((r, i) => r.k[0] === E16.rows[i].region && (r.aggs['avg(amount)'] as { num: bigint; den: bigint }).den === BigInt(E16.rows[i].c)));
});

// r145 P3-4 rider (r144 review-A ledger; census §6 envelope leg): `phases`
// present on the W8/W7 tier2 family SUCCESS envelopes — the r121 phases law
// never reached the join lane (zero `phases` matches in this file since
// r133; the scalar-lane mirror is the wh_handshake_test.ts ADD #4 pin).
// Threaded preChain + the counting timers: pre_chain EXACT (the threaded
// entrypoint value), handshake EXACT 0 (the unsampled fold steady state),
// fanout MEASURED (the runFanout block wall — a lower bound by law, never
// hardcoded: the r121 house style).
Deno.test('r145 P3-4 (census §6 envelope leg): the W8/W7 tier2 join-family success envelopes carry `phases` — pre_chain threaded exact, handshake 0 unsampled, fanout measured', async () => {
  const stepTimers = (): WhEngineTimers => {
    let n = 0;
    return { nowMs: () => (n += 3), startTimeout: (_ms: number) => ({ promise: new Promise<'timeout'>(() => {}), dispose: () => {} }) };
  };
  const oneShardWave = (reqBody: Record<string, unknown>, templateHash: string, env: WhPartialEnvelope): Parameters<typeof executeWhQuery>[0] => ({
    ...joinExecArgs({
      reqOverride: parseWhEngineRequest(reqBody),
      facts: [factRow('shard-a')],
      dims: [dimRow('shard-a', { isReference: true })],
      fetcher: recordingFetch({ 'shard-a': env }, []),
      templateHashes: [templateHash],
    }),
    timers: stepTimers(),
    timings: { preChainMs: 123 },
  });
  // W8 (tier2-avg): `phases` rides the SAME post-assembly injection site
  // the scalar lane pins — exact where hand-computable, measured where it
  // is a block wall.
  const res8 = await executeWhQuery(oneShardWave(w8avgReqBody(), W8H, w8env('shard-a', E16.rows)));
  eq('W8 tier2-avg envelope: pre_chain_ms is the THREADED 123 and handshake_ms the unsampled 0 (the r121 phases law reaches the join family — census §6 "phases present")', [res8.phases?.pre_chain_ms, res8.phases?.handshake_ms], [123, 0]);
  eqTrue('W8 fanout_ms MEASURED (>= one counting step — the runFanout block wall; a dropped phases injection or a hardcoded 0 REDs)', (res8.phases?.fanout_ms ?? 0) >= 1);
  // W7 (tier2-sum): BOTH tier2 family members carry the leg.
  const res7 = await executeWhQuery(oneShardWave(tier2JoinReqBody(), W7H, w7env('shard-a', E16.rows)));
  eq('W7 tier2-sum envelope: the SAME exact {pre_chain 123, handshake 0} phases pair (both W8/W7 family members carry the leg)', [res7.phases?.pre_chain_ms, res7.phases?.handshake_ms], [123, 0]);
  eqTrue('W7 fanout_ms MEASURED (the block wall — non-vacuous vs a hardcoded {123, 0, 0})', (res7.phases?.fanout_ms ?? 0) >= 1);
});

// -----------------------------------------------------------------------------
// r144 fanout banding (test-side hand rule): the E16 fixture pins the
// per-band TOTALS only (per_band {n, sum_x} — the banked oracle's projection
// home), so the per-band per-region split is the deterministic
// largest-remainder apportionment of each region's x_r against the pinned
// band targets. Per-region folds stay EXACT (s_A+s_B+s_C = x_r,
// c_A+c_B+c_C = c_r; n 120+80+80 = 280 by the corpus's uniform coprime
// banding), and the pinned per-band Σs/Σn are asserted where the partials
// are built. The per-band null placement is NOT pinned by the fixture; the
// hand rule gives bands B/C the full 80 and band A the residual
// (c_A = c_r − 160 — the ≤3 nulls land in band A).
// -----------------------------------------------------------------------------
function bandSums(target: number): number[] {
  const X = E16.globals.sum_x;
  const floors = E16.rows.map((r) => Math.floor((r.x * target) / X));
  let rem = target - floors.reduce((a, b) => a + b, 0);
  const byRemainder = E16.rows.map((r, i) => ({ i, frac: (r.x * target) % X })).sort((a, b) => b.frac - a.frac || a.i - b.i);
  for (const { i } of byRemainder) {
    if (rem === 0) break;
    floors[i] += 1;
    rem -= 1;
  }
  return floors;
}
const S_BAND_A = bandSums(E16.per_band.A.sum_x);
const S_BAND_B = bandSums(E16.per_band.B.sum_x);
const S_BAND_C = E16.rows.map((r, i) => r.x - S_BAND_A[i] - S_BAND_B[i]);
const C_BAND_A = E16.rows.map((r) => r.c - 160);
const N_BAND: Record<'A' | 'B' | 'C', number> = { A: 120, B: 80, C: 80 };
const bandRow = (band: 'A' | 'B' | 'C', i: number): E15Row => ({
  region: E16.rows[i].region,
  x: band === 'A' ? S_BAND_A[i] : band === 'B' ? S_BAND_B[i] : S_BAND_C[i],
  c: band === 'A' ? C_BAND_A[i] : 80,
  n: N_BAND[band],
});
const W8_BANDS: Record<'A' | 'B' | 'C', E15Row[]> = {
  A: E16.rows.map((_, i) => bandRow('A', i)),
  B: E16.rows.map((_, i) => bandRow('B', i)),
  C: E16.rows.map((_, i) => bandRow('C', i)),
};
const w8envA = w8env('shard-a', W8_BANDS.A);
const w8envB = w8env('shard-b', W8_BANDS.B);
const w8envC = w8env('shard-c', W8_BANDS.C);

Deno.test('r144 W8 fanout: the FULL E16 tier2 avg oracle through the real pipeline (17 rows EXACT, partials 17×3), perShard ok-arm pins + the W8 dim-labeled envelope_invalid twin', async () => {
  eq('band construction folds EXACTLY (the fixture pins per-band TOTALS only — the apportionment must hit them; a per_band/globals inconsistency would surface as a negative s_C here)', [
    W8_BANDS.A.reduce((a, r) => a + r.x, 0),
    W8_BANDS.B.reduce((a, r) => a + r.x, 0),
    W8_BANDS.C.reduce((a, r) => a + r.x, 0),
    W8_BANDS.A.reduce((a, r) => a + r.n, 0),
    W8_BANDS.B.reduce((a, r) => a + r.n, 0),
    W8_BANDS.C.reduce((a, r) => a + r.n, 0),
  ], [E16.per_band.A.sum_x, E16.per_band.B.sum_x, E16.per_band.C.sum_x, E16.per_band.A.n, E16.per_band.B.n, E16.per_band.C.n]);
  const seen: SeenCall[] = [];
  const res = await executeWhQuery(joinExecArgs({
    reqOverride: parseWhEngineRequest(w8avgReqBody()),
    facts: [factRow('shard-a'), factRow('shard-b'), factRow('shard-c')],
    dims: [dimRow('shard-a', { isReference: true }), dimRow('shard-b', { isReference: true }), dimRow('shard-c', { isReference: true })],
    fetcher: recordingFetch({ 'shard-a': w8envA, 'shard-b': w8envB, 'shard-c': w8envC }, seen),
    templateHashes: [W8H],
  }));
  eq('tier2-avg colocated wave: coverage 3/3, partial false, warnings [] (silent-when-exact law), exactly 3 shard POSTs', [res.coverage, res.partial, res.warnings, seen.length], ['3/3', false, [], 3]);
  eq('finalize output === the E16 projection EXACT (17 rows; per-region avg = {num: x_r, den: c_r} — the 3-band fold is the EXACT rational, never the mean-of-means)', res.rows, E16_W8_FINAL);
  eq('perShard exact shape (R-3 law: exact ok-arm key set, partial_rows 17×3 — the unsharded echo)', res.perShard, [
    { shard: 'shard-a', ok: true, latencyMs: res.perShard[0]?.latencyMs, error: null, partial_rows: 17, partial_bytes: bytes(w8envA.partial) },
    { shard: 'shard-b', ok: true, latencyMs: res.perShard[1]?.latencyMs, error: null, partial_rows: 17, partial_bytes: bytes(w8envB.partial) },
    { shard: 'shard-c', ok: true, latencyMs: res.perShard[2]?.latencyMs, error: null, partial_rows: 17, partial_bytes: bytes(w8envC.partial) },
  ]);
  const sumNum = (res.rows ?? []).reduce((a, r) => a + (r.aggs['avg(amount)'] as { num: bigint; den: bigint }).num, 0n);
  const sumDen = (res.rows ?? []).reduce((a, r) => a + (r.aggs['avg(amount)'] as { num: bigint; den: bigint }).den, 0n);
  const sumN = (res.rows ?? []).reduce((a, r) => a + (r.aggs.n as number), 0);
  eqTrue('globals recompute from the ENGINE output: Σnum 22,264,985 / Σden 4,711 / Σn 4,760 (the E16 globals ride the live path)', sumNum.toString() === '22264985' && sumDen.toString() === '4711' && sumN === 4760);
  eqTrue('non-vacuity (r121 law): the mean-of-means of the 3 band pairs DIFFERS from the exact rational for at least one region — the finalize EXACT pin is a LIVE mean-of-means discriminator', E16.rows.some((_, i) => (W8_BANDS.A[i].x / W8_BANDS.A[i].c + W8_BANDS.B[i].x / W8_BANDS.B[i].c + W8_BANDS.C[i].x / W8_BANDS.C[i].c) / 3 !== E16.rows[i].x / E16.rows[i].c));
  const dimLabeledW8 = { ...w8env('s-dim', [W8_BANDS.A[0]]), table: 'wh_probe_dim' };
  throwsMerge('W8-shaped partial labeled with the DIM table => envelope_invalid vs the W8 plan (the §2.4 envelope law is variant-independent — the W6/W7 twins above)', () => mergeGroupedPartials(W8_PLAN, [dimLabeledW8]), 'envelope_invalid');
});

Deno.test('r144 W8 twin-consumption pin (rpc plane): the adapter fuses avg(amount) from s+c AND maps count(amount) DIRECT to the SAME c encoding — both consumed legally in one wave; the exact 17-row merged shape is pinned', () => {
  const w8row = manifestRowByHash(W8H);
  if (w8row === null) throw new Error('W8 manifest row missing — the manifest statics block would already have REDd');
  eqTrue('pre: the W8 manifest row IS the adapter template view (kind rows + merge_ops declaring avg_pair + the s/c/n encodings)', w8row.kind === 'rows' && w8row.merge_ops.includes('avg_pair') && w8row.aggs.s.op === 'sum' && w8row.aggs.c.op === 'count_col');
  const adapted = [W8_BANDS.A, W8_BANDS.B, W8_BANDS.C].map((band) => adaptWireEnvelope(w8wire(band), w8row, W8_PLAN));
  eqTrue('all three band wires adapt through the REAL adapter (every plan agg served — nothing excluded)', adapted.every((a) => a !== null));
  const firstRow = (adapted[0] as NonNullable<typeof adapted[number]>).partial.rows[0].a;
  eq('the fused avg pair AND the direct c count BOTH carry values from the SAME encoding key (twin consumption: pair {s,c} + bare c, both the wire c)', [firstRow['avg(amount)'], firstRow.c], [{ s: String(W8_BANDS.A[0].x), c: W8_BANDS.A[0].c }, W8_BANDS.A[0].c]);
  eqTrue('twin-consumption identity holds on EVERY adapted row of EVERY band (pair.c === direct c — one encoding, two legal consumers)', adapted.every((a) => (a as NonNullable<typeof a>).partial.rows.every((row) => (row.a['avg(amount)'] as { c: number }).c === row.a.c)));
  // The adapter returns the shard-less Omit shape — the CALL SITE stamps
  // shard (the :2183 convention) before the merge consumes it.
  const merged = mergeGroupedPartials(W8_PLAN, adapted.map((a, i) => ({ ...(a as NonNullable<typeof a>), shard: `rpc-band-${['A', 'B', 'C'][i]}` })));
  eq('merged through the REAL merge path: EXACT 17-row shape — per region the fused pair {s: x_r text, c: c_r} AND the direct c count, n 280 (the accToWire wire shapes)', merged.partial.rows, E16.rows.map((r) => ({ k: [r.region], a: { 'avg(amount)': { s: String(r.x), c: r.c }, c: r.c, n: r.n } })));
  const fin = finalizeGroups(merged, W8_PLAN);
  eq('finalize(merged) === the E16 projection EXACT (the adapter → merge → finalize rpc-plane path lands the same exact rationals)', fin, E16_W8_FINAL);
});

// -----------------------------------------------------------------------------
// r144 KILLER K-W8 (agent-ctx/r140-w7-census.md §6, the lethal arm): the avg
// fusion must NOT fire when the template does NOT declare avg_pair —
// avgPairEncoding's gate (wh_engine_core.ts:1226-1227). Call chain (the
// RED-proof): adaptWireEnvelope (:1278) → the plan-agg loop (:1324-1331:
// plan op 'avg' → avgPairEncoding(template, col); pair === null ⇒ return
// null — the loud excluded path upstream) → avgPairEncoding (:1226: `if
// (template.merge_ops !== undefined && !template.merge_ops.includes
// ('avg_pair')) return null;` → the structural scan). The lethal view below
// carries a REAL same-col sum+count_col pair, so the scan WOULD find
// {sumKey:'s', countKey:'c'} — the gate line is the ONLY code between the
// wire and a fused pair: the parent's gate-deletion mutant fuses here ⇒ this
// arm REDs (commit-before-mutant law; the W2 no-pair twin in
// wh_shard_channel_test.ts stays green under the same mutant — it cannot
// see the gate). The merge_ops-UNDEFINED control proves the scan finds the
// pair on the IDENTICAL aggs (the mutant is not saved by an empty scan).
// -----------------------------------------------------------------------------
Deno.test('r144 K-W8 (lethal): a same-col sum+count_col pair WITHOUT the avg_pair declaration NEVER fuses — the avg plan agg fails closed (null ⇒ the call-site excluded path); merge_ops-undefined control proves the pair is findable', () => {
  const w5ClassView = {
    kind: 'rows',
    aggs: { s: { op: 'sum', col: 'amount' }, c: { op: 'count_col', col: 'amount' } },
    merge_ops: ['sum', 'count_col'], // the W5-class shape — a genuine sum+count_col encoding, NO avg_pair
  };
  const avgPlanView = { aggs: { 'avg(amount)': { op: 'avg', col: 'amount' } } };
  eqTrue('K-W8: avg against a DEFINED merge_ops WITHOUT avg_pair (same-col s/c pair present) => null — NOT fused, the avg plan agg is unserved (the gate, not an empty scan)', adaptWireEnvelope(w8wire([W8_BANDS.A[0]]), w5ClassView, avgPlanView) === null);
  eqTrue('control: the IDENTICAL aggs with merge_ops UNDEFINED => the structural reading FUSES (the scan finds {s,c} — only the declaration gate blocks the lethal arm)', adaptWireEnvelope(w8wire([W8_BANDS.A[0]]), { kind: 'rows', aggs: w5ClassView.aggs }, avgPlanView) !== null);
  const w8rowK = manifestRowByHash(W8H);
  eqTrue('positive control: the SAME aggs with avg_pair DECLARED (the real W8 row) => fuses (the declaration is the sole discriminator)', w8rowK !== null && adaptWireEnvelope(w8wire([W8_BANDS.A[0]]), w8rowK, avgPlanView) !== null);
});

Deno.test('r144 W8 fetch_rows (rpc plane): grouped ⇒ p_params={} byte-pin for ALL K (the r139 law on the W8 surface); rows = the first K of the 17 canonical key-asc; partial_rows still 17×3', async () => {
  const seen: SeenCall[] = [];
  const k5Req = parseWhEngineRequest({ ...w8avgReqBody(), query: { ...(w8avgReqBody().query as Record<string, unknown>), fetch_rows: 5 } });
  const res = await executeWhQuery(joinExecArgs({
    reqOverride: k5Req,
    facts: [factRow('shard-a'), factRow('shard-b'), factRow('shard-c')],
    dims: [dimRow('shard-a', { isReference: true }), dimRow('shard-b', { isReference: true }), dimRow('shard-c', { isReference: true })],
    fetcher: recordingFetch({ 'shard-a': w8wire(W8_BANDS.A), 'shard-b': w8wire(W8_BANDS.B), 'shard-c': w8wire(W8_BANDS.C) }, seen),
    templateHashes: [W8H],
    rpcMode: true,
  }));
  eq('rpc specs byte-pin: {p_template_hash: W8H, p_params: {}} ×3 (grouped ⇒ {} — the per-shard $2 threading stays retired on the W8 surface)', seen.map((s) => s.rpc), [
    { p_template_hash: W8H, p_params: {} },
    { p_template_hash: W8H, p_params: {} },
    { p_template_hash: W8H, p_params: {} },
  ]);
  eq('response rows = the FIRST 5 of the 17 canonical key-asc (the post-merge slice: g01,g04,g07,g10,g13)', res.rows, E16_W8_FINAL.slice(0, 5));
  eq('perShard partial_rows still 17×3 (the unsharded echo — ok arms keep the measurement keys, bytes > 0)', res.perShard.map((p) => [p.ok, p.partial_rows, (p.partial_bytes ?? 0) > 0]), [[true, 17, true], [true, 17, true], [true, 17, true]]);
  eq('the clamp did not disturb the wave: coverage 3/3, partial false, warnings []', [res.coverage, res.partial, res.warnings], ['3/3', false, []]);
});

// =============================================================================
// §6.6 — rpcParams arms (r139 R4 re-pin): join plans are GROUPED ⇒ p_params
// is {} for ALL K; the join descriptor adds NO new params (span params stay
// DEFERRED — design §2.3).
// =============================================================================
Deno.test('r139 R4 rpcParams (r129 re-pin): join plans are GROUPED — ALL K arms => {} (per-shard LIMIT retired); the join descriptor still adds NOTHING', () => {
  eq('join plan without limit => {} (p_params empty — byte-identical to the pre-r139 unclamped wire)', rpcParams(JOIN_PLAN, parseWhEngineRequest(joinReqBody()).query), {});
  eq('join plan with limit 10 => {} (r139 R4 (a): join waves slice POST-merge — the $2 alias is retired for grouped plans)', rpcParams(JOIN_PLAN, parseWhEngineRequest(joinReqBody({ query: { limit: 10 } })).query), {});
  const noJoin = parseWhEngineRequest({ v: 1, qid: 'q', table: 'wh_probe_agg', query: { select: [{ op: 'sum', col: 'amount', alias: 'x' }], groupBy: ['region'] } });
  eqTrue('the join descriptor adds NO param (rpcParams identical with and without query.join)', deepEq(rpcParams(JOIN_PLAN, parseWhEngineRequest(joinReqBody()).query), rpcParams(JOIN_PLAN, noJoin.query)));
  // r139 R4 (a): limit and fetch_rows retire TOGETHER for grouped waves —
  // one K source (effectiveLimitK), one law; the clamp is the post-merge
  // slice (supersedes the r133 emission-shape pin on join plans).
  eq('join plan with fetch_rows 10 => {} (the same retire — the alias law keeps ONE K source)', rpcParams(JOIN_PLAN, parseWhEngineRequest(joinReqBody({ query: { fetch_rows: 10 } })).query), {});
  eq('join plan with fetch_rows null (no limit) => {} (AM-7 holds: never null — and here never present)', rpcParams(JOIN_PLAN, parseWhEngineRequest(joinReqBody({ query: { fetch_rows: null } })).query), {});
});

// =============================================================================
// §6.6 (r133) — fetch_rows arms: parse (OPTIONAL int, mutually exclusive with
// limit, positive-integer-or-null) + the effectiveK chain end-to-end (the
// manifest pre-refusal reads planRef.limitK = effectiveK — M-J4's living
// arm; the boundary == law; and the P2-1 TRUNCATION arm — the rows slice is
// its OWN edit site, a missed threading returns shards×K rows silently).
// =============================================================================
Deno.test('r133 fetch_rows parse: OPTIONAL int, mutually exclusive with limit, positive-integer-or-null (fixed-string malformed, r57 law)', () => {
  const base = { v: 1, qid: 'q-fetch-parse', table: 'wh_probe_agg' };
  const q = (extra: Record<string, unknown>) => ({ ...base, query: { select: [{ op: 'count' }], ...extra } });
  // valid: normalized additively — fetch_rows rides, limit ABSENT.
  const fr = parseWhEngineRequest(q({ fetch_rows: 25 }));
  eq('fetch_rows 25 parses; normalized query carries fetch_rows 25 and NO limit key', [fr.query.fetch_rows, 'limit' in fr.query], [25, false]);
  eqTrue('fetch_rows null is kept (the explicit no-truncation alias of limit:null — effectiveK falls through the ?? law)', parseWhEngineRequest(q({ fetch_rows: null })).query.fetch_rows === null);
  eqTrue('fetch_rows absent: the normalized query carries NO fetch_rows key (byte-identical pre-r133 emit)', !('fetch_rows' in parseWhEngineRequest(q({})).query));
  // mutual exclusion — BOTH keys present is a consumer error, not a precedence question.
  throwsEngine('fetch_rows 25 + limit 10 BOTH present => malformed', () => parseWhEngineRequest(q({ fetch_rows: 25, limit: 10 })), 'malformed', 'query.fetch_rows and query.limit are mutually exclusive');
  throwsEngine('fetch_rows 25 + limit:null BOTH present => malformed (presence, not value, triggers exclusion)', () => parseWhEngineRequest(q({ fetch_rows: 25, limit: null })), 'malformed', 'query.fetch_rows and query.limit are mutually exclusive');
  // malformed variants (the limit parse convention: non-negative integer or null — fetch_rows tightens to POSITIVE).
  throwsEngine('fetch_rows non-integer (2.5) => malformed', () => parseWhEngineRequest(q({ fetch_rows: 2.5 })), 'malformed', 'query.fetch_rows must be a positive integer or null');
  throwsEngine('fetch_rows 0 => malformed (a zero-row fetch is a no-op masquerading as a clamp)', () => parseWhEngineRequest(q({ fetch_rows: 0 })), 'malformed', 'query.fetch_rows must be a positive integer or null');
  throwsEngine('fetch_rows negative (-5) => malformed', () => parseWhEngineRequest(q({ fetch_rows: -5 })), 'malformed', 'query.fetch_rows must be a positive integer or null');
  throwsEngine('fetch_rows string ("25") => malformed', () => parseWhEngineRequest(q({ fetch_rows: '25' })), 'malformed', 'query.fetch_rows must be a positive integer or null');
});

Deno.test('r133 fetch_rows effectiveK chain end-to-end: pre-refusal (M-J4), boundary ==, and the P2-1 truncation arm', async () => {
  // (a) M-J4 living arm: fetch_rows 1001 > manifest max_rows 1000 => the
  // manifest-side pre-refusal fires (it reads planRef.limitK = effectiveK —
  // NO code change needed there, the K source flows) with ZERO shard POSTs.
  // Hand-derived shape (the LETHAL 3 law, wh_handshake_test.ts):
  // warnings {shard, max_rows_exceeded, est_rows 0, retried false} ×2,
  // perShard {ok false, latencyMs 0 EXACTLY, error} ×2, coverage 0/2.
  const overReq = parseWhEngineRequest({ ...joinReqBody(), query: { ...(joinReqBody().query as Record<string, unknown>), fetch_rows: 1001 } });
  const seenOver: SeenCall[] = [];
  const resOver = await executeWhQuery(joinExecArgs({ fetcher: recordingFetch({}, seenOver), templateHashes: [W6H], reqOverride: overReq }));
  eq('fetch_rows 1001 > max_rows 1000: max_rows_exceeded warnings ×2 (est_rows 0 — no directory estimate)', resOver.warnings, [
    { shard: 'shard-a', code: 'max_rows_exceeded', est_rows: 0, retried: false },
    { shard: 'shard-b', code: 'max_rows_exceeded', est_rows: 0, retried: false },
  ]);
  eq('fetch_rows 1001: perShard latencyMs == 0 EXACTLY (no round trip — hand-computed law), fanout dead', resOver.perShard, [
    { shard: 'shard-a', ok: false, latencyMs: 0, error: 'max_rows_exceeded' },
    { shard: 'shard-b', ok: false, latencyMs: 0, error: 'max_rows_exceeded' },
  ]);
  eq('fetch_rows 1001: coverage 0/2 partial and the recording fetcher saw ZERO POSTs (M-J4: the pre-refusal is unconditional over the K source)', [resOver.coverage, resOver.partial, seenOver], ['0/2', true, []]);
  // (b) boundary == passes: fetch_rows 1000 == max_rows 1000 (refuse iff
  // K > max_rows STRICTLY) — the wave fans out and completes.
  const eqReq = parseWhEngineRequest({ ...joinReqBody(), query: { ...(joinReqBody().query as Record<string, unknown>), fetch_rows: 1000 } });
  const seenEq: SeenCall[] = [];
  const resEq = await executeWhQuery(joinExecArgs({ fetcher: recordingFetch({ 'shard-a': envA, 'shard-b': envB }, seenEq), templateHashes: [W6H], reqOverride: eqReq }));
  eq('fetch_rows 1000 == max_rows 1000: the boundary PASSES and both shards are POSTed (2/2)', [resEq.coverage, seenEq.length], ['2/2', 2]);
  // (c) P2-1 TRUNCATION arm (audit A P2-1 — design §6.6): 2 shards × 2
  // regions merge to 2 groups; fetch_rows 1 => response.rows EXACTLY 1.
  // The slice reads effectiveLimitK directly (its OWN edit site) — a missed
  // threading would return shards×K = 2 rows SILENTLY.
  const truncReq = parseWhEngineRequest({ ...joinReqBody(), query: { ...(joinReqBody().query as Record<string, unknown>), fetch_rows: 1 } });
  const resTrunc = await executeWhQuery(joinExecArgs({ fetcher: recordingFetch({ 'shard-a': smallEnvA, 'shard-b': smallEnvB }, []), templateHashes: [W6H], reqOverride: truncReq }));
  eq('fetch_rows 1 on a 2-shard wave merging 2 groups: response.rows is EXACTLY the first row (merged rows ≤ effectiveK)', resTrunc.rows, [{ k: ['g00'], aggs: { x: 140n, c: 4, n: 5 } }]);
  const resUnclamped = await executeWhQuery(joinExecArgs({ fetcher: recordingFetch({ 'shard-a': smallEnvA, 'shard-b': smallEnvB }, []), templateHashes: [W6H] }));
  eqTrue('same wave WITHOUT fetch_rows returns both rows (the clamp, not the merge, did the trimming)', resUnclamped.rows?.length === 2);
});

// =============================================================================
// §6.2 (A4) — the entrypoint :464-478 DISPATCH-POPULATION SPLIT, end-to-end:
// the embed keeps BOTH tables' rows; the dim feeds the gate ONLY; the
// dispatch population stays fact-only; the head stays a FACT row.
// =============================================================================
const EP_TOKEN = 'test-fleet-token';

function withEnv(key: string, value: string | undefined, body: () => Promise<void>): Promise<void> {
  return (async () => {
    const saved = Deno.env.get(key);
    try {
      if (value === undefined) Deno.env.delete(key);
      else Deno.env.set(key, value);
      await body();
    } finally {
      if (saved === undefined) Deno.env.delete(key);
      else Deno.env.set(key, saved);
    }
  })();
}

function epRequest(): Request {
  return new Request('https://ref.supabase.co/functions/v1/warehouse-engine/query', {
    method: 'POST',
    headers: {
      'authorization': `Bearer ${EP_TOKEN}`,
      'apikey': EP_TOKEN,
      'content-type': 'application/json',
    },
    body: JSON.stringify({
      ...joinReqBody(),
      column_types: COLS,
      column_scales: SCALES,
    }),
  });
}

Deno.test('r129 dispatch-population split (entrypoint :464-478): dim rows feed the GATE only — dispatch stays fact-only and the head stays fact', async () => {
  await withEnv('WHE_BEARER_TOKEN', EP_TOKEN, async () => {
    // Embed: 2 fact rows (schema 1) + 2 dim rows (schema_version 9!) + 1
    // dim-ONLY ref. If dim rows ever entered the dispatch population they
    // would survive E11 (NULL/NULL bounds) — shard-d POSTs + phantom 3/3;
    // if a dim row ever displaced the fact head the schema binding would
    // become 9 and EVERY envelope would schema_mismatch.
    const seen: string[] = [];
    const deps: WhEngineDeps = {
      probeDirectoryVersion: async () => 42,
      readDirectory: async () => ({
        version: 42,
        rows: [
          factRow('shard-a'),
          factRow('shard-b'),
          dimRow('shard-a', { isReference: true }),
          dimRow('shard-b', { isReference: true }),
          dimRow('shard-d', { isReference: true }), // dim-only ref — NEVER dispatched
        ],
      }),
      fetcher: ((shard: string) => {
        seen.push(shard);
        return Promise.resolve({ ok: true as const, envelope: shard === 'shard-a' ? smallEnvA : smallEnvB, estRows: 10 });
      }) as WhShardFetcher,
      hasRealFetcher: true,
    };
    const res = await handleWhEngineRequest(epRequest(), deps);
    const j = await res.json();
    eq('status 200 + coverage 2/2 + clean warnings (head stayed the FACT row: the dim\'s schema 9 never became the binding)', [res.status, j.coverage, j.warnings.length], [200, '2/2', 0]);
    eq('dispatch population = FACT ONLY: the dim-only ref (shard-d) was NEVER POSTed', [...seen].sort(), ['shard-a', 'shard-b']);
    eq('merged rows ride the wire with x as fixed-point TEXT (the bigint serializer law)', j.rows, [
      { k: ['g00'], aggs: { x: '140', c: 4, n: 5 } },
      { k: ['g01'], aggs: { x: '110', c: 5, n: 5 } },
    ]);
    eq('the additive ok-arm keys SURVIVE the HTTP wire (partial_rows/partial_bytes on every ok entry)', j.perShard.map((p: { shard: string; ok: boolean; partial_rows: number; partial_bytes: number }) => [p.shard, p.ok, p.partial_rows, p.partial_bytes > 0]), [['shard-a', true, 2, true], ['shard-b', true, 2, true]]);

    // The gate half: the dim rows WERE collected from the SAME embed and
    // consumed — break the dim side (is_reference absent on shard-b) and the
    // whole request 400s BEFORE any POST.
    const seen2: string[] = [];
    const deps2: WhEngineDeps = {
      probeDirectoryVersion: async () => 42,
      readDirectory: async () => ({
        version: 42,
        rows: [
          factRow('shard-a'),
          factRow('shard-b'),
          dimRow('shard-a', { isReference: true }),
          dimRow('shard-b'), // is_reference ABSENT — the embed row is the gate's input
        ],
      }),
      fetcher: ((shard: string) => {
        seen2.push(shard);
        return Promise.resolve({ ok: true as const, envelope: smallEnvA, estRows: 10 });
      }) as WhShardFetcher,
      hasRealFetcher: true,
    };
    const res2 = await handleWhEngineRequest(epRequest(), deps2);
    const j2 = await res2.json();
    eq('dim is_reference absent on one embed row => 400 join_not_colocated (the gate consumed BOTH tables\' rows)', [res2.status, j2.error.code], [400, 'join_not_colocated']);
    eqTrue('the 400 fired BEFORE any shard POST (dispatch never started)', seen2.length === 0);
  });
});

// =============================================================================
// legB R-B3 DECISION PIN — the join gate fires REGARDLESS of rpcMode. A join
// request with rpcMode ABSENT passes gate + binding and fans out SELECT-
// SHAPE targets. Decision (battery leg, r129): PIN the current behavior —
// the surface is NEW (no pre-r129 path exists to preserve; F-N4 stays intact
// for every non-join request), the design is silent, and the D2/rpc plane is
// an additive wiring lever the r130 live ladder arms end-to-end. This block
// is the named pin: changing the behavior (gating joins to rpcMode, or
// widening D2 to rpcMode-absent) must RED here LOUDLY.
// =============================================================================
Deno.test('r129 R-B3 DECISION PIN: the join gate + binding fire identically on BOTH rpc planes — rpcMode-absent fans out select-shape, rpcMode-on rides the wh_query RPC', async () => {
  // Arm 1 — rpcMode ABSENT (the select plane): gate + binding PASS, targets
  // are the plain 2-arg select shape, NO rpc spec, NO D2 (D2 does not exist
  // on this plane for ANY plan — F-N4 intact).
  const seenSel: SeenCall[] = [];
  const resSel = await executeWhQuery(joinExecArgs({
    fetcher: recordingFetch({ 'shard-a': envA, 'shard-b': envB }, seenSel),
  }));
  eq('rpcMode-absent join: gate passed, coverage 2/2 (no plan_untemplated — D2 is rpcMode-scoped)', resSel.coverage, '2/2');
  eq('rpcMode-absent join: targets are SELECT-SHAPE — the rpc spec is NEVER attached and the url never rides /rpc/', seenSel.map((s) => [s.rpc ?? null, s.url.includes('/rpc/')]), [[null, false], [null, false]]);

  // Arm 2 — rpcMode ON (the wh_query plane): the SAME join request rides the
  // §6.1 rpc shape with p_template_hash = W6H and p_params = {} (no new
  // params), the flat §6.2 wire adapts through the MANIFEST row, and the
  // values merge identically (hand-computed: g00 x140/c4/n5, g01 x110/c5/n5).
  const seenRpc: SeenCall[] = [];
  const resRpc = await executeWhQuery(joinExecArgs({
    fetcher: recordingFetch({ 'shard-a': w6Wire(SMALL_A), 'shard-b': w6Wire(SMALL_B) }, seenRpc),
    rpcMode: true,
  }));
  eq('rpcMode-on join: targets carry {p_template_hash: W6H, p_params: {}} (the existing param law — no new params)', seenRpc.map((s) => s.rpc), [{ p_template_hash: W6H, p_params: {} }, { p_template_hash: W6H, p_params: {} }]);
  eq('rpcMode-on join: the flat §6.2 wire adapts through the manifest W6 row and merges to the same values', resRpc.rows, [
    { k: ['g00'], aggs: { x: 140n, c: 4, n: 5 } },
    { k: ['g01'], aggs: { x: 110n, c: 5, n: 5 } },
  ]);
});

// -----------------------------------------------------------------------------
// r138 B1 arm 4 (d3 §3 row 4 + §8 P3-e wall b): the planIsJoin guard. A W6-
// shaped JOIN wave with limitK null and USABLE fact-placement estimates that
// MISMATCH the merged Σn: the inner-join reduction is lawful, so the advisory
// MUST be ABSENT — the join plane's completeness law is EXACT-oracle + the
// per-band pins, never row reconciliation. Hand-computed: merged Σn = 10
// (g00 4+1 + g01 2+3), fact estimates 7+7 = 14 ≠ 10 — the ONLY wall between
// this wave and a false-positive advisory is the join guard
// (wh_engine_core.ts:2315 `args.req.query.join === undefined`).
// KILLER K-ADV-c: removing that guard fires EXACTLY ONE
// {shard:'<merged>', code:'row_estimate_mismatch', est_rows:14, retried:false}
// here ⇒ RED.
// -----------------------------------------------------------------------------
Deno.test('r138 B1 arm 4 (join guard): the W6 join wave NEVER arms the row-estimate advisory — even with usable mismatched estimates and limitK null (K-ADV-c lethal)', async () => {
  const facts = [{ ...factRow('shard-a'), row_estimate: 7 }, { ...factRow('shard-b'), row_estimate: 7 }];
  const seen: SeenCall[] = [];
  const res = await executeWhQuery(joinExecArgs({
    facts,
    fetcher: recordingFetch({ 'shard-a': smallEnvA, 'shard-b': smallEnvB }, seen),
  }));
  eq('B1 arm 4: join guard — advisory ABSENT on the mismatched join wave (Σest 14 ≠ merged Σn 10), warnings EXACTLY []', res.warnings, []);
  eq('B1 arm 4: the join wave still merges honestly (the guard suppresses NOTHING else)', res.rows, [
    { k: ['g00'], aggs: { x: 140n, c: 4, n: 5 } },
    { k: ['g01'], aggs: { x: 110n, c: 5, n: 5 } },
  ]);
  eq('B1 arm 4: the wave is a complete 2/2 (preconditions: placements plane, bare count n present, limitK null, full coverage — only the join wall remains)', [res.coverage, res.partial], ['2/2', false]);
});

// =============================================================================
// §6.7 — r148 G2 post-merge rank-by-agg (design_r147_g2_rankbyagg §3): 12
// arms + unit comparator arms, every expectation HAND-COMPUTED from the
// fixture numbers via an independent exact-rational oracle (Python
// Fraction), never from a run. MUTANT KILL-CITES (commit-before-mutant,
// ONE at a time, RED-proof each):
//   M1 float-quotient comparator  -> REDs arm 6b (+ arm 1's exact pairs)
//   M2 direction flips NULL placement -> REDs the NULL unit arm + arm 8
//   M3 missing tiebreak (sort-stability reliance) -> REDs arm 6c
//   M4 silent parse drop          -> REDs arm 1 (rows stay key-asc)
//   M5 alias-only binding         -> REDs arm 1 (400 instead of ranked rows)
//   M6 missing scalar gate        -> REDs arm 11's scalar arm
//
// ERRATUM (P1 finding, banked r148 — carried to the r149 review): the
// design doc §3 arm-4 top-5 lists [g37,g16,g01,g04,g07] — a hand-
// computation transposition. g16 and g37 BOTH carry c=278 in E16 (the
// fixture), and binding clause 7 (canonical key-ASCENDING among value
// peers) orders g16 < g37. The law-correct order [g16,g37,g01,g04,g07] is
// what this battery pins; the audits' own full-order computation agrees.
// =============================================================================
const G2_AVG_DESC_FULL = ['g13', 'g40', 'g04', 'g49', 'g46', 'g37', 'g19', 'g10', 'g34', 'g01', 'g28', 'g43', 'g16', 'g07', 'g31', 'g25', 'g22'];
const rowByRegion = new Map(E16_W8_FINAL.map((r) => [r.k[0] as string, r]));
const ranked = (order: string[]) => order.map((rg) => rowByRegion.get(rg) as (typeof E16_W8_FINAL)[number]);
const g2arm = (query: Record<string, unknown>) => {
  const base = w8avgReqBody();
  return { ...base, query: { ...(base.query as Record<string, unknown>), ...query } };
};
const rankWave = (query: Record<string, unknown>, seen: SeenCall[]) => executeWhQuery(joinExecArgs({
  reqOverride: parseWhEngineRequest(g2arm(query)),
  facts: [factRow('shard-a'), factRow('shard-b'), factRow('shard-c')],
  dims: [dimRow('shard-a', { isReference: true }), dimRow('shard-b', { isReference: true }), dimRow('shard-c', { isReference: true })],
  fetcher: recordingFetch({ 'shard-a': w8wire(W8_BANDS.A), 'shard-b': w8wire(W8_BANDS.B), 'shard-c': w8wire(W8_BANDS.C) }, seen),
  templateHashes: [W8H],
  rpcMode: true,
}));
const rankWaveEnv = (query: Record<string, unknown>, rows: E15Row[]) => executeWhQuery(joinExecArgs({
  reqOverride: parseWhEngineRequest(g2arm(query)),
  facts: [factRow('shard-a')],
  dims: [dimRow('shard-a', { isReference: true })],
  fetcher: recordingFetch({ 'shard-a': w8env('shard-a', rows) }, []),
  templateHashes: [W8H],
}));
// NULL-sink variant: c=0 rows carry the avg pair {s:null, c:0} — the
// s:null <=> c:0 PAIRING LAW (a {s:'0', c:0} partial is envelope_invalid:
// the accumulator identity requires s null exactly when c is 0).
const rankWaveEnvNull = (query: Record<string, unknown>, rows: E15Row[]) => {
  const base = w8env('shard-a', rows);
  const env: WhPartialEnvelope = {
    ...base,
    partial: {
      ...base.partial,
      rows: rows.map((r) => ({
        k: [r.region],
        a: { 'avg(amount)': r.c === 0 ? { s: null, c: 0 } : { s: String(r.x), c: r.c }, c: r.c, n: r.n },
      })),
    },
  };
  return executeWhQuery(joinExecArgs({
    reqOverride: parseWhEngineRequest(g2arm(query)),
    facts: [factRow('shard-a')],
    dims: [dimRow('shard-a', { isReference: true })],
    fetcher: recordingFetch({ 'shard-a': env }, []),
    templateHashes: [W8H],
  }));
};
// plan-time gates live in buildMergePlan (the first statement of
// executeWhQuery — pre-network); direct-call it the way
// wh_handshake_test.ts ARM_A_PLAN does.
const planOf = (body: Record<string, unknown>) => buildMergePlan(parseWhEngineRequest(body), { columnTypes: COLS, columnScales: SCALES });

Deno.test('r148 G2 arm 1 (the discriminator): W8 + rank_by {agg:"avg(amount)"} desc, K=5 => rows = [g13,g40,g04,g49,g46] EXACT rationals — ZERO of 5 positions match the key-asc first-5; rpc specs stay p_params:{} ×3 (rank is POST-merge, the shard plane never sees rank_by); partial_rows still 17×3', async () => {
  const seen: SeenCall[] = [];
  const res = await rankWave({ limit: 5, rank_by: { agg: 'avg(amount)' } }, seen);
  eq('rows = top-5 by EXACT rational avg desc (the banked order — hand-computed, zero ties)', res.rows, ranked(G2_AVG_DESC_FULL.slice(0, 5)));
  eq('ZERO overlap check: the rank top-5 shares NO position with the key-asc first-5 (maximally discriminative)', res.rows?.map((r) => r.k[0]), ['g13', 'g40', 'g04', 'g49', 'g46']);
  eq('rpc specs byte-pin WITH rank_by armed: {p_template_hash: W8H, p_params: {}} ×3 (the r139 grouped law holds — rank_by rides NO param)', seen.map((s) => s.rpc), [
    { p_template_hash: W8H, p_params: {} },
    { p_template_hash: W8H, p_params: {} },
    { p_template_hash: W8H, p_params: {} },
  ]);
  eq('perShard partial_rows still 17×3 (unsharded echo)', res.perShard.map((p) => [p.ok, p.partial_rows, (p.partial_bytes ?? 0) > 0]), [[true, 17, true], [true, 17, true], [true, 17, true]]);
  eq('clamp + rank did not disturb the wave: coverage 3/3, partial false, warnings []', [res.coverage, res.partial, res.warnings], ['3/3', false, []]);
});

Deno.test('r148 G2 arm 2 (disarm byte-identity): the SAME wave with rank_by ABSENT => byte-identical to the r144 K=5 key-asc pin (binding clause 1)', async () => {
  const seen: SeenCall[] = [];
  const res = await rankWave({ limit: 5 }, seen);
  eq('rows = the FIRST 5 of the 17 canonical key-asc (the pre-r148 law — g01,g04,g07,g10,g13)', res.rows, E16_W8_FINAL.slice(0, 5));
  eq('rpc specs identical to the armed twin ({} ×3 — only the ROW ORDER differs)', seen.map((s) => s.rpc), [
    { p_template_hash: W8H, p_params: {} },
    { p_template_hash: W8H, p_params: {} },
    { p_template_hash: W8H, p_params: {} },
  ]);
});

Deno.test('r148 G2 arm 3 (asc twin): direction:"asc" => EXACT value-reverse of desc over the 17 (no ties, no NULLs on E16); K absent => all 17 ranked', async () => {
  const resDesc = await rankWaveEnv({ rank_by: { agg: 'avg(amount)', direction: 'desc' } }, E16.rows);
  const resAsc = await rankWaveEnv({ rank_by: { agg: 'avg(amount)', direction: 'asc' } }, E16.rows);
  eq('desc full order == the banked 17-order', resDesc.rows?.map((r) => r.k[0]), G2_AVG_DESC_FULL);
  eq('asc full order == EXACT reverse (values flip, nothing else)', resAsc.rows?.map((r) => r.k[0]), [...G2_AVG_DESC_FULL].reverse());
});

Deno.test('r148 G2 arm 4 (natural tie): rank_by {agg:"c"} desc, K=5 => [g16,g37,g01,g04,g07] — the 278 pair in key-asc order (g16<g37), then the 15-way 277 tie key-asc. ERRATUM: the design doc lists [g37,g16,...] — see the §6.7 header (clause-7 law wins; independent oracle)', async () => {
  const res = await rankWaveEnv({ limit: 5, rank_by: { agg: 'c' } }, E16.rows);
  eq('top-5 by count_col desc with key-asc ties', res.rows, ranked(['g16', 'g37', 'g01', 'g04', 'g07']));
  eq('the tie values themselves: c = [278, 278, 277, 277, 277]', res.rows?.map((r) => r.aggs.c), [278, 278, 277, 277, 277]);
});

Deno.test('r148 G2 arm 5 (all-tie degenerate): rank_by {agg:"n"} — count(*)=280 ∀17 => rank is the IDENTITY permutation; output byte-identical to key-asc in BOTH directions (direction is inert on an all-tie)', async () => {
  const resDesc = await rankWaveEnv({ limit: 5, rank_by: { agg: 'n' } }, E16.rows);
  const resAsc = await rankWaveEnv({ limit: 5, rank_by: { agg: 'n', direction: 'asc' } }, E16.rows);
  eq('desc == key-asc first-5', resDesc.rows, E16_W8_FINAL.slice(0, 5));
  eq('asc == the SAME first-5 (direction cannot matter when every value ties)', resAsc.rows, E16_W8_FINAL.slice(0, 5));
});

Deno.test('r148 G2 arm 9 (K>groups passthrough): limit 25 over 17 groups => ALL 17 rows in the full rank order (slice clamps, rank survives)', async () => {
  const res = await rankWaveEnv({ limit: 25, rank_by: { agg: 'avg(amount)' } }, E16.rows);
  eq('all 17 rows, full banked rank order', res.rows?.map((r) => r.k[0]), G2_AVG_DESC_FULL);
  eq('and the row BODIES are byte-identical to the E16 projection', res.rows, ranked(G2_AVG_DESC_FULL));
});

Deno.test('r148 G2 arm 10 (K=0 + zero rows): rank_by + limit:0 => []; rank_by + fetch_rows:0 => the SAME 400 (the positive-int law is NOT relaxed for rank requests); rank over a 0-row partial wave => []', async () => {
  const res0 = await rankWaveEnv({ limit: 0, rank_by: { agg: 'avg(amount)' } }, E16.rows);
  eq('limit:0 + rank => [] (slice(0,0) — the :711 law holds armed)', res0.rows, []);
  throwsEngine('fetch_rows:0 + rank => malformed (no-op masquerading as a clamp — unchanged)', () => parseWhEngineRequest(g2arm({ fetch_rows: 0, rank_by: { agg: 'avg(amount)' } })), 'malformed', 'query.fetch_rows must be a positive integer or null');
  const resEmpty = await rankWaveEnv({ rank_by: { agg: 'avg(amount)' } }, []);
  eq('rank over an empty (0-row) grouped wave => [] (no special case)', resEmpty.rows, []);
});

Deno.test('r148 G2 arm 11 (rejects + order-of-rejects pins): scalar gate, membership, {op,col} form, unknown keys, direction LAST, null-lenient-absent, min/max scope — every message the r57 fixed-string law', () => {
  // scalar gate (binding clause 3 — PLAN time, buildMergePlan; parse accepts the shape)
  const scalarBody = { v: 1, qid: 'g2-scalar', table: 'wh_probe_agg', query: { select: [{ op: 'count' }], rank_by: { agg: 'count(*)' } } };
  throwsEngine('scalar wave + rank_by => 400 (a silent no-op directive is the lying-parser class; M6 kill-site)', () => planOf(scalarBody), 'malformed', 'query.rank_by requires a grouped query');
  // membership (grouped, unknown name — PLAN time)
  throwsEngine('grouped + rank_by.agg unknown => 400 (NAME-membership binding)', () => planOf(g2arm({ rank_by: { agg: 'nope(amount)' } })), 'malformed', 'query.rank_by.agg does not name an aggregate in the plan (rank binds by the aggregate NAME the envelope carries)');
  // {op,col} reference form REJECTED at parse (agg must be a STRING — no second matching path)
  throwsEngine('rank_by.agg as {op,col} object => 400 (the reference form is NOT admitted — col-strict r130)', () => parseWhEngineRequest(g2arm({ rank_by: { agg: { op: 'avg', col: 'amount' } } })), 'malformed', 'query.rank_by.agg must be a non-empty string naming a plan aggregate');
  // shape rejects (parse)
  throwsEngine('rank_by as a bare string => 400 (must be an object)', () => parseWhEngineRequest(g2arm({ rank_by: 'c' })), 'malformed', 'query.rank_by must be an object');
  throwsEngine('rank_by with an unknown key ("order") => 400 (tighter-than-outer, the join precedent)', () => parseWhEngineRequest(g2arm({ rank_by: { agg: 'c', order: 'desc' } })), 'malformed', 'query.rank_by carries unknown keys (only agg|direction are allowed — rank directives are strictly validated)');
  throwsEngine('rank_by.direction not the enum => 400 (checked LAST — the variant-last precedent)', () => parseWhEngineRequest(g2arm({ rank_by: { agg: 'c', direction: 'sideways' } })), 'malformed', 'query.rank_by.direction must be "asc" or "desc"');
  throwsEngine('rank_by.agg empty string => 400', () => parseWhEngineRequest(g2arm({ rank_by: { agg: '' } })), 'malformed', 'query.rank_by.agg must be a non-empty string naming a plan aggregate');
  // ORDER pins: unknown-keys fire BEFORE agg; agg fires BEFORE direction
  throwsEngine('order pin A: {agg:123, direction:"sideways", order:"x"} => unknown-keys FIRST', () => parseWhEngineRequest(g2arm({ rank_by: { agg: 123, direction: 'sideways', order: 'x' } })), 'malformed', 'query.rank_by carries unknown keys');
  throwsEngine('order pin B: {agg:123, direction:"sideways"} => agg BEFORE direction (direction is LAST)', () => parseWhEngineRequest(g2arm({ rank_by: { agg: 123, direction: 'sideways' } })), 'malformed', 'query.rank_by.agg must be a non-empty string naming a plan aggregate');
  // null-lenient-absent: rank_by:null IS the absent state
  const nulled = parseWhEngineRequest(g2arm({ rank_by: null }));
  eqTrue('rank_by:null parses ABSENT (no rank_by key in the normalized query — the where/read_plane precedent)', !('rank_by' in nulled.query));
  // normalized shape
  const shaped = parseWhEngineRequest(g2arm({ rank_by: { agg: 'c', direction: 'asc' } }));
  eq('normalized rank_by carries EXACTLY {agg, direction} (no echo of unknowns, no extras)', shaped.query.rank_by, { agg: 'c', direction: 'asc' });
  const minimal = parseWhEngineRequest(g2arm({ rank_by: { agg: 'c' } }));
  eq('normalized rank_by without direction carries EXACTLY {agg} (default desc is a COMPARATOR default, never materialized)', minimal.query.rank_by, { agg: 'c' });
  // 11b (impl-added, clause-5 scope): min/max names are OUT of the v1 rank scope (PLAN time)
  const minBody = { v: 1, qid: 'g2-scope', table: 'wh_probe_agg', query: { select: [{ op: 'min', col: 'amount' }], groupBy: ['region'], rank_by: { agg: 'min(amount)' } } };
  throwsEngine('rank_by naming a min aggregate => 400 (clause-5 scope: v1 ranks avg|count|sum; min/max is a free extension NOT yet admitted)', () => planOf(minBody), 'malformed', 'query.rank_by.agg names a min/max aggregate — outside the v1 rank scope (avg|count|sum only)');
  // ORDER of the plan-time gates: scalar gate BEFORE membership (a scalar request with a garbage agg name reports the scalar violation)
  const scalarBadName = { v: 1, qid: 'g2-order', table: 'wh_probe_agg', query: { select: [{ op: 'count' }], rank_by: { agg: 'nope' } } };
  throwsEngine('plan-gate order: scalar+unknown-agg => the SCALAR message fires first', () => planOf(scalarBadName), 'malformed', 'query.rank_by requires a grouped query');
});

Deno.test('r148 G2 arm 12 (1001-band byte-pin): the pre-POST max_rows_exceeded refusal is rank-AGNOSTIC (reads planRef.limitK only) — armed request refuses byte-identically with ZERO shard POSTs; the 1000 boundary passes and ranks', async () => {
  const seen: SeenCall[] = [];
  const res = await rankWave({ fetch_rows: 1001, rank_by: { agg: 'avg(amount)' } }, seen);
  eq('refusal warnings ×3 (max_rows_exceeded, est_rows 0, retried false — the r133 shape, rank-agnostic)', res.warnings.map((w) => [w.shard, w.code, w.est_rows, w.retried]), [['shard-a', 'max_rows_exceeded', 0, false], ['shard-b', 'max_rows_exceeded', 0, false], ['shard-c', 'max_rows_exceeded', 0, false]]);
  eq('ZERO POSTs (pre-POST refusal — the fetcher was never called)', seen.length, 0);
  eq('perShard all not-ok, coverage 0/3', [res.perShard.map((p) => p.ok), res.coverage], [[false, false, false], '0/3']);
  const seenOk: SeenCall[] = [];
  const resOk = await rankWave({ fetch_rows: 1000, rank_by: { agg: 'avg(amount)' } }, seenOk);
  eq('boundary PASS: fetch_rows:1000 + rank => all 17 rows, still in RANK order (K=1000 clamps nothing)', resOk.rows?.map((r) => r.k[0]), G2_AVG_DESC_FULL);
});

Deno.test('r148 G2 arm 8 (E2E NULL sink): synthetic c=0 groups (avg pair {s:null,c:0} — the pairing law) finalize avg NULL — ranked LAST in BOTH directions, NEVER excluded; two NULLs are peers (key-asc among themselves)', async () => {
  const wave = [...E16.rows, { region: 'gz', x: 0, c: 0, n: 280 }, { region: 'gz2', x: 0, c: 0, n: 280 }];
  const resDesc = await rankWaveEnvNull({ rank_by: { agg: 'avg(amount)' } }, wave);
  eq('desc: 19 rows, top = g13 (the banked order holds above the NULLs)', resDesc.rows?.length, 19);
  eq('desc: first 17 == the banked rank order (NULLs never displace values)', resDesc.rows?.slice(0, 17).map((r) => r.k[0]), G2_AVG_DESC_FULL);
  eq('desc: LAST two = the NULL pair in key-asc [gz, gz2]', resDesc.rows?.slice(17).map((r) => r.k[0]), ['gz', 'gz2']);
  eq('desc: the NULL rows carry avg null (in rows, never excluded)', resDesc.rows?.slice(17).map((r) => r.aggs['avg(amount)']), [null, null]);
  const resAsc = await rankWaveEnvNull({ rank_by: { agg: 'avg(amount)', direction: 'asc' } }, wave);
  eq('asc: FIRST = g22 (smallest avg — the value law flips), LAST two STILL [gz, gz2] (placement FIXED)', [
    resAsc.rows?.[0]?.k[0],
    resAsc.rows?.slice(17).map((r) => r.k[0]),
  ], ['g22', ['gz', 'gz2']]);
});

Deno.test('r148 G2 acceptance (zero registry/derivation/manifest surface): derivation is rank-BLIND — the armed request derives [W8H] exactly like the unarmed twin; rpcParams grouped {}; manifest row intact', () => {
  const armed = parseWhEngineRequest(g2arm({ rank_by: { agg: 'avg(amount)' } }));
  const unarmed = parseWhEngineRequest(w8avgReqBody());
  eq('deriveTemplateHashes(plan view) == [W8H] for BOTH (rank_by is NOT a derivation input — DerivePlanView untouched)', [
    deriveTemplateHashes(entrypointPlanView(armed) as never, 1),
    deriveTemplateHashes(entrypointPlanView(unarmed) as never, 1),
  ], [[W8H], [W8H]]);
  eq('rpcParams(W8 plan, armed query) == {} (byte-identical to the unarmed law)', rpcParams(W8_PLAN, armed.query), {});
  eqTrue('manifest row by W8H still present (zero registry change)', manifestRowByHash(W8H) !== null);
  eqTrue('the armed and unarmed plan VIEWS are deep-equal (table+groupKeys+join+aggs — the :1008 shape)', deepEq(entrypointPlanView(armed), entrypointPlanView(unarmed)));
});

Deno.test('r148 G2 arm 6 (unit, comparator law): equal fractions tie => key-asc peers BOTH directions; float64-colliding distinct rationals order EXACTLY (the M1 float-mutant killer)', () => {
  const rk = (rows: WhGroupFinal[], agg: string, dir?: 'asc' | 'desc') => [...rows].sort(rankComparator(dir ? { agg, direction: dir } : { agg }, [{ col: 'region', type: 'text' }]));
  // 6a: 100/3 vs 200/6 — equal EXACTLY (100*6 == 200*3 == 600) => tie => key-asc
  const eqf: WhGroupFinal[] = [
    { k: ['gb'], aggs: { 'avg(amount)': { num: 200n, den: 6n } } },
    { k: ['ga'], aggs: { 'avg(amount)': { num: 100n, den: 3n } } },
  ];
  eq('equal fractions, input reversed => key-asc [ga, gb] in BOTH directions (a float quotient ALSO ties here — 6a pins the tie law, 6b pins exactness)', [
    rk(eqf, 'avg(amount)', 'desc').map((r) => r.k[0]),
    rk(eqf, 'avg(amount)', 'asc').map((r) => r.k[0]),
  ], [['ga', 'gb'], ['ga', 'gb']]);
  // 6b: (2^53+1)/2^53 vs 1/1 — Number(2^53+1) rounds to 2^53 so BOTH float
  // quotients are 1.0 (COLLIDE), but the exact cross-multiply differs.
  const coll: WhGroupFinal[] = [
    { k: ['ga'], aggs: { 'avg(amount)': { num: 1n, den: 1n } } },
    { k: ['gb'], aggs: { 'avg(amount)': { num: 9007199254740993n, den: 9007199254740992n } } },
  ];
  eq('float-colliding rationals: exact desc = [gb, ga] (a float mutant would TIE => key-asc [ga, gb] — the registered mutant order, M1 kill-site)', rk(coll, 'avg(amount)', 'desc').map((r) => r.k[0]), ['gb', 'ga']);
  eq('float-colliding rationals: exact asc = [ga, gb]', rk(coll, 'avg(amount)', 'asc').map((r) => r.k[0]), ['ga', 'gb']);
});

Deno.test('r148 G2 arm 6c (unit, tie law is EXPLICIT): reverse-shuffled input with value ties STILL ranks key-asc — the M3 stability-reliant mutant REDs here (it would echo the reversed order)', () => {
  const tied: WhGroupFinal[] = [
    { k: ['g05'], aggs: { c: 277 } },
    { k: ['g03'], aggs: { c: 277 } },
    { k: ['g04'], aggs: { c: 277 } },
  ];
  const out = [...tied].sort(rankComparator({ agg: 'c', direction: 'desc' }, [{ col: 'region', type: 'text' }]));
  eq('ties under desc, input already reversed => STILL key-asc [g03, g04, g05]', out.map((r) => r.k[0]), ['g03', 'g04', 'g05']);
});

Deno.test('r148 G2 arm 7 (unit, NULL law): NULLs LAST FIXED in BOTH directions (the M2 mutant kill-site); two NULLs are peers; NULL ranks after ANY value', () => {
  const rk = (rows: WhGroupFinal[], dir: 'asc' | 'desc') => [...rows].sort(rankComparator({ agg: 'avg(amount)', direction: dir }, [{ col: 'region', type: 'text' }]));
  const rows: WhGroupFinal[] = [
    { k: ['ga'], aggs: { 'avg(amount)': null } },
    { k: ['gb'], aggs: { 'avg(amount)': { num: 5n, den: 2n } } },
    { k: ['gc'], aggs: { 'avg(amount)': null } },
    { k: ['gd'], aggs: { 'avg(amount)': { num: 7n, den: 2n } } },
  ];
  eq('desc: values desc [gd(3.5), gb(2.5)] then NULLs last key-asc [ga, gc]', rk(rows, 'desc').map((r) => r.k[0]), ['gd', 'gb', 'ga', 'gc']);
  eq('asc: values asc [gb, gd] then NULLs STILL last [ga, gc] (placement NEVER flips)', rk(rows, 'asc').map((r) => r.k[0]), ['gb', 'gd', 'ga', 'gc']);
});

Deno.test('r148 G2 (unit, sum law): scaled BigInt finals compare exactly (ONE colPlan scale per agg — same-scale monotone; the wire s-TEXT law is bypassed, canonicalization already happened at ingestion); sum NULL (empty-input, E14) ranks LAST both directions', () => {
  const rk = (rows: WhGroupFinal[], dir: 'asc' | 'desc') => [...rows].sort(rankComparator({ agg: 'x', direction: dir }, [{ col: 'region', type: 'text' }]));
  const rows: WhGroupFinal[] = [
    { k: ['ga'], aggs: { x: 1000n } },
    { k: ['gb'], aggs: { x: null } },
    { k: ['gc'], aggs: { x: 999n } },
  ];
  eq('sum desc: [ga(1000), gc(999), gb(null) last]', rk(rows, 'desc').map((r) => r.k[0]), ['ga', 'gc', 'gb']);
  eq('sum asc: [gc(999), ga(1000), gb(null) STILL last]', rk(rows, 'asc').map((r) => r.k[0]), ['gc', 'ga', 'gb']);
});

Deno.test('r148 G2 (unit, fail-closed depth): a min/max STRING final reaching the comparator throws TypeError (the binding scope gate makes it unreachable E2E — defense in depth, never silently wrong)', () => {
  const rows: WhGroupFinal[] = [
    { k: ['ga'], aggs: { 'max(amount)': 'abc' } },
    { k: ['gb'], aggs: { 'max(amount)': 'abd' } },
  ];
  let threw = '';
  try {
    [...rows].sort(rankComparator({ agg: 'max(amount)' }, [{ col: 'region', type: 'text' }]));
  } catch (err) {
    threw = err instanceof TypeError ? 'TypeError' : String(err);
  }
  eq('min/max string final => TypeError (fail-closed loud)', threw, 'TypeError');
});

// -----------------------------------------------------------------------------
// Harness report (hand-rolled runner, no external deps).
// -----------------------------------------------------------------------------
Deno.test('__report__', () => {
  console.log(`\nwh_join_test: ${passed} assertions passed, ${failed} failed`);
  if (failed > 0) throw new Error(`${failed} assertion(s) failed`);
});
