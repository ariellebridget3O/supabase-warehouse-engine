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
//
// Offline + pure: every transport is an injected recording fake — zero
// sockets, no --allow-net (the battery never grants it; a real fetch would
// throw PermissionDenied and fail closed). The only disk reads are the E15
// fixture + the W6 template body (the --allow-read the battery grants).
// =============================================================================

import {
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
import { finalizeGroups, mergeGroupedPartials } from './wh_merge.ts';
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
  eq('append-only + partition laws (r133: W6 index 5 unchanged, W7 index 6, TWO join-carrying rows, SEVEN rows)', [ENGINE_TEMPLATE_MANIFEST[5] === w6, ENGINE_TEMPLATE_MANIFEST[6] === w7, ENGINE_TEMPLATE_MANIFEST.filter((r) => r.join !== undefined).length, ENGINE_TEMPLATE_MANIFEST.length], [true, true, 2, 7]);
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
// §6.6 — rpcParams arms: {} + optional limit; the join descriptor adds NO
// new params (span params stay DEFERRED — design §2.3).
// =============================================================================
Deno.test('r129 rpcParams: join plans ride the EXISTING param law — {} + optional limit, NOTHING new', () => {
  eq('join plan without limit => {} (p_params empty)', rpcParams(JOIN_PLAN, parseWhEngineRequest(joinReqBody()).query), {});
  eq('join plan with limit 10 => {limit:10} (the existing $2 alias)', rpcParams(JOIN_PLAN, parseWhEngineRequest(joinReqBody({ query: { limit: 10 } })).query), { limit: 10 });
  const noJoin = parseWhEngineRequest({ v: 1, qid: 'q', table: 'wh_probe_agg', query: { select: [{ op: 'sum', col: 'amount', alias: 'x' }], groupBy: ['region'] } });
  eqTrue('the join descriptor adds NO param (rpcParams identical with and without query.join)', deepEq(rpcParams(JOIN_PLAN, parseWhEngineRequest(joinReqBody()).query), rpcParams(JOIN_PLAN, noJoin.query)));
  // r133 (design_r132_w7_family §3): fetch_rows rides $2 IDENTICALLY — the
  // emission shape {limit: N} is UNCHANGED, the N source is effectiveK.
  eq('join plan with fetch_rows 10 => {limit:10} (the same $2 alias — emission shape unchanged)', rpcParams(JOIN_PLAN, parseWhEngineRequest(joinReqBody({ query: { fetch_rows: 10 } })).query), { limit: 10 });
  eq('join plan with fetch_rows null (no limit) => {} (the ?? law falls through — p_params.limit is NEVER null, AM-7 holds)', rpcParams(JOIN_PLAN, parseWhEngineRequest(joinReqBody({ query: { fetch_rows: null } })).query), {});
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
// Harness report (hand-rolled runner, no external deps).
// -----------------------------------------------------------------------------
Deno.test('__report__', () => {
  console.log(`\nwh_join_test: ${passed} assertions passed, ${failed} failed`);
  if (failed > 0) throw new Error(`${failed} assertion(s) failed`);
});
