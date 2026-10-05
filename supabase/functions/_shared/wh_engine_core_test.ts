// =============================================================================
// _shared/wh_engine_core_test.ts — RED-first proofs for the ENGINE CORE.
// =============================================================================
// Normative source: research/findings_wh_catalog_contract.md §4 (request
// envelope §4.3, response/error envelopes §4.4, limits §4.5) + research/
// findings_wh_scatter_gather.md §1.2 #4 (planner honesty), §4.1 (pipeline:
// compile → WINDOW fan-out → allSettled merge), §5.1 (failure semantics +
// stamped-header classification), §8 E11/E14.
//
// RED-first: written BEFORE wh_engine_core.ts exists and observed failing
// (module-not-found). Offline + pure: fetcher/timers/directory are injected
// fakes — zero net/fs/env.
// =============================================================================

import {
  DEFAULT_SHARD_TIMEOUT_MS,
  DEFAULT_WINDOW,
  MAX_GROUPS,
  MAX_WINDOW,
  SHARD_REQUEST_HEADERS,
  WhEngineError,
  buildMergePlan,
  classifyFetchFailure,
  compileShardUrl,
  defaultWhEngineTimers,
  executeWhQuery,
  parseWhEngineRequest,
  runFanout,
  selectShards,
} from './wh_engine_core.ts';
import type {
  WhDirectoryRow,
  WhEngineRequest,
  WhEngineTimers,
  WhShardFetcher,
} from './wh_engine_core.ts';
import type { WhPartialEnvelope } from './wh_types.ts';
import { deepEq, show } from './wh_testutil.ts';

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

function throwsTypeError(name: string, fn: () => unknown): void {
  try {
    fn();
    failed++;
    console.error(`FAIL  ${name} — expected TypeError, got no throw`);
  } catch (err) {
    if (err instanceof TypeError) {
      passed++;
      console.log(`  ok  ${name} (TypeError)`);
    } else {
      failed++;
      console.error(`FAIL  ${name} — wrong throw: ${err}`);
    }
  }
}

// -----------------------------------------------------------------------------
// Fixtures — hand-built envelopes (full control, incl. lying envelopes).
// -----------------------------------------------------------------------------

const COLS: Record<string, string> = {
  region: 'text', amount: 'numeric', qty: 'int8', price: 'numeric',
  created_at: 'timestamptz', id: 'int8', note: 'text',
};
const SCALES: Record<string, number> = { amount: 0, price: 2 };

function env(
  shard: string,
  table: string,
  kind: 'grouped' | 'scalar',
  groupKeys: string[] | undefined,
  aggs: Record<string, { op: string; col?: string }>,
  rows: { k: unknown[]; a: Record<string, unknown> }[],
  schemaVersion = 3,
): WhPartialEnvelope {
  return {
    v: 1,
    shard,
    table,
    schema_version: schemaVersion,
    partial: {
      kind,
      ...(groupKeys ? { groupKeys } : {}),
      aggs: aggs as WhPartialEnvelope['partial']['aggs'],
      rows,
      rowCount: rows.length,
      more: false,
    },
  };
}

const E1_AGGS = { s: { op: 'sum', col: 'amount' }, c: { op: 'count' } };
function e1(shard: string, rows: { k: string[]; s: string; c: number }[]): WhPartialEnvelope {
  return env(shard, 'orders', 'grouped', ['region'], E1_AGGS, rows.map((r) => ({ k: r.k, a: { s: r.s, c: r.c } })));
}

function scalarEnv(shard: string, table: string, a: Record<string, unknown>, schemaVersion = 3): WhPartialEnvelope {
  return env(shard, table, 'scalar', undefined, { s: { op: 'sum', col: 'amount' }, c: { op: 'count' } }, [{ k: [], a }], schemaVersion);
}

function dirRow(shard: string, keyMin: string | null, keyMax: string | null, hashSlot: number | null = null): WhDirectoryRow {
  return {
    shard,
    key_min: keyMin,
    key_max: keyMax,
    hash_slot: hashSlot,
    state: 'serving',
    platform_status: 'ACTIVE_HEALTHY',
    schema_version: 3,
    last_health_at: '2026-09-28T00:00:00.000000Z',
  };
}

/** The E11 range fleet: S1 [Jan-Feb), S2 [Feb-Apr), S3 [Apr-Jun). */
const RANGE_FLEET = [
  dirRow('S1', '2026-01-01', '2026-02-01'),
  dirRow('S2', '2026-02-01', '2026-04-01'),
  dirRow('S3', '2026-04-01', '2026-06-01'),
];

function okFetch(envelope: WhPartialEnvelope, estRows?: number): WhShardFetcher {
  return async () => ({ ok: true, envelope, ...(estRows === undefined ? {} : { estRows }) });
}

function failFetch(
  warning: { code?: string; httpStatus?: number; stamped?: boolean },
  estRows?: number,
): WhShardFetcher {
  return async () => ({ ok: false, warning, ...(estRows === undefined ? {} : { estRows }) });
}

/** Counter clock + never-firing injected timeouts (dispose no-op). */
function neverTimeoutTimers(): WhEngineTimers {
  let n = 100;
  return {
    nowMs: () => ++n,
    startTimeout: (_ms: number) => {
      const p = new Promise<'timeout'>(() => {});
      return { promise: p, dispose: () => {} };
    },
  };
}

/** Already-fired injected timeouts — every shard times out deterministically. */
function firedTimeoutTimers(): WhEngineTimers {
  let n = 100;
  return {
    nowMs: () => ++n,
    startTimeout: (_ms: number) => ({ promise: Promise.resolve('timeout' as const), dispose: () => {} }),
  };
}

function baseReq(overrides: Partial<WhEngineRequest> = {}): Record<string, unknown> {
  return {
    v: 1,
    qid: '01J9Q1ZZZZZZZZZZZZZZZZZZZZ',
    table: 'orders',
    query: {
      select: [{ op: 'sum', col: 'amount', alias: 's' }, { op: 'count', alias: 'c' }],
    },
    ...overrides,
  };
}

const E1_EXECUTE = {
  req: parseWhEngineRequest(baseReq({ query: { select: [{ op: 'sum', col: 'amount', alias: 's' }, { op: 'count', alias: 'c' }], groupBy: ['region'] } })),
  columnTypes: COLS as Record<string, never>,
  columnScales: SCALES,
  directoryRows: [dirRow('S1', null, null), dirRow('S2', null, null), dirRow('S3', null, null)],
  shardKeyColumn: 'created_at',
  shardKeyType: 'range' as const,
  directoryVersion: 42,
  timers: neverTimeoutTimers(),
};

// -----------------------------------------------------------------------------
// Request parse pins (§4.3).
// -----------------------------------------------------------------------------
Deno.test('parse: valid minimal request normalizes (defaults filled, echoes kept)', () => {
  const raw = baseReq();
  const req = parseWhEngineRequest(raw);
  eq('normalized request', req, {
    v: 1,
    qid: '01J9Q1ZZZZZZZZZZZZZZZZZZZZ',
    table: 'orders',
    query: { select: [{ op: 'sum', col: 'amount', alias: 's' }, { op: 'count', alias: 'c' }] },
    coverage_mode: 'best_effort',
  });
});

Deno.test('parse: full-shape request keeps where/groupBy/limit/snapshot; neutral clauses dropped', () => {
  const req = parseWhEngineRequest(baseReq({
    query: {
      select: [{ op: 'count', alias: 'c' }],
      where: [{ col: 'created_at', op: 'gte', value: '2026-01-01' }],
      groupBy: ['region'],
      having: null,
      orderBy: null,
      limit: 10,
      offset: 0,
      distinct: false,
    },
    coverage_mode: 'fail_fast',
    directory_snapshot: 'c25sZXNz',
  }));
  eq('normalized full request', req, {
    v: 1,
    qid: '01J9Q1ZZZZZZZZZZZZZZZZZZZZ',
    table: 'orders',
    query: {
      select: [{ op: 'count', alias: 'c' }],
      where: [{ col: 'created_at', op: 'gte', value: '2026-01-01' }],
      groupBy: ['region'],
      limit: 10,
    },
    coverage_mode: 'fail_fast',
    directory_snapshot: 'c25sZXNz',
  });
  eq('snapshot echoed byte-exact (opaque, engine never rewrites)', req.directory_snapshot, 'c25sZXNz');
});

Deno.test('parse: v!==1 => malformed (never best-effort parse an unknown version)', () => {
  throwsEngine('v:2', () => parseWhEngineRequest(baseReq({ v: 2 })), 'malformed');
  throwsEngine('v missing', () => parseWhEngineRequest({ qid: 'x', table: 't', query: { select: [{ op: 'count', alias: 'c' }] } }), 'malformed');
  throwsEngine('v:"1"', () => parseWhEngineRequest(baseReq({ v: '1' })), 'malformed');
});

Deno.test('parse: missing/empty/wrong-type qid => malformed', () => {
  throwsEngine('no qid', () => parseWhEngineRequest(baseReq({ qid: undefined })), 'malformed');
  throwsEngine('empty qid', () => parseWhEngineRequest(baseReq({ qid: '' })), 'malformed');
  throwsEngine('numeric qid', () => parseWhEngineRequest(baseReq({ qid: 7 })), 'malformed');
});

Deno.test('parse: missing/bad table => malformed', () => {
  throwsEngine('no table', () => parseWhEngineRequest(baseReq({ table: undefined })), 'malformed');
  throwsEngine('table with quote (injection surface)', () => parseWhEngineRequest(baseReq({ table: 'orders; drop' })), 'malformed');
  throwsEngine('table not an identifier', () => parseWhEngineRequest(baseReq({ table: '9orders' })), 'malformed');
});

Deno.test('parse: bad select shapes => malformed (bad op pinned)', () => {
  throwsEngine('bad op (pin)', () => parseWhEngineRequest(baseReq({ query: { select: [{ op: 'median', col: 'amount', alias: 'm' }] } })), 'malformed');
  throwsEngine('sum without col', () => parseWhEngineRequest(baseReq({ query: { select: [{ op: 'sum', alias: 's' }] } })), 'malformed');
  throwsEngine('empty select', () => parseWhEngineRequest(baseReq({ query: { select: [] } })), 'malformed');
  throwsEngine('select not an array', () => parseWhEngineRequest(baseReq({ query: { select: 'sum' } })), 'malformed');
  throwsEngine('bad alias type', () => parseWhEngineRequest(baseReq({ query: { select: [{ op: 'count', alias: 3 }] } })), 'malformed');
  throwsEngine('bare projection (no op) is not v0', () => parseWhEngineRequest(baseReq({ query: { select: [{ col: 'amount' }] } })), 'malformed');
});

Deno.test('parse: planner honesty — HAVING rejects, message names the clause (scatter §1.2 #4)', () => {
  throwsEngine('having object', () => parseWhEngineRequest(baseReq({ query: { select: [{ op: 'count', alias: 'c' }], having: { c: 5 } } })), 'malformed', 'HAVING');
  throwsEngine('having string', () => parseWhEngineRequest(baseReq({ query: { select: [{ op: 'count', alias: 'c' }], having: 'x' } })), 'malformed', 'HAVING');
});

Deno.test('parse: planner honesty — ORDER BY rejects non-empty; null/absent/[] accepted', () => {
  throwsEngine('orderBy non-empty', () => parseWhEngineRequest(baseReq({ query: { select: [{ op: 'count', alias: 'c' }], orderBy: [{ col: 'c', dir: 'desc' }] } })), 'malformed', 'ORDER BY');
  throwsEngine('orderBy number', () => parseWhEngineRequest(baseReq({ query: { select: [{ op: 'count', alias: 'c' }], orderBy: 5 } })), 'malformed', 'ORDER BY');
  eqTrue('orderBy null accepted', deepEq(parseWhEngineRequest(baseReq({ query: { select: [{ op: 'count', alias: 'c' }], orderBy: null } })).query, { select: [{ op: 'count', alias: 'c' }] }));
  eqTrue('orderBy [] accepted', deepEq(parseWhEngineRequest(baseReq({ query: { select: [{ op: 'count', alias: 'c' }], orderBy: [] } })).query, { select: [{ op: 'count', alias: 'c' }] }));
});

Deno.test('parse: planner honesty — OFFSET>0 / DISTINCT=true reject; offset:0 / distinct:false accepted', () => {
  throwsEngine('offset 20', () => parseWhEngineRequest(baseReq({ query: { select: [{ op: 'count', alias: 'c' }], offset: 20 } })), 'malformed', 'OFFSET');
  throwsEngine('distinct true', () => parseWhEngineRequest(baseReq({ query: { select: [{ op: 'count', alias: 'c' }], distinct: true } })), 'malformed', 'DISTINCT');
  eqTrue('offset 0 accepted (dropped as neutral, matching the full-shape normalization pin)', !('offset' in parseWhEngineRequest(baseReq({ query: { select: [{ op: 'count', alias: 'c' }], offset: 0 } })).query));
  eqTrue('distinct false accepted', !('distinct' in parseWhEngineRequest(baseReq({ query: { select: [{ op: 'count', alias: 'c' }], distinct: false } })).query));
});

Deno.test('parse: bad where shapes => malformed (in-list dropped in v0 — planner honesty)', () => {
  throwsEngine('unknown where op', () => parseWhEngineRequest(baseReq({ query: { select: [{ op: 'count', alias: 'c' }], where: [{ col: 'a', op: 'natch', value: 1 }] } })), 'malformed');
  throwsEngine('in rejected in v0', () => parseWhEngineRequest(baseReq({ query: { select: [{ op: 'count', alias: 'c' }], where: [{ col: 'a', op: 'in', value: [1, 2] }] } })), 'malformed');
  throwsEngine('missing value', () => parseWhEngineRequest(baseReq({ query: { select: [{ op: 'count', alias: 'c' }], where: [{ col: 'a', op: 'eq' }] } })), 'malformed');
  throwsEngine('between needs a 2-array', () => parseWhEngineRequest(baseReq({ query: { select: [{ op: 'count', alias: 'c' }], where: [{ col: 'a', op: 'between', value: [1] }] } })), 'malformed');
  throwsEngine('between 3-array', () => parseWhEngineRequest(baseReq({ query: { select: [{ op: 'count', alias: 'c' }], where: [{ col: 'a', op: 'between', value: [1, 2, 3] }] } })), 'malformed');
  throwsEngine('is only null|true|false', () => parseWhEngineRequest(baseReq({ query: { select: [{ op: 'count', alias: 'c' }], where: [{ col: 'a', op: 'is', value: 0 }] } })), 'malformed');
  throwsEngine('where entry not an object', () => parseWhEngineRequest(baseReq({ query: { select: [{ op: 'count', alias: 'c' }], where: ['a'] } })), 'malformed');
  throwsEngine('where col not identifier', () => parseWhEngineRequest(baseReq({ query: { select: [{ op: 'count', alias: 'c' }], where: [{ col: 'a;b', op: 'eq', value: 1 }] } })), 'malformed');
});

Deno.test('parse: coverage_mode validation + limit shapes + bad query container', () => {
  throwsEngine('unknown coverage_mode', () => parseWhEngineRequest(baseReq({ coverage_mode: 'quorum' })), 'malformed', 'coverage_mode');
  throwsEngine('numeric coverage_mode', () => parseWhEngineRequest(baseReq({ coverage_mode: 1 })), 'malformed', 'coverage_mode');
  throwsEngine('negative limit', () => parseWhEngineRequest(baseReq({ query: { select: [{ op: 'count', alias: 'c' }], limit: -1 } })), 'malformed');
  throwsEngine('float limit', () => parseWhEngineRequest(baseReq({ query: { select: [{ op: 'count', alias: 'c' }], limit: 1.5 } })), 'malformed');
  throwsEngine('no query', () => parseWhEngineRequest(baseReq({ query: undefined })), 'malformed');
  throwsEngine('query not object', () => parseWhEngineRequest(baseReq({ query: 9 })), 'malformed');
  eqTrue('limit null accepted (no truncation)', parseWhEngineRequest(baseReq({ query: { select: [{ op: 'count', alias: 'c' }], limit: null } })).query.limit === null);
});

// -----------------------------------------------------------------------------
// Plan-builder pins.
// -----------------------------------------------------------------------------
Deno.test('plan: grouped build — groupKeys + colPlans + numeric scales exact', () => {
  const req = parseWhEngineRequest(baseReq({
    query: {
      select: [
        { op: 'sum', col: 'amount', alias: 's' },
        { op: 'count', alias: 'c' },
        { op: 'count', col: 'qty', alias: 'cq' },
        { op: 'min', col: 'price', alias: 'mn' },
      ],
      groupBy: ['region'],
    },
  }));
  const plan = buildMergePlan(req, { columnTypes: COLS as Record<string, never>, columnScales: SCALES });
  eq('exact plan', plan, {
    table: 'orders',
    groupKeys: [{ col: 'region', type: 'text' }],
    aggs: {
      s: { op: 'sum', col: 'amount', colPlan: { col: 'amount', type: 'numeric', scale: 0 } },
      c: { op: 'count' },
      cq: { op: 'count', col: 'qty', colPlan: { col: 'qty', type: 'int8' } },
      mn: { op: 'min', col: 'price', colPlan: { col: 'price', type: 'numeric', scale: 2 } },
    },
  });
});

Deno.test('plan: scalar build — count(*) col-less, derived names, no groupKeys', () => {
  const req = parseWhEngineRequest(baseReq({
    query: { select: [{ op: 'sum', col: 'amount' }, { op: 'count' }] },
  }));
  const plan = buildMergePlan(req, { columnTypes: COLS as Record<string, never>, columnScales: SCALES });
  eq('scalar plan with derived names', plan, {
    table: 'orders',
    aggs: {
      'sum(amount)': { op: 'sum', col: 'amount', colPlan: { col: 'amount', type: 'numeric', scale: 0 } },
      'count(*)': { op: 'count' },
    },
  });
  eqTrue('no groupKeys on scalar plans', plan.groupKeys === undefined);
});

Deno.test('plan: groupBy col not in columnTypes map => malformed (pin)', () => {
  const req = parseWhEngineRequest(baseReq({
    query: { select: [{ op: 'count', alias: 'c' }], groupBy: ['nope'] },
  }));
  throwsEngine('unknown groupBy col', () => buildMergePlan(req, { columnTypes: COLS as Record<string, never> }), 'malformed');
});

Deno.test('plan: unknown select col / sum-over-text / min-over-timestamptz => malformed (merge law as 400)', () => {
  const unknown = parseWhEngineRequest(baseReq({ query: { select: [{ op: 'sum', col: 'ghost', alias: 's' }] } }));
  throwsEngine('unknown select col', () => buildMergePlan(unknown, { columnTypes: COLS as Record<string, never> }), 'malformed');

  const sumText = parseWhEngineRequest(baseReq({ table: 'notes', query: { select: [{ op: 'sum', col: 'note', alias: 's' }] } }));
  throwsEngine('sum over text (P2-4 law)', () => buildMergePlan(sumText, { columnTypes: COLS as Record<string, never> }), 'malformed');

  const minTs = parseWhEngineRequest(baseReq({ query: { select: [{ op: 'min', col: 'created_at', alias: 'm' }] } }));
  throwsEngine('min over timestamptz', () => buildMergePlan(minTs, { columnTypes: COLS as Record<string, never> }), 'malformed');
});

Deno.test('plan: duplicate alias / duplicate groupBy / bad groupBy shape => malformed', () => {
  const dupAlias = parseWhEngineRequest(baseReq({ query: { select: [{ op: 'sum', col: 'amount', alias: 'x' }, { op: 'count', alias: 'x' }] } }));
  throwsEngine('duplicate alias', () => buildMergePlan(dupAlias, { columnTypes: COLS as Record<string, never> }), 'malformed');
  const dupGroup = parseWhEngineRequest(baseReq({ query: { select: [{ op: 'count', alias: 'c' }], groupBy: ['region', 'region'] } }));
  throwsEngine('duplicate groupBy', () => buildMergePlan(dupGroup, { columnTypes: COLS as Record<string, never> }), 'malformed');
  throwsEngine('non-string groupBy', () => parseWhEngineRequest(baseReq({ query: { select: [{ op: 'count', alias: 'c' }], groupBy: [3] } })), 'malformed');
});

// -----------------------------------------------------------------------------
// E11 shard-pruning pins (conservative, boundary-inclusive).
// -----------------------------------------------------------------------------
Deno.test('E11: gte 2026-04-10 over [Jan-Feb)/[Feb-Apr)/[Apr-Jun) => only S3 (pin)', () => {
  const sel = selectShards(RANGE_FLEET, [{ col: 'created_at', op: 'gte', value: '2026-04-10' }], {
    shardKeyColumn: 'created_at', shardKeyType: 'range', shardKeyPlan: { col: 'created_at', type: 'timestamptz' },
  });
  eq('only S3', sel.map((r) => r.shard), ['S3']);
});

Deno.test('E11: boundary — predicate start == key_max of S2 conservatively includes S2; no predicate => all (pins)', () => {
  const boundary = selectShards(RANGE_FLEET, [{ col: 'created_at', op: 'gte', value: '2026-04-01' }], {
    shardKeyColumn: 'created_at', shardKeyType: 'range', shardKeyPlan: { col: 'created_at', type: 'timestamptz' },
  });
  eq('boundary shard included (conservative)', boundary.map((r) => r.shard), ['S2', 'S3']);

  const all = selectShards(RANGE_FLEET, undefined, {
    shardKeyColumn: 'created_at', shardKeyType: 'range', shardKeyPlan: { col: 'created_at', type: 'timestamptz' },
  });
  eq('no predicate => all shards', all.map((r) => r.shard), ['S1', 'S2', 'S3']);

  const nonKey = selectShards(RANGE_FLEET, [{ col: 'note', op: 'eq', value: 'x' }], {
    shardKeyColumn: 'created_at', shardKeyType: 'range', shardKeyPlan: { col: 'created_at', type: 'timestamptz' },
  });
  eq('predicate on a non-key column never prunes', nonKey.map((r) => r.shard), ['S1', 'S2', 'S3']);
});

Deno.test('E11: eq/lte/gt/between conservative closed-range pruning', () => {
  const sel = (op: string, value: unknown): string[] =>
    selectShards(RANGE_FLEET, [{ col: 'created_at', op, value }], {
      shardKeyColumn: 'created_at', shardKeyType: 'range', shardKeyPlan: { col: 'created_at', type: 'timestamptz' },
    }).map((r) => r.shard);
  eq('eq inside S2 => only S2', sel('eq', '2026-02-15'), ['S2']);
  eq('eq touching both boundary shards => S2+S3 (conservative)', sel('eq', '2026-04-01'), ['S2', 'S3']);
  eq('lte 2026-02-01 => S1+S2 (boundary conservative)', sel('lte', '2026-02-01'), ['S1', 'S2']);
  eq('gt 2026-04-01 => S2+S3 (gt maps to the closed conservative range)', sel('gt', '2026-04-01'), ['S2', 'S3']);
  eq('between [Feb-15, May-15] => S2+S3', sel('between', ['2026-02-15', '2026-05-15']), ['S2', 'S3']);
  eq('lt 2025-12-31 => nothing matches (legit empty)', sel('lt', '2025-12-31'), []);
  eq('lt 2026-01-01 => S1 (predicate end == key_min: boundary-touching shard conservatively included)', sel('lt', '2026-01-01'), ['S1']);
  eq('gte 2026-03-01 + lt 2026-05-01 (two preds) => intersected to S2+S3', selectShards(RANGE_FLEET, [
    { col: 'created_at', op: 'gte', value: '2026-03-01' }, { col: 'created_at', op: 'lt', value: '2026-05-01' },
  ], { shardKeyColumn: 'created_at', shardKeyType: 'range', shardKeyPlan: { col: 'created_at', type: 'timestamptz' } }).map((r) => r.shard), ['S2', 'S3']);
  throwsEngine('between reversed bounds => malformed', () =>
    selectShards(RANGE_FLEET, [{ col: 'created_at', op: 'between', value: ['2026-05-01', '2026-01-01'] }], {
      shardKeyColumn: 'created_at', shardKeyType: 'range', shardKeyPlan: { col: 'created_at', type: 'timestamptz' },
    }), 'malformed');
});

Deno.test('E11: hash — eq with hashSlotFn filters the slot; without fn => all; non-eq on hash key => all (pins)', () => {
  const fleet = [dirRow('S1', null, null, 0), dirRow('S2', null, null, 1), dirRow('S3', null, null, 0)];
  const sel = (where: { col: string; op: string; value: unknown }[], fn?: (v: unknown) => number): string[] =>
    selectShards(fleet, where, { shardKeyColumn: 'account', shardKeyType: 'hash', ...(fn ? { hashSlotFn: fn } : {}) }).map((r) => r.shard);
  eq('eq hash key => matching slot only (pin)', sel([{ col: 'account', op: 'eq', value: 'x' }], (v) => (v === 'x' ? 1 : 0)), ['S2']);
  eq('eq hash key other value => slot-0 shards', sel([{ col: 'account', op: 'eq', value: 'y' }], (v) => (v === 'x' ? 1 : 0)), ['S1', 'S3']);
  eq('no hashSlotFn => ALL shards (conservative fallback, pin)', sel([{ col: 'account', op: 'eq', value: 'x' }]), ['S1', 'S2', 'S3']);
  eq('non-eq op on hash key => all shards (conservative)', sel([{ col: 'account', op: 'gte', value: 'x' }], (v) => (v === 'x' ? 1 : 0)), ['S1', 'S2', 'S3']);
  eq('predicate on non-key col with hashSlotFn => all (fn not consulted)', sel([{ col: 'note', op: 'eq', value: 'x' }], (v) => (v === 'x' ? 1 : 0)), ['S1', 'S2', 'S3']);
});

Deno.test('E11: typed canonical compare — numeric-key inversion case + text key code-point order', () => {
  // numeric compare: '1000' < '995' as CODE POINTS — string compare would
  // wrongly EXCLUDE the shard; typed compare keeps it.
  const numFleet = [dirRow('N1', '999', '1000')];
  eq('numeric gte 995 includes the shard (typed, not lexicographic)', selectShards(numFleet, [{ col: 'id', op: 'gte', value: 995 }], {
    shardKeyColumn: 'id', shardKeyType: 'range', shardKeyPlan: { col: 'id', type: 'int8' },
  }).map((r) => r.shard), ['N1']);
  eq('numeric gte 1001 excludes it', selectShards(numFleet, [{ col: 'id', op: 'gte', value: 1001 }], {
    shardKeyColumn: 'id', shardKeyType: 'range', shardKeyPlan: { col: 'id', type: 'int8' },
  }).map((r) => r.shard), []);

  const textFleet = [dirRow('T1', 'apple', 'banana')];
  eq('text key gte avocado => included (avocado < banana)', selectShards(textFleet, [{ col: 'region', op: 'gte', value: 'avocado' }], {
    shardKeyColumn: 'region', shardKeyType: 'range', shardKeyPlan: { col: 'region', type: 'text' },
  }).map((r) => r.shard), ['T1']);
  eq('text key gte blue => excluded (blue > banana)', selectShards(textFleet, [{ col: 'region', op: 'gte', value: 'blue' }], {
    shardKeyColumn: 'region', shardKeyType: 'range', shardKeyPlan: { col: 'region', type: 'text' },
  }).map((r) => r.shard), []);
});

Deno.test('E11: unparseable bound conservatively included; bad predicate value => malformed', () => {
  const weird = [dirRow('W1', 'garbage!!', '2026-06-01')];
  eq('unparseable key_min => include (cannot compare => do not prune)', selectShards(weird, [{ col: 'created_at', op: 'gte', value: '2027-01-01' }], {
    shardKeyColumn: 'created_at', shardKeyType: 'range', shardKeyPlan: { col: 'created_at', type: 'timestamptz' },
  }).map((r) => r.shard), ['W1']);

  throwsEngine('bad predicate value => malformed (planner honesty)', () =>
    selectShards(RANGE_FLEET, [{ col: 'created_at', op: 'gte', value: 'not-a-date' }], {
      shardKeyColumn: 'created_at', shardKeyType: 'range', shardKeyPlan: { col: 'created_at', type: 'timestamptz' },
    }), 'malformed');
  throwsTypeError('range selection without a shardKeyPlan => TypeError (programmer error)', () =>
    selectShards(RANGE_FLEET, [{ col: 'created_at', op: 'gte', value: '2026-04-10' }], {
      shardKeyColumn: 'created_at', shardKeyType: 'range',
    }));
});

// -----------------------------------------------------------------------------
// Per-shard URL compile pins (kit-validated bare agg form, implicit grouping).
// -----------------------------------------------------------------------------
Deno.test('compile: grouped URL — bare sum()/count() + implicit grouping + filter, exact bytes', () => {
  const req = parseWhEngineRequest(baseReq({
    query: {
      select: [{ op: 'sum', col: 'amount', alias: 's' }, { op: 'count', alias: 'c' }],
      where: [{ col: 'created_at', op: 'gte', value: '2026-01-01' }],
      groupBy: ['region'],
    },
  }));
  const plan = buildMergePlan(req, { columnTypes: COLS as Record<string, never>, columnScales: SCALES });
  eq('exact URL', compileShardUrl('S1', plan, req.query.where), 'https://S1.supabase.co/rest/v1/orders?select=region,sum(amount),count()&created_at=gte.2026-01-01');
});

Deno.test('compile: scalar URL + count(col) + between => gte/lte pair + value encoding', () => {
  const req = parseWhEngineRequest(baseReq({
    query: {
      select: [{ op: 'sum', col: 'amount' }, { op: 'count', col: 'qty' }, { op: 'avg', col: 'qty' }],
      where: [
        { col: 'created_at', op: 'between', value: ['2026-01-01', '2026-04-01'] },
        { col: 'note', op: 'eq', value: 'hello world&x=1' },
      ],
    },
  }));
  const plan = buildMergePlan(req, { columnTypes: COLS as Record<string, never>, columnScales: SCALES });
  eq('scalar URL with between pair + encoded value', compileShardUrl('S2', plan, req.query.where),
    'https://S2.supabase.co/rest/v1/orders?select=sum(amount),count(qty),avg(qty)&created_at=gte.2026-01-01&created_at=lte.2026-04-01&note=eq.hello%20world%26x%3D1');
  const noWhere = parseWhEngineRequest(baseReq({ query: { select: [{ op: 'count', col: 'qty' }] } }));
  eq('no where => select param only', compileShardUrl('S3', buildMergePlan(noWhere, { columnTypes: COLS as Record<string, never> })), 'https://S3.supabase.co/rest/v1/orders?select=count(qty)');
});

Deno.test('compile: Accept-Profile public header constant pinned (shard plane, §4.1)', () => {
  eq('Accept-Profile: public', SHARD_REQUEST_HEADERS, { 'Accept-Profile': 'public' });
});

// -----------------------------------------------------------------------------
// Warning classification pins (§5.1 + stamped-header law).
// -----------------------------------------------------------------------------
Deno.test('classify: gateway 402/429 vs stamped shard-app 5xx vs network vs timeout (pinned classes)', () => {
  eq('402 => http_402 (gateway class)', classifyFetchFailure({ httpStatus: 402 }), { code: 'http_402' });
  eq('429 => http_429 (gateway class)', classifyFetchFailure({ httpStatus: 429 }), { code: 'http_429' });
  eq('503 stamped => http_5xx + stamped (shard-app class, retried-eligible)', classifyFetchFailure({ httpStatus: 503, stamped: true }), { code: 'http_5xx', stamped: true });
  eq('500 unstamped => http_5xx, stamped:false', classifyFetchFailure({ httpStatus: 500 }), { code: 'http_5xx', stamped: false });
  eq('network', classifyFetchFailure({ code: 'network' }), { code: 'network' });
  eq('timeout', classifyFetchFailure({ code: 'timeout' }), { code: 'timeout' });
  eq('abort_on_oversize passes through', classifyFetchFailure({ code: 'abort_on_oversize' }), { code: 'abort_on_oversize' });
});

Deno.test('classify: unclassifiable failures => excluded (conservative), detail carried', () => {
  eq('odd 4xx', classifyFetchFailure({ httpStatus: 400 }), { code: 'excluded', detail: 'http 400' });
  eq('unknown code', classifyFetchFailure({ code: 'weird' }), { code: 'excluded', detail: 'weird' });
  eq('empty', classifyFetchFailure({}), { code: 'excluded' });
});

// -----------------------------------------------------------------------------
// Fan-out pins (WINDOW=16, per-shard timeout, allSettled semantics).
// -----------------------------------------------------------------------------
Deno.test('fanout: WINDOW=16 respected — max concurrent === 16 for n=40 (pin)', async () => {
  let inFlight = 0;
  let maxInFlight = 0;
  const fetcher: WhShardFetcher = async (shard: string) => {
    inFlight++;
    maxInFlight = Math.max(maxInFlight, inFlight);
    await Promise.resolve();
    await Promise.resolve();
    inFlight--;
    return { ok: true, envelope: scalarEnv(shard, 'orders', { s: '1', c: 1 }) };
  };
  const targets = Array.from({ length: 40 }, (_, i) => ({ shard: `S${i}`, url: `https://S${i}.supabase.co/rest/v1/orders?select=count()` }));
  const outcomes = await runFanout(targets, fetcher, neverTimeoutTimers(), {});
  eq('max concurrent is exactly the window (<= 16 AND fills it)', maxInFlight, DEFAULT_WINDOW);
  eq('all 40 resolved ok, in target order', outcomes.map((o) => o.shard), targets.map((t) => t.shard));
  eqTrue('all ok', outcomes.every((o) => o.ok));
});

Deno.test('fanout: window hard max 32 (33 => TypeError); window = min(window, n) for small n', async () => {
  throwsTypeError('window 33 above the hard max', () => {
    void runFanout([], okFetch(scalarEnv('S', 'orders', { s: null, c: 0 })), neverTimeoutTimers(), { window: 33 });
  });
  let inFlight = 0;
  let maxInFlight = 0;
  const fetcher: WhShardFetcher = async (shard: string) => {
    inFlight++;
    maxInFlight = Math.max(maxInFlight, inFlight);
    await Promise.resolve();
    inFlight--;
    return { ok: true, envelope: scalarEnv(shard, 'orders', { s: '1', c: 1 }) };
  };
  const targets3 = Array.from({ length: 3 }, (_, i) => ({ shard: `S${i}`, url: 'u' }));
  await runFanout(targets3, fetcher, neverTimeoutTimers(), {});
  eq('window = min(16, n=3) = 3 (never idle workers)', maxInFlight, 3);
  eqTrue('constants pinned', DEFAULT_WINDOW === 16 && MAX_WINDOW === 32 && DEFAULT_SHARD_TIMEOUT_MS === 8000 && MAX_GROUPS === 2000);
});

Deno.test('fanout: per-shard timeout produces a {code:timeout} warning (pin)', async () => {
  const calls: string[] = [];
  const fetcher: WhShardFetcher = async (shard: string) => {
    calls.push(shard);
    await new Promise<never>(() => {}); // hangs forever
  };
  const outcomes = await runFanout([{ shard: 'S1', url: 'u' }], fetcher, firedTimeoutTimers(), {});
  eq('exactly one timeout outcome', outcomes, [{
    shard: 'S1',
    ok: false,
    latencyMs: outcomes[0].latencyMs,
    estRows: 0,
    warning: { code: 'timeout' },
    error: 'timeout',
  }]);
  eq('fetcher WAS called (no cancellation)', calls, ['S1']);
});

Deno.test('fanout: per-shard timeout with the DEFAULT real timers (8ms) — smoke', async () => {
  const fetcher: WhShardFetcher = async () => await new Promise<never>(() => {});
  const outcomes = await runFanout([{ shard: 'S1', url: 'u' }], fetcher, defaultWhEngineTimers(), { shardTimeoutMs: 8 });
  eq('real timer fires the timeout classification', outcomes[0].warning, { code: 'timeout' });
  const fast: WhShardFetcher = async (s: string) => ({ ok: true, envelope: scalarEnv(s, 'orders', { s: null, c: 0 }) });
  const okOut = await runFanout([{ shard: 'S2', url: 'u' }], fast, defaultWhEngineTimers(), { shardTimeoutMs: 5000 });
  eqTrue('fast fetch under the default timers stays ok', okOut[0].ok);
});

Deno.test('fanout: fetcher throw => network warning; failure est_rows passthrough / default 0 (pin)', async () => {
  const thrower: WhShardFetcher = async () => {
    throw new TypeError('fetch failed');
  };
  const outThrow = await runFanout([{ shard: 'T1', url: 'u' }], thrower, neverTimeoutTimers(), {});
  eq('throw classified as network, est_rows 0', outThrow[0], {
    shard: 'T1', ok: false, latencyMs: outThrow[0].latencyMs, estRows: 0, warning: { code: 'network' }, error: 'network',
  });

  const outEst = await runFanout([{ shard: 'T2', url: 'u' }], failFetch({ httpStatus: 402 }, 1234), neverTimeoutTimers(), {});
  eq('failure keeps the fetcher-reported est_rows (pin)', outEst[0].estRows, 1234);
  eq('warning code classified from httpStatus', outEst[0].warning, { code: 'http_402' });
  eqTrue('not ok', outEst[0].ok === false);
});

// -----------------------------------------------------------------------------
// Execute pins: full pipeline (parse -> plan -> E11 -> fan-out -> merge ->
// finalize -> assembly).
// -----------------------------------------------------------------------------
Deno.test('E1 through the engine: 3-shard GROUP BY — rows exact canonical order, coverage 3/3, echoes (pin)', async () => {
  const urls: string[] = [];
  const fetcher: WhShardFetcher = async (shard: string, url: string) => {
    urls.push(url);
    if (shard === 'S1') return { ok: true, envelope: e1('S1', [{ k: ['eu'], s: '600', c: 6 }, { k: ['us'], s: '300', c: 3 }]) };
    if (shard === 'S2') return { ok: true, envelope: e1('S2', [{ k: ['eu'], s: '400', c: 4 }, { k: ['ap'], s: '50', c: 1 }]) };
    return { ok: true, envelope: e1('S3', [{ k: ['us'], s: '700', c: 7 }]) };
  };
  const res = await executeWhQuery({ ...E1_EXECUTE, fetcher });
  eq('rows in deterministic canonical order with exact E1 values (pin)', res.rows, [
    { k: ['ap'], aggs: { s: 50n, c: 1 } },
    { k: ['eu'], aggs: { s: 1000n, c: 10 } },
    { k: ['us'], aggs: { s: 1000n, c: 10 } },
  ]);
  eq('coverage string', res.coverage, '3/3');
  eq('coverage ratio', res.coverage_ratio, 1);
  eq('partial flag', res.partial, false);
  eq('qid echoed', res.qid, '01J9Q1ZZZZZZZZZZZZZZZZZZZZ');
  eq('directory_version echoed', res.directory_version, 42);
  eq('warnings empty', res.warnings, []);
  eq('perShard all ok', res.perShard.map((p) => ({ shard: p.shard, ok: p.ok, error: p.error })), [
    { shard: 'S1', ok: true, error: null },
    { shard: 'S2', ok: true, error: null },
    { shard: 'S3', ok: true, error: null },
  ]);
  eq('v pinned', res.v, 1);
  eqTrue('latency_ms present and numeric', typeof res.latency_ms === 'number');
  eqTrue('scalar result NEVER set alongside rows', res.result === undefined);
  eq('compiled per-shard URL is the kit bare form', urls[0], 'https://S1.supabase.co/rest/v1/orders?select=region,sum(amount),count()');
});

Deno.test('execute: scalar happy path — result ONE-of, perShard shape, latency_ms', async () => {
  const res = await executeWhQuery({
    ...E1_EXECUTE,
    req: parseWhEngineRequest(baseReq()),
    directoryRows: [dirRow('S1', null, null), dirRow('S2', null, null)],
    fetcher: (async (shard: string) => {
      if (shard === 'S1') return { ok: true, envelope: scalarEnv('S1', 'orders', { s: '600', c: 6 }) };
      return { ok: true, envelope: scalarEnv('S2', 'orders', { s: '400', c: 4 }) };
    }) as WhShardFetcher,
  });
  eq('scalar result (exact rational NOT expected here — sum/count)', res.result, { s: 1000n, c: 10 });
  eqTrue('rows NEVER set alongside result', res.rows === undefined);
  // r129 re-pin (census §2.1 #1; legB R-B2): the OK arm gained the additive
  // measurement keys — partial_rows = the CONSUMED envelope's rowCount;
  // partial_bytes = the UTF-8 byte length of JSON.stringify(consumed
  // envelope.partial) — the serialization convention of record
  // (wh_engine_core.ts:1951-1969). The expected byte count is computed from
  // the SAME deterministic fixture helper the fetcher returned (never from a
  // run variance); the exact-key-set law of deepEq is preserved (never
  // weakened to a projection). Error arms carry NEITHER key (unchanged pins).
  const s1Partial = scalarEnv('S1', 'orders', { s: '600', c: 6 }).partial;
  eq('perShard entry shape (§4.4: error null when ok) + r129 ok-arm partial_rows/partial_bytes (census §2.1 #1 re-pin)', res.perShard[0], {
    shard: 'S1',
    ok: true,
    latencyMs: res.perShard[0].latencyMs,
    error: null,
    partial_rows: 1,
    partial_bytes: new TextEncoder().encode(JSON.stringify(s1Partial)).length,
  });
  // Byte-convention hand-check: the §4.4 scalar partial serializes to EXACTLY
  // 143 UTF-8 bytes (key order kind,aggs,rows,rowCount,more — the env()
  // literal order; verified offline: {"kind":"scalar","aggs":{"s":{"op":
  // "sum","col":"amount"},"c":{"op":"count"}},"rows":[{"k":[],"a":{"s":
  // "600","c":6}}],"rowCount":1,"more":false}). A mutant measuring the whole
  // envelope, the pre-adaptation wire, or any other serialization REDs here.
  eq('r129 byte-convention hand-check: the scalar partial is 143 bytes', res.perShard[0].partial_bytes, 143);
  eq('coverage', res.coverage, '2/2');
});

// -----------------------------------------------------------------------------
// r138 C2 green-keeping pins: C1 row-estimate reconciliation advisory + F-2
// completeness floor (minimal shapes — the full lethal block is the battery
// leg's, against the FROZEN core; r53 split-cells).
// Hand-computed expectations, independent of the implementation: the E1 wave
// merges c = 6+3 (S1) + 4+1 (S2) + 7 (S3) => count_star 21.
// -----------------------------------------------------------------------------
Deno.test('r138 C2: advisory armed-and-SILENT on match; FIRES EXACTLY ONE on mismatch (never fails); F-2 floor degrades post-merge', async () => {
  const wave = (estimates: number[]) => ({
    ...E1_EXECUTE,
    directoryRows: estimates.map((est, i) => ({ ...dirRow(`S${i + 1}`, null, null), row_estimate: est })),
    fetcher: (async (shard: string) => {
      if (shard === 'S1') return { ok: true, envelope: e1('S1', [{ k: ['eu'], s: '600', c: 6 }, { k: ['us'], s: '300', c: 3 }]) };
      if (shard === 'S2') return { ok: true, envelope: e1('S2', [{ k: ['eu'], s: '400', c: 4 }, { k: ['ap'], s: '50', c: 1 }]) };
      return { ok: true, envelope: e1('S3', [{ k: ['us'], s: '700', c: 7 }]) };
    }) as WhShardFetcher,
  });

  // Hand-computed: Σ estimates 7+9+5 = 21 == merged count_star 21 => SILENT.
  const silent = await executeWhQuery(wave([7, 9, 5]));
  eq('C2 advisory: armed preconditions met (full coverage, bare count, no limit) yet warnings EXACTLY [] on match', silent.warnings, []);
  eq('C2 advisory silent: response stays 200-shaped (coverage 3/3, partial false)', [silent.coverage, silent.partial], ['3/3', false]);

  // Hand-computed: Σ estimates 7+8+5 = 20 != 21 => EXACTLY ONE warning, exact
  // shape; rows/partial/coverage untouched (advisory NEVER fails).
  const fired = await executeWhQuery(wave([7, 8, 5]));
  eq('C2 advisory fires on mismatch: EXACTLY ONE row_estimate_mismatch, est_rows = Σ estimates (20)', fired.warnings, [
    { shard: '<merged>', code: 'row_estimate_mismatch', est_rows: 20, retried: false },
  ]);
  eq('C2 advisory never fails: rows byte-identical to the silent wave', fired.rows, silent.rows);
  eq('C2 advisory never fails: coverage/partial unchanged', [fired.coverage, fired.partial, fired.coverage_ratio], ['3/3', false, 1]);

  // F-2 floor: min_shards 4 with 3 ok shards => partial:true + ONE additive
  // coverage_floor_unmet warning, data STILL returned (READS DEGRADE).
  const flooredReq = baseReq({ query: { select: [{ op: 'sum', col: 'amount', alias: 's' }, { op: 'count', alias: 'c' }], groupBy: ['region'], min_shards: 4 } });
  const floored = await executeWhQuery({ ...E1_EXECUTE, req: parseWhEngineRequest(flooredReq), directoryRows: wave([7, 9, 5]).directoryRows, fetcher: wave([7, 9, 5]).fetcher });
  eq('F-2 floor: floor unmet degrades partial:true (data still returned — 3 rows)', [floored.partial, (floored.rows ?? []).length], [true, 3]);
  eqTrue('F-2 floor: EXACTLY ONE coverage_floor_unmet warning rides the envelope', floored.warnings.filter((w) => w.code === 'coverage_floor_unmet').length === 1);
});

Deno.test('execute: coverage 2/3 + ratio + partial flag + warnings sorted by shard (pins)', async () => {
  const res = await executeWhQuery({
    ...E1_EXECUTE,
    fetcher: (async (shard: string) => {
      if (shard === 'S1') return { ok: false, warning: { httpStatus: 402 }, estRows: 10 };
      if (shard === 'S3') return { ok: false, warning: { code: 'timeout' }, estRows: 5 };
      return { ok: true, envelope: e1('S2', [{ k: ['eu'], s: '400', c: 4 }, { k: ['ap'], s: '50', c: 1 }]) };
    }) as WhShardFetcher,
  });
  eq('coverage string (pin: coverage := responded_shards/n, spec §5.3 single definition)', res.coverage, '1/3');
  eq('coverage ratio', res.coverage_ratio, 1 / 3);
  eq('partial flag (pin)', res.partial, true);
  eq('warnings sorted by shard ref (pin)', res.warnings, [
    { shard: 'S1', code: 'http_402', est_rows: 10, retried: false },
    { shard: 'S3', code: 'timeout', est_rows: 5, retried: false },
  ]);
  eq('perShard error carries the warning code', res.perShard.map((p) => p.error), ['http_402', null, 'timeout']);
  eq('merged rows come only from S2', res.rows, [{ k: ['ap'], aggs: { s: 50n, c: 1 } }, { k: ['eu'], aggs: { s: 400n, c: 4 } }]);
});

Deno.test('execute: all-fail scalar under best_effort => result {s:null,c:0} — E14 through the engine (pin)', async () => {
  const res = await executeWhQuery({
    ...E1_EXECUTE,
    req: parseWhEngineRequest(baseReq()),
    fetcher: failFetch({ code: 'network' }),
  });
  eq('E14 identity through the engine (pin)', res.result, { s: null, c: 0 });
  eq('coverage 0/3', res.coverage, '0/3');
  eq('ratio 0', res.coverage_ratio, 0);
  eqTrue('partial', res.partial);
  eq('three network warnings with est_rows 0', res.warnings, [
    { shard: 'S1', code: 'network', est_rows: 0, retried: false },
    { shard: 'S2', code: 'network', est_rows: 0, retried: false },
    { shard: 'S3', code: 'network', est_rows: 0, retried: false },
  ]);
  eq('HTTP 200-class semantics: the engine RETURNS, it does not throw', res.v, 1);
});

Deno.test('execute: fail_fast with 1-of-3 failed => 5xx internal + perShard filled + collect-then-fail (pin)', async () => {
  const calls: string[] = [];
  let guard = 0;
  try {
    await executeWhQuery({
      ...E1_EXECUTE,
      req: parseWhEngineRequest(baseReq({ coverage_mode: 'fail_fast' })),
      fetcher: (async (shard: string) => {
        calls.push(shard);
        guard++;
        if (shard === 'S2') return { ok: false, warning: { httpStatus: 503, stamped: true }, estRows: 9 };
        return { ok: true, envelope: e1(shard, [{ k: ['eu'], s: '1', c: 1 }]) };
      }) as WhShardFetcher,
    });
    failed++;
    console.error('FAIL  fail_fast should have thrown');
  } catch (err) {
    if (err instanceof WhEngineError && err.code === 'internal') {
      passed++;
      console.log('  ok  fail_fast => WhEngineError internal (5xx class)');
      eq('perShard filled on the error payload (pin)', err.perShard?.map((p) => ({ shard: p.shard, ok: p.ok, error: p.error })), [
        { shard: 'S1', ok: true, error: null },
        { shard: 'S2', ok: false, error: 'http_5xx' },
        { shard: 'S3', ok: true, error: null },
      ]);
      eqTrue('stamped flag rides the perShard error path', err.perShard?.[1].stamped === true);
      eqTrue('latency_ms on the error payload', typeof err.latencyMs === 'number');
      eqTrue('directory_version present on the error payload (contract: even on errors when known)', err.directoryVersion === 42);
    } else {
      failed++;
      console.error(`FAIL  wrong throw: ${err}`);
    }
  }
  eq('collect-then-fail: ALL shards were fetched (no cancellation in v0, pin)', calls.length, 3);
  eqTrue('sanity', guard === 3);
});

Deno.test('execute: maxGroups — 2001 groups => capacity_exceeded; 2000 => ok (pin)', async () => {
  const bigRows = (n: number) => Array.from({ length: n }, (_, i) => ({ k: [i + 1], a: { s: '1', c: 1 } }));
  const bigFetch: WhShardFetcher = async (shard: string) => ({
    ok: true,
    envelope: env(shard, 'orders', 'grouped', ['id'], { s: { op: 'sum', col: 'amount' }, c: { op: 'count' } }, bigRows(2001)),
  });
  await rejectsEngine('2001 groups => 422 capacity_exceeded (pin)', () => executeWhQuery({
    ...E1_EXECUTE,
    req: parseWhEngineRequest(baseReq({ query: { select: [{ op: 'sum', col: 'amount', alias: 's' }, { op: 'count', alias: 'c' }], groupBy: ['id'] } })),
    shardKeyColumn: 'created_at',
    fetcher: bigFetch,
  }), 'capacity_exceeded');

  const okRes = await executeWhQuery({
    ...E1_EXECUTE,
    req: parseWhEngineRequest(baseReq({ query: { select: [{ op: 'sum', col: 'amount', alias: 's' }, { op: 'count', alias: 'c' }], groupBy: ['id'] } })),
    fetcher: (async (shard: string) => ({
      ok: true,
      envelope: env(shard, 'orders', 'grouped', ['id'], { s: { op: 'sum', col: 'amount' }, c: { op: 'count' } }, bigRows(2000)),
    })) as WhShardFetcher,
  });
  eq('2000 groups exactly => ok (boundary)', okRes.rows?.length, 2000);
});

Deno.test('execute: fetcher-reported oversize => abort_on_oversize warning + partial DISCARDED, not merged (pin)', async () => {
  const res = await executeWhQuery({
    ...E1_EXECUTE,
    fetcher: (async (shard: string) => {
      if (shard === 'S2') return { ok: false, warning: { code: 'abort_on_oversize' }, estRows: 500 };
      if (shard === 'S1') return { ok: true, envelope: e1('S1', [{ k: ['eu'], s: '600', c: 6 }]) };
      return { ok: true, envelope: e1('S3', [{ k: ['eu'], s: '100', c: 1 }]) };
    }) as WhShardFetcher,
  });
  eq('oversize warning with est_rows (pin)', res.warnings, [{ shard: 'S2', code: 'abort_on_oversize', est_rows: 500, retried: false }]);
  eq('S2 partial DISCARDED — eu total excludes S2', res.rows, [{ k: ['eu'], aggs: { s: 700n, c: 7 } }]);
  eq('coverage counts only merged contributions', res.coverage, '2/3');
  eqTrue('partial', res.partial);
});

Deno.test('execute: schema_version mismatch => schema_mismatch warning + excluded from merge', async () => {
  const res = await executeWhQuery({
    ...E1_EXECUTE,
    tableSchemaVersion: 3,
    fetcher: (async (shard: string) => {
      if (shard === 'S2') return { ok: true, envelope: env('S2', 'orders', 'grouped', ['region'], E1_AGGS, [{ k: ['eu'], a: { s: '999999', c: 999 } }], 2), estRows: 42 };
      return { ok: true, envelope: e1(shard === 'S1' ? 'S1' : 'S3', [{ k: ['eu'], s: '10', c: 1 }]) };
    }) as WhShardFetcher,
  });
  eq('schema_mismatch warning carries est_rows', res.warnings, [{ shard: 'S2', code: 'schema_mismatch', est_rows: 42, retried: false }]);
  eq('stale-schema partial excluded from merge', res.rows, [{ k: ['eu'], aggs: { s: 20n, c: 2 } }]);
  eq('coverage counts merged only', res.coverage, '2/3');
});

Deno.test('execute: merge-phase lying envelope (envelope_invalid) => excluded warning; best_effort survives', async () => {
  const res = await executeWhQuery({
    ...E1_EXECUTE,
    fetcher: (async (shard: string) => {
      if (shard === 'S2') {
        // Missing declared agg value in the row — passes fetch, dies in merge.
        return { ok: true, envelope: env('S2', 'orders', 'grouped', ['region'], E1_AGGS, [{ k: ['eu'], a: { s: '400' } }], 3) };
      }
      if (shard === 'S1') return { ok: true, envelope: e1('S1', [{ k: ['eu'], s: '600', c: 6 }]) };
      return { ok: true, envelope: e1('S3', [{ k: ['us'], s: '700', c: 7 }]) };
    }) as WhShardFetcher,
  });
  eq('lying shard excluded with a warning', res.warnings.length, 1);
  eqTrue('code excluded', res.warnings[0].code === 'excluded');
  eqTrue('detail carries the merge error', typeof res.warnings[0].detail === 'string');
  eq('the honest shards still merged', res.rows, [{ k: ['eu'], aggs: { s: 600n, c: 6 } }, { k: ['us'], aggs: { s: 700n, c: 7 } }]);
  eq('coverage 2/3', res.coverage, '2/3');
});

Deno.test('execute: deterministic output — shuffled completion orders => identical response', async () => {
  const mkFetcher = (ticks: Record<string, number>): WhShardFetcher =>
    async (shard: string) => {
      for (let i = 0; i < (ticks[shard] ?? 0); i++) await Promise.resolve();
      return { ok: true, envelope: e1(shard, [{ k: ['eu'], s: '600', c: 6 }, { k: ['us'], s: '300', c: 3 }]) };
    };
  const a = await executeWhQuery({ ...E1_EXECUTE, fetcher: mkFetcher({ S1: 3, S2: 1, S3: 2 }) });
  const b = await executeWhQuery({ ...E1_EXECUTE, fetcher: mkFetcher({ S1: 1, S2: 3, S3: 2 }) });
  eq('latency-independent: identical rows', a.rows, b.rows);
  eq('latency-independent: identical warnings/coverage', [a.coverage, a.warnings], [b.coverage, b.warnings]);
  eq('arrival order does not leak into the merged envelope', a.rows?.length, 2);
});

Deno.test('execute: limit truncates grouped rows post-finalize (deterministic canonical order)', async () => {
  const res = await executeWhQuery({
    ...E1_EXECUTE,
    req: parseWhEngineRequest(baseReq({
      query: { select: [{ op: 'sum', col: 'amount', alias: 's' }, { op: 'count', alias: 'c' }], groupBy: ['region'], limit: 2 },
    })),
    fetcher: (async (shard: string) => {
      if (shard === 'S1') return { ok: true, envelope: e1('S1', [{ k: ['eu'], s: '600', c: 6 }, { k: ['us'], s: '300', c: 3 }]) };
      if (shard === 'S2') return { ok: true, envelope: e1('S2', [{ k: ['eu'], s: '400', c: 4 }, { k: ['ap'], s: '50', c: 1 }]) };
      return { ok: true, envelope: e1('S3', [{ k: ['us'], s: '700', c: 7 }]) };
    }) as WhShardFetcher,
  });
  eq('first 2 groups in canonical order (ap, eu)', res.rows, [
    { k: ['ap'], aggs: { s: 50n, c: 1 } },
    { k: ['eu'], aggs: { s: 1000n, c: 10 } },
  ]);
  eqTrue('coverage is NOT affected by limit', res.coverage === '3/3');
});

Deno.test('execute: empty directory => tier_warm; pruned-to-zero => empty result, coverage 0/0, not partial', async () => {
  await rejectsEngine('no hot placements at all => 404-class tier_warm', () => executeWhQuery({ ...E1_EXECUTE, directoryRows: [] }), 'tier_warm');

  const pruned = await executeWhQuery({
    ...E1_EXECUTE,
    directoryRows: RANGE_FLEET, // disjoint-span test needs RANGE rows — E1's are unbounded (null/null)
    req: parseWhEngineRequest(baseReq({
      query: {
        select: [{ op: 'sum', col: 'amount', alias: 's' }, { op: 'count', alias: 'c' }],
        where: [{ col: 'created_at', op: 'gte', value: '2027-01-01' }],
        groupBy: ['region'],
      },
    })),
  });
  eq('predicate disjoint from every span => zero shards selected', pruned.rows, []);
  eq('coverage 0/0 (nothing attempted, nothing lost)', pruned.coverage, '0/0');
  eq('ratio vacuously 1', pruned.coverage_ratio, 1);
  // r138 F-1b ADDITIVE RE-PIN (same commit as the re-adjudication — exact
  // folded shape, never weakened): the poison `0/0 + partial:false + no
  // warnings` verdict is retired. The empty/under-selection envelope is now
  // partial:true + EXACTLY the ONE <merged>-labelled fleet_de_listed
  // warning ("complete answer: no data exists" never ships silently again).
  eqTrue('re-adjudicated: partial:true (a 0/0 verdict is never a complete answer)', pruned.partial === true);
  eq('re-adjudicated: EXACTLY the ONE fleet_de_listed warning', pruned.warnings, [
    { shard: '<merged>', code: 'fleet_de_listed', est_rows: 0, retried: false },
  ]);
});

Deno.test('execute: window/timeout params plumb through (window:2, n=4 => max 2)', async () => {
  let inFlight = 0;
  let maxInFlight = 0;
  const res = await executeWhQuery({
    ...E1_EXECUTE,
    req: parseWhEngineRequest(baseReq()), // scalar req — the E1_EXECUTE default is GROUPED and would kind-mismatch the scalar envelopes below
    directoryRows: Array.from({ length: 4 }, (_, i) => dirRow(`S${i}`, null, null)),
    window: 2,
    fetcher: (async (shard: string) => {
      inFlight++;
      maxInFlight = Math.max(maxInFlight, inFlight);
      await Promise.resolve();
      await Promise.resolve();
      inFlight--;
      return { ok: true, envelope: scalarEnv(shard, 'orders', { s: '1', c: 1 }) };
    }) as WhShardFetcher,
  });
  eq('execution window respected through the engine', maxInFlight, 2);
  eq('coverage 4/4', res.coverage, '4/4');
});

// -----------------------------------------------------------------------------
// Harness report (hand-rolled runner, no external deps).
// -----------------------------------------------------------------------------
Deno.test('__report__', () => {
  console.log(`\nwh_engine_core_test: ${passed} assertions passed, ${failed} failed`);
  if (failed > 0) throw new Error(`${failed} assertion(s) failed`);
});
