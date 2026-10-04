// =============================================================================
// _shared/wh_geo_plane_test.ts — r47 read-plane battery (wh-contract r47
// errata; impl plan research/geo_readplane_impl_plan.md §2/§6).
//
// Harness: same eq/throws conventions as wh_engine_core_test.ts; hand-built
// fixtures with hand-computed expectations (tests-lethal law). BigInt LSN
// cells include the LEXICOGRAPHIC TRAPS ('0/9' vs '0/1A', '0/1F' vs '0/9') —
// the exact inputs where text order and numeric order DISAGREE.
// Run: deno test -A --no-check -q supabase/functions/_shared/wh_geo_plane_test.ts
// =============================================================================

import {
  executeWhQuery,
  geoCoverageCovers,
  lsnAtLeast,
  PG_LSN_RE,
  parsePgLsn,
  parseWhEngineRequest,
  WhEngineError,
  type WhDirectoryRow,
  type WhEngineRequest,
  type WhGeoDirectoryRow,
  type WhShardFetcher,
  type WhEngineTimers,
} from './wh_engine_core.ts';
import { deriveTemplateHashes } from './wh_handshake.ts';
import { makeGeoDirectoryReader, toWhGeoDirectoryRow } from './wh_directory_reader.ts';
import { ENGINE_TEMPLATE_MANIFEST } from './wh_handshake.ts';
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

function throwsEngine(name: string, fn: () => unknown, code: WhEngineError['code'], msgPart?: string): void {
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

// ---------- fixtures ----------

function baseReq(overrides: Record<string, unknown> = {}): Record<string, unknown> {
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

const COLS: Record<string, string> = { region: 'text', amount: 'numeric', qty: 'int8', created_at: 'timestamptz' };

function dirRow(shard: string): WhDirectoryRow {
  return {
    shard,
    key_min: null,
    key_max: null,
    hash_slot: null,
    state: 'serving',
    platform_status: 'ACTIVE_HEALTHY',
    schema_version: 3,
    last_health_at: '2026-09-29T00:00:00.000000Z',
    logical_name: 'orders',
    shard_key_type: 'none',
    shard_key_column: null,
    table_schema_version: 3,
  };
}

function geoRow(overrides: Partial<WhGeoDirectoryRow> = {}): WhGeoDirectoryRow {
  return {
    project_ref: 'fmhostref',
    region: 'us-east-1',
    subscription: 'geo_sub',
    state: 'serving',
    applied_lsn: '0/FFFFFFFE',
    replay_ts: '2026-09-29T00:00:00Z',
    lag_bytes: null,
    coverage: ['orders'],
    last_health_at: '2026-09-29T00:00:00.000000Z',
    updated_at: '2026-09-29T00:00:00.000000Z',
    ...overrides,
  };
}

function scalarEnv(shard: string, table: string, schemaVersion = 3): WhPartialEnvelope {
  return {
    v: 1,
    shard,
    table,
    schema_version: schemaVersion,
    partial: {
      kind: 'scalar',
      aggs: { s: { op: 'sum', col: 'amount' }, c: { op: 'count' } },
      rows: [{ k: [], a: { s: '100', c: 4 } }],
      rowCount: 1,
      more: false,
    },
  };
}

function neverTimeoutTimers(): WhEngineTimers {
  return {
    nowMs: () => 0,
    startTimeout: () => ({ promise: new Promise<'timeout'>(() => {}), dispose: () => {} }),
  };
}

/** Stub fetcher: serves a valid scalar envelope for any (shard,url); records calls. */
function recordingFetcher(calls: { shard: string; url: string }[], schemaVersion = 3): WhShardFetcher {
  return async (shard, url) => {
    calls.push({ shard, url });
    return { ok: true as const, envelope: scalarEnv(shard, 'orders', schemaVersion) };
  };
}

const BASE_ARGS = {
  columnTypes: COLS as Record<string, never>,
  directoryRows: [dirRow('S1'), dirRow('S2'), dirRow('S3')],
  shardKeyColumn: '',
  shardKeyType: 'none' as const,
  directoryVersion: 42,
  timers: neverTimeoutTimers(),
};

// ---------- parse gates (§4.3 additive extension) ----------

Deno.test('geo parse: read_plane absent => field dropped, primary behavior', () => {
  const req = parseWhEngineRequest(baseReq());
  eqTrue('no read_plane key', (req as Record<string, unknown>).read_plane === undefined);
  eqTrue('no min_lsn key', (req as Record<string, unknown>).min_lsn === undefined);
});

Deno.test('geo parse: read_plane null = absent; read_plane primary explicit = kept', () => {
  eqTrue('null drops', parseWhEngineRequest(baseReq({ read_plane: null })).read_plane === undefined);
  eqTrue('primary kept', parseWhEngineRequest(baseReq({ read_plane: 'primary' })).read_plane === 'primary');
  eqTrue('replica kept', parseWhEngineRequest(baseReq({ read_plane: 'replica' })).read_plane === 'replica');
});

Deno.test('geo parse: unknown read_plane value => 400 malformed (frozen-contract law)', () => {
  for (const bad of ['edge', 'REPLICA', 'replica ', 1, true, ['replica']]) {
    throwsEngine(`read_plane=${JSON.stringify(bad)} rejected`, () => parseWhEngineRequest(baseReq({ read_plane: bad })), 'malformed');
  }
});

Deno.test('geo parse: min_lsn without read_plane => 400 naming both fields', () => {
  throwsEngine('min_lsn alone rejected', () => parseWhEngineRequest(baseReq({ min_lsn: '0/9' })), 'malformed', 'requires read_plane');
});

Deno.test('geo parse: min_lsn with read_plane=primary => 400 (floor vacuous on the write head)', () => {
  throwsEngine('min_lsn+primary rejected', () => parseWhEngineRequest(baseReq({ read_plane: 'primary', min_lsn: '0/9' })), 'malformed', 'requires read_plane');
});

Deno.test('geo parse: min_lsn format gate (pg_lsn text, no trim, <=8 hex halves)', () => {
  const ok = ['0/9', '0/1A2B3C4D', 'FFFFFFFF/FFFFFFFF', 'ab/CD', '0/0'];
  for (const v of ok) {
    const req = parseWhEngineRequest(baseReq({ read_plane: 'replica', min_lsn: v }));
    eqTrue(`accept ${v}`, req.min_lsn === v);
  }
  const bad = [' 0/9', '0/9 ', '0/1A2B3C4D5', '/9', '0/', '0/ZZ', '0/9/1', 9, null === undefined ? '' : '0x/9', ''];
  for (const v of bad) {
    throwsEngine(`reject ${JSON.stringify(v)}`, () => parseWhEngineRequest(baseReq({ read_plane: 'replica', min_lsn: v })), 'malformed');
  }
});

Deno.test('geo parse: min_lsn null = absent (null-lenient precedent)', () => {
  const req = parseWhEngineRequest(baseReq({ read_plane: 'replica', min_lsn: null }));
  eqTrue('min_lsn dropped', req.min_lsn === undefined);
});

// ---------- LSN algebra (BigInt; the lexicographic traps) ----------

Deno.test('geo lsn: regex pins', () => {
  eqTrue('valid', PG_LSN_RE.test('0/1A2B3C4D'));
  eqTrue('too many digits', !PG_LSN_RE.test('0/1A2B3C4D5'));
  eqTrue('empty half', !PG_LSN_RE.test('/9'));
});

Deno.test('geo lsn: TRAP 1 — text says behind, numeric says ahead (0/1F vs 0/9)', () => {
  eqTrue("text '0/1F' < '0/9'", '0/1F' < '0/9'); // the trap is real
  eqTrue('numeric 0x1F > 0x9', lsnAtLeast('0/1F', '0/9') === true);
});

Deno.test('geo lsn: TRAP 2 — text says ahead, numeric says behind (0/9 vs 0/A)', () => {
  eqTrue("text '0/9' > '0/1A'", '0/9' > '0/1A'); // the trap is real ('9' > '1' in text; '9' < 'A' in ASCII — use the 1A form)
  eqTrue('numeric 0x9 < 0xA', lsnAtLeast('0/A', '0/9') === true && lsnAtLeast('0/9', '0/A') === false);
});

Deno.test('geo lsn: hi-half dominance + boundary + unadjudicable cells', () => {
  eqTrue('hi dominates lo', lsnAtLeast('1/0', '0/FFFFFFFF') === true);
  eqTrue('equal passes (>= semantics)', lsnAtLeast('0/10', '0/10') === true);
  eqTrue('max vs min', lsnAtLeast('FFFFFFFF/FFFFFFFF', '0/0') === true);
  eqTrue('null applied => null', lsnAtLeast(null, '0/9') === null);
  eqTrue('unparseable applied => null', lsnAtLeast('xyz', '0/9') === null);
  eqTrue('unparseable min => null', lsnAtLeast('0/9', '0/9/') === null);
});

Deno.test('geo lsn: parsePgLsn returns BigInt halves, fails closed on junk', () => {
  const p = parsePgLsn('AB/CD');
  eqTrue('parses', p !== null && p[0] === 0xABn && p[1] === 0xCDn);
  eqTrue('junk null', parsePgLsn('no-slash') === null);
  eqTrue('number null', parsePgLsn(9) === null);
});

// ---------- coverage gate (full-only law) ----------

Deno.test('geo coverage: null => is_reference only; false/null ref => miss (fail closed)', () => {
  eqTrue('ref true covers', geoCoverageCovers(null, 'dims_x', true));
  eqTrue('ref false misses', !geoCoverageCovers(null, 'facts', false));
  eqTrue('ref unknown misses', !geoCoverageCovers(null, 'dims_x', null));
});

Deno.test('geo coverage: array = exact full-table set; other shapes => empty (fail closed)', () => {
  eqTrue('member covered', geoCoverageCovers(['orders', 'dims_x'], 'orders', false));
  eqTrue('non-member missed', !geoCoverageCovers(['dims_x'], 'orders', true));
  for (const bad of [{ orders: 'full' }, 'orders', 7, true, [['orders']]]) {
    eqTrue(`shape ${show(bad).slice(0, 30)} => empty`, !geoCoverageCovers(bad, 'orders', true));
  }
});

// ---------- geo reader (limit(2) tripwire; loud mapping) ----------

Deno.test('geo reader: maps rows; >1 row => law-breakage throw (never pick one)', async () => {
  const okReader = makeGeoDirectoryReader({
    fetchGeo: async () => ({ data: [geoRow()], error: null }),
  });
  const rows = await okReader.readGeoDirectory();
  eqTrue('one row mapped', rows.length === 1 && rows[0].project_ref === 'fmhostref');
  const tripwire = makeGeoDirectoryReader({
    fetchGeo: async () => ({ data: [geoRow(), geoRow({ project_ref: 'other' })], error: null }),
  });
  let threw = '';
  try {
    await tripwire.readGeoDirectory();
  } catch (e) {
    threw = (e as Error).message;
  }
  eqTrue('tripwire fired', threw.includes('one_geo_replica_serving'));
});

Deno.test('geo reader: error propagates (engine degrades); missing project_ref throws loud', async () => {
  const errReader = makeGeoDirectoryReader({ fetchGeo: async () => ({ data: null, error: { message: 'boom' } }) });
  let threw = '';
  try {
    await errReader.readGeoDirectory();
  } catch (e) {
    threw = (e as Error).message;
  }
  eqTrue('error propagated', threw.includes('boom'));
  let mapped = '';
  try {
    toWhGeoDirectoryRow({ state: 'serving' });
  } catch (e) {
    mapped = (e as Error).message;
  }
  eqTrue('missing ref throws', mapped.includes('project_ref'));
});

// ---------- deriveTemplateHashes (engine-derived, manifest-bounded) ----------

Deno.test('geo derive: wh_probe_agg grouped plan derives W1; facts_blocks scalar derives W5', () => {
  const w1 = deriveTemplateHashes(
    { table: 'wh_probe_agg', groupKeys: ['region'], aggs: { s: { op: 'sum', col: 'amount' }, c: { op: 'count' } } },
    1,
  );
  eq('W1 only for wh_probe_agg grouped', w1, [ENGINE_TEMPLATE_MANIFEST[0].template_hash]);
  const w5 = deriveTemplateHashes({ table: 'facts_blocks', aggs: { s: { op: 'sum', col: 'value' }, c: { op: 'count', col: 'value' } } }, 1);
  eq('W5 for facts_blocks', w5, [ENGINE_TEMPLATE_MANIFEST[4].template_hash]);
});

Deno.test('geo derive: KIND matching — scalar sum/count does NOT ride grouped W1', () => {
  // The battery catch: subset-only matching over-matched a SCALAR sum/count
  // plan onto W1 (a GROUPED template, merge_ops [groupby,sum,count] ⊇ {sum,
  // count} vacuously through the missing groupby). Kind pins the shape.
  const scalarSumCount = deriveTemplateHashes({ table: 'wh_probe_agg', aggs: { s: { op: 'sum', col: 'amount' }, c: { op: 'count' } } }, 1);
  eqTrue('scalar sum/count => empty (W1 is grouped; no scalar sum/count template)', scalarSumCount.length === 0);
  // Vacuous-subset cell: an aggregate-less scalar plan ({aggs:{}}) has an
  // EMPTY plan-op set — a subset of EVERYTHING (F14 law, working as pinned);
  // W4 (scalar? NO — W4 kind is 'rows' with empty group_keys)...
  eqTrue('W4 is kind rows', ENGINE_TEMPLATE_MANIFEST[3].kind === 'rows');
  // ...so the scalar empty plan matches NO row (W2/W3/W5 are scalar but have
  // real op requirements; W1/W4 are rows-kind).
  eqTrue('scalar empty plan still excludes rows-kind W1/W4', !deriveTemplateHashes({ table: 'wh_probe_agg', aggs: {} }, 1).some((h) => h === ENGINE_TEMPLATE_MANIFEST[0].template_hash || h === ENGINE_TEMPLATE_MANIFEST[3].template_hash));
  // Vacuous subset, scalar side: the aggregate-less scalar plan's EMPTY
  // plan-op set is a subset of EVERYTHING — W2 [min,max,count_col] and
  // W3 [avg_pair] both derive. Unreachable through parse (select must be
  // non-empty); pinned to DOCUMENT the vacuous-subset behavior of F14.
  const vac = deriveTemplateHashes({ table: 'wh_probe_agg', aggs: {} }, 1);
  eq('vacuous empty scalar plan derives W2+W3', vac, [ENGINE_TEMPLATE_MANIFEST[1].template_hash, ENGINE_TEMPLATE_MANIFEST[2].template_hash]);
  // A GROUPED plan with empty ops (degenerate) would vacuously match W1+W4 —
  // unreachable through parse (select non-empty), pinned for the algebra.
  const v2 = deriveTemplateHashes(
    { table: 'wh_probe_agg', groupKeys: ['region'], aggs: { s: { op: 'sum', col: 'amount' }, c: { op: 'count' } } },
    2,
  );
  eqTrue('schema_version 2 filters W1 (manifest pins 1)', v2.length === 0);
  eqTrue('unknown table => empty', deriveTemplateHashes({ table: 'nope', aggs: {} }, 1).length === 0);
  // count(col) => count_col token; W2 declares min/max/count_col.
  const w2 = deriveTemplateHashes({ table: 'wh_probe_agg', aggs: { m: { op: 'min', col: 'amount' }, c: { op: 'count', col: 'amount' } } }, 1);
  eq('W2 for min+count(col) scalar', w2, [ENGINE_TEMPLATE_MANIFEST[1].template_hash]);
});

// ---------- executeWhQuery: the gate ladder (G0..G7) ----------

async function runWith(req: Record<string, unknown>, extra: Record<string, unknown>, calls: { shard: string; url: string }[], opts: { schemaVersion?: number; fetchThrows?: boolean } = {}) {
  const fetcher: WhShardFetcher = async (shard, url) => {
    calls.push({ shard, url });
    if (opts.fetchThrows) throw new Error('fetch exploded');
    return { ok: true as const, envelope: scalarEnv(shard, 'orders', opts.schemaVersion ?? 3) };
  };
  return executeWhQuery({
    ...(BASE_ARGS as unknown as Record<string, unknown>),
    req: parseWhEngineRequest(req),
    fetcher,
    ...extra,
  } as never);
}

function fallbackArgs(detail: string): Record<string, unknown> {
  return { geoMode: 'spread', geoReader: () => Promise.resolve([geoRow()]), __detail: detail };
}

Deno.test('geo exec G1: mode colocated => primary fallback with warning + detail', async () => {
  const calls: { shard: string; url: string }[] = [];
  const res = await runWith(baseReq({ read_plane: 'replica' }), { geoMode: 'colocated', geoReader: () => Promise.resolve([geoRow()]) }, calls);
  eqTrue('served primary shards', res.perShard.length === 3 && res.coverage === '3/3');
  eq('fallback warning', res.warnings[0], { shard: 'geo_replica', code: 'geo_fallback_primary', est_rows: 0, retried: false, detail: 'mode_colocated' });
});

Deno.test('geo exec G1b: unreadable mode (null) fails closed; primary plane never consults geo deps', async () => {
  const calls: { shard: string; url: string }[] = [];
  let geoReads = 0;
  const res = await runWith(baseReq({ read_plane: 'replica' }), { geoMode: null, geoReader: () => { geoReads++; return Promise.resolve([geoRow()]); } }, calls);
  eqTrue('fallback', res.warnings[0]?.code === 'geo_fallback_primary' && res.warnings[0]?.detail === 'mode_colocated');
  eqTrue('geo reader untouched after mode gate', geoReads === 0);
  const calls2: { shard: string; url: string }[] = [];
  let consulted = 0;
  await runWith(baseReq(), { geoMode: 'spread', geoReader: () => { consulted++; return Promise.resolve([geoRow()]); } }, calls2);
  eqTrue('primary plane zero geo consults', consulted === 0);
});

Deno.test('geo exec G2: geo read throws / dep absent => degrade-not-throw fallback', async () => {
  const calls: { shard: string; url: string }[] = [];
  const res = await runWith(baseReq({ read_plane: 'replica' }), { geoMode: 'spread', geoReader: () => Promise.reject(new Error('down')) }, calls);
  eqTrue('throwing reader degrades', res.warnings[0]?.detail === 'geo_read_error' && res.coverage === '3/3');
  const calls2: { shard: string; url: string }[] = [];
  const res2 = await runWith(baseReq({ read_plane: 'replica' }), { geoMode: 'spread' }, calls2); // no reader dep
  eqTrue('absent reader degrades', res2.warnings[0]?.detail === 'geo_read_error');
});

Deno.test('geo exec G3: draining-only => state_draining; empty view => no_serving_row', async () => {
  const calls: { shard: string; url: string }[] = [];
  const res = await runWith(baseReq({ read_plane: 'replica' }), {
    geoMode: 'spread',
    geoReader: () => Promise.resolve([geoRow({ state: 'draining' })]),
  }, calls);
  eqTrue('draining detail + ref echo', res.warnings[0]?.detail === 'state_draining' && res.warnings[0]?.shard === 'fmhostref');
  const res2 = await runWith(baseReq({ read_plane: 'replica' }), { geoMode: 'spread', geoReader: () => Promise.resolve([]) }, calls);
  eqTrue('empty detail, RESERVED pseudo-shard token', res2.warnings[0]?.detail === 'no_serving_row' && res2.warnings[0]?.shard === 'geo_replica');
});

Deno.test('geo exec G4: coverage miss on non-reference table + unknown ref; array hit passes', async () => {
  const calls: { shard: string; url: string }[] = [];
  const res = await runWith(baseReq({ read_plane: 'replica' }), {
    geoMode: 'spread',
    geoReader: () => Promise.resolve([geoRow({ coverage: null })]),
    resolveTableReference: () => Promise.resolve(false),
  }, calls);
  eqTrue('coverage_miss on non-ref', res.warnings[0]?.detail === 'coverage_miss');
  const res2 = await runWith(baseReq({ read_plane: 'replica' }), {
    geoMode: 'spread',
    geoReader: () => Promise.resolve([geoRow({ coverage: null })]),
    resolveTableReference: () => Promise.resolve(true),
  }, calls);
  eqTrue('reference table rides replica', res2.coverage === '1/1' && res2.perShard[0]?.shard === 'fmhostref');
  const res3 = await runWith(baseReq({ read_plane: 'replica' }), {
    geoMode: 'spread',
    geoReader: () => Promise.resolve([geoRow({ coverage: ['other_table'] })]),
  }, calls);
  eqTrue('array miss fails closed', res3.warnings[0]?.detail === 'coverage_miss');
  const res4 = await runWith(baseReq({ read_plane: 'replica' }), {
    geoMode: 'spread',
    geoReader: () => Promise.resolve([geoRow({ coverage: null })]),
    resolveTableReference: () => Promise.reject(new Error('x')), // resolver throws => fail closed
  }, calls);
  eqTrue('throwing resolver fails closed', res4.warnings[0]?.detail === 'coverage_miss');
});

Deno.test('geo exec G5: derived templateHashes non-empty => template plans never ride the replica', async () => {
  const calls: { shard: string; url: string }[] = [];
  // orders is NOT a manifest table — inject a manifest-matching template set directly.
  const res = await runWith(baseReq({ read_plane: 'replica' }), {
    geoMode: 'spread',
    geoReader: () => Promise.resolve([geoRow()]),
    templateHashes: [ENGINE_TEMPLATE_MANIFEST[0].template_hash],
  }, calls);
  eqTrue('templates fallback', res.warnings[0]?.detail === 'templates_not_replica_served' && res.coverage === '3/3');
});

Deno.test('geo exec G6: BigInt LSN gate — traps, unknown, behind, absent => replica', async () => {
  const calls: { shard: string; url: string }[] = [];
  // TRAP: applied '0/A' (=10) < floor '0/10' (=16) => BEHIND (BigInt falls
  // back). TEXT order says '0/A' > '0/10' ('A' 0x41 > '1' 0x31) — text would
  // WRONGLY SERVE a behind replica as read-your-writes. The gate is
  // applied >= min; the BigInt verdict is the opposite of the text verdict.
  const res = await runWith(baseReq({ read_plane: 'replica', min_lsn: '0/10' }), {
    geoMode: 'spread',
    geoReader: () => Promise.resolve([geoRow({ applied_lsn: '0/A' })]),
  }, calls);
  eqTrue('trap: BigInt correctly REFUSES (text would serve)', res.warnings[0]?.detail === 'lsn_behind' && res.coverage === '3/3');
  // TRAP reverse: text '0/9' > '0/1A' but 0x9 < 0x1A => behind => fallback.
  const res2 = await runWith(baseReq({ read_plane: 'replica', min_lsn: '0/1A' }), {
    geoMode: 'spread',
    geoReader: () => Promise.resolve([geoRow({ applied_lsn: '0/9' })]),
  }, calls);
  eqTrue('trap reverse: lsn_behind', res2.warnings[0]?.detail === 'lsn_behind' && res2.coverage === '3/3');
  // null applied_lsn => lsn_unknown.
  const res3 = await runWith(baseReq({ read_plane: 'replica', min_lsn: '0/9' }), {
    geoMode: 'spread',
    geoReader: () => Promise.resolve([geoRow({ applied_lsn: null })]),
  }, calls);
  eqTrue('null applied => lsn_unknown', res3.warnings[0]?.detail === 'lsn_unknown');
  // min_lsn ABSENT => replica rides (documented eventual consistency).
  const res4 = await runWith(baseReq({ read_plane: 'replica' }), {
    geoMode: 'spread',
    geoReader: () => Promise.resolve([geoRow({ applied_lsn: null })]),
  }, calls);
  eqTrue('no floor => replica rides', res4.coverage === '1/1');
  // equal LSNs pass (>= semantics).
  const res5 = await runWith(baseReq({ read_plane: 'replica', min_lsn: '0/FFFFFFFE' }), {
    geoMode: 'spread',
    geoReader: () => Promise.resolve([geoRow()]),
  }, calls);
  eqTrue('equal floor serves', res5.coverage === '1/1');
});

Deno.test('geo exec: replica target shape + coverage denominator 1 (never 1/N over primary)', async () => {
  const calls: { shard: string; url: string }[] = [];
  const res = await runWith(baseReq({ read_plane: 'replica' }), { geoMode: 'spread', geoReader: () => Promise.resolve([geoRow()]) }, calls);
  eqTrue('exactly ONE fetch', calls.length === 1);
  eqTrue('target = replica ref', calls[0].shard === 'fmhostref');
  eqTrue('URL carries the table + aggs (compileShardUrl shape)', calls[0].url.includes('/rest/v1/orders') && calls[0].url.includes('select='));
  eq('response coverage', res.coverage, '1/1');
  eqTrue('partial false', res.partial === false);
  eqTrue('directory_version = WRITE directory version (plan basis)', res.directory_version === 42);
});

Deno.test('geo exec G7: replica fetch failure = normal degrade path (0/1 partial, NO re-dispatch)', async () => {
  const calls: { shard: string; url: string }[] = [];
  const res = await runWith(baseReq({ read_plane: 'replica' }), { geoMode: 'spread', geoReader: () => Promise.resolve([geoRow()]) }, calls, { fetchThrows: true });
  eqTrue('one attempt only', calls.length === 1);
  eqTrue('coverage 0/1 partial', res.coverage === '0/1' && res.partial === true);
});

Deno.test('geo exec: ddl-wave schema gate applies on the replica plane', async () => {
  const calls: { shard: string; url: string }[] = [];
  const res = await runWith(baseReq({ read_plane: 'replica' }), {
    geoMode: 'spread',
    geoReader: () => Promise.resolve([geoRow()]),
    tableSchemaVersion: 3, // the entrypoint wires head.table_schema_version in prod
  }, calls, { schemaVersion: 2 });
  eqTrue('stale replica partial excluded', res.warnings[0]?.code === 'schema_mismatch' && res.coverage === '0/1' && res.partial === true);
});

Deno.test('geo exec G-A: tier_warm is PLANE-INVARIANT (fires before the geo gates)', async () => {
  const calls: { shard: string; url: string }[] = [];
  let geoReads = 0;
  try {
    await executeWhQuery({
      ...(BASE_ARGS as unknown as Record<string, unknown>),
      directoryRows: [],
      req: parseWhEngineRequest(baseReq({ read_plane: 'replica' })),
      fetcher: recordingFetcher(calls),
      geoMode: 'spread',
      geoReader: () => { geoReads++; return Promise.resolve([geoRow()]); },
    } as never);
    throw new Error('expected tier_warm');
  } catch (e) {
    eqTrue('404 tier_warm', e instanceof WhEngineError && e.code === 'tier_warm');
  }
  eqTrue('geo gates never consulted after the 404', geoReads === 0);
});

Deno.test('geo exec (r121 folded): handshake skipped on a won replica plane AND on the R3 dispatch; the primary samples ONLY in-bucket', async () => {
  const calls: { shard: string; url: string }[] = [];
  let inventoryReads = 0;
  const handshake = { readTemplateInventory: () => { inventoryReads++; return Promise.resolve([]); } };
  // A won replica plane NEVER samples (plane guard `!onReplicaPlane`) — even
  // with an in-bucket qid + limit. (A TEMPLATED plan can never WIN the
  // replica — G5 — so on a won replica the empty-hash guard stacks on top;
  // the plane guard stays load-bearing defense-in-depth for it.)
  await runWith(baseReq({ read_plane: 'replica', qid: 'skip-me', query: { select: [{ op: 'sum', col: 'amount', alias: 's' }, { op: 'count', alias: 'c' }], limit: 10 } }), {
    geoMode: 'spread',
    geoReader: () => Promise.resolve([geoRow()]),
    handshake,
  }, calls);
  eqTrue('no inventory reads on a won replica plane (in-bucket qid — the plane guard dominates)', inventoryReads === 0);
  eqTrue('the won replica serves normally (one fetch, 1/1)', calls.length === 1 && calls[0]?.shard === 'fmhostref');
  // The R3 geoDispatch target NEVER samples either (`geoDispatch === null` guard).
  inventoryReads = 0;
  const callsEd: { shard: string; url: string }[] = [];
  await executeWhQuery({
    ...(BASE_ARGS as unknown as Record<string, unknown>),
    req: parseWhEngineRequest(baseReq({ qid: 'skip-me', query: { select: [{ op: 'sum', col: 'amount', alias: 's' }, { op: 'count', alias: 'c' }], limit: 10 } })),
    fetcher: async (shard: string) => { callsEd.push({ shard, url: '' }); return { ok: true as const, envelope: scalarEnv(shard, 'orders') }; },
    handshake,
    templateHashes: [ENGINE_TEMPLATE_MANIFEST[0].template_hash],
    geoReadDispatch: { kind: 'remote', target: 'remote-ref' },
  } as never);
  eqTrue('no inventory reads on the R3 dispatch plane (in-bucket qid — the geoDispatch guard dominates)', inventoryReads === 0);
  eqTrue('the R3 dispatch target IS fetched (single-target serve)', callsEd.length === 1 && callsEd[0]?.shard === 'remote-ref');
  // PRIMARY plane, SAMPLED-FORCED (in-bucket qid + limit + templated plan):
  // the fired sweep runs one inventory GET per selected shard and the empty
  // inventory fails every shard closed BEFORE any POST.
  inventoryReads = 0;
  const callsS: { shard: string; url: string }[] = [];
  const resS = await executeWhQuery({
    ...(BASE_ARGS as unknown as Record<string, unknown>),
    req: parseWhEngineRequest(baseReq({ qid: 'skip-me', query: { select: [{ op: 'sum', col: 'amount', alias: 's' }, { op: 'count', alias: 'c' }], limit: 10 } })),
    fetcher: async (shard: string) => { callsS.push({ shard, url: '' }); return { ok: true as const, envelope: scalarEnv(shard, 'orders') }; },
    handshake,
    templateHashes: [ENGINE_TEMPLATE_MANIFEST[0].template_hash],
  } as never);
  eqTrue('SAMPLED primary: inventory reads happen per primary shard (one per shard, per fired sweep)', inventoryReads === 3);
  eqTrue('SAMPLED primary: template_missing excludes BEFORE dispatch (zero fetches)', callsS.length === 0);
  eqTrue('SAMPLED primary: coverage 0/3 partial', resS.coverage === '0/3' && resS.partial === true);
  // PRIMARY plane, UNSAMPLED (the fixture qid, bucket 14): the fold's steady
  // state — zero inventory reads, the fan-out proceeds untouched.
  inventoryReads = 0;
  const callsU: { shard: string; url: string }[] = [];
  await runWith(baseReq(), { handshake, templateHashes: [ENGINE_TEMPLATE_MANIFEST[0].template_hash] }, callsU);
  eqTrue('UNSAMPLED primary: ZERO inventory GETs (folded steady state)', inventoryReads === 0);
  eqTrue('UNSAMPLED primary: the fan-out proceeded (3 fetches, 3/3)', callsU.length === 3);
});

Deno.test('geo exec: fail_fast still collects-then-fails on the replica plane', async () => {
  const calls: { shard: string; url: string }[] = [];
  try {
    await executeWhQuery({
      ...(BASE_ARGS as unknown as Record<string, unknown>),
      req: parseWhEngineRequest(baseReq({ read_plane: 'replica', coverage_mode: 'fail_fast' })),
      fetcher: async (shard) => { calls.push({ shard, url: '' }); throw new Error('down'); },
      geoMode: 'spread',
      geoReader: () => Promise.resolve([geoRow()]),
    } as never);
    throw new Error('expected internal');
  } catch (e) {
    eqTrue('fail_fast 5xx on replica fetch failure', e instanceof WhEngineError && e.code === 'internal');
  }
});

// ---------- r48 P3-5 pins: pruned-empty-selection × the read plane ----------
// Shared fixture: a 2-shard range fleet + a where-clause that prunes BOTH
// shards to zero (the E11 'legit empty' class). Hand-computed: the predicate
// upper bound '2025-06-30' < S1.key_min '2026-01-01' ⇒ hi < min on every row
// ⇒ selectShards(...)[keyPreds] = [] — the PRIMARY selection is empty while
// the geo plane is untouched by span pruning (whole-table coverage).
const PRUNED_FLEET = [
  { ...dirRow('S1'), key_min: '2026-01-01', key_max: '2026-06-30' },
  { ...dirRow('S2'), key_min: '2026-07-01', key_max: '2026-12-31' },
];
const PRUNE_WHERE = [{ col: 'created_at', op: 'lt', value: '2025-06-30' }];

Deno.test('geo exec P3-5a: replica wins while the primary selection prunes to 0 (denominator law)', async () => {
  // Span pruning is a PRIMARY-plane concept (it reads key_min/key_max
  // spans); the replica plane is whole-table coverage. A where-clause that
  // prunes the primary fleet to ZERO must not leak into the replica plane:
  // the replica still wins, the dispatch is exactly ONE fetch, and coverage
  // is honest over the DISPATCHED denominator (1) — the pre-r47 partial bug
  // read selected.length and would report coverage '1/0' here. Expected:
  // targets=[replicaTarget] regardless of selected (single-dispatch law),
  // responded 1 / denominator 1 ⇒ '1/1', partial false, replica envelope
  // merged through the wire ({s:'100', c:4} from the stub scalarEnv).
  const calls: { shard: string; url: string }[] = [];
  const res = await executeWhQuery({
    ...(BASE_ARGS as unknown as Record<string, unknown>),
    directoryRows: PRUNED_FLEET,
    shardKeyColumn: 'created_at',
    shardKeyType: 'range',
    req: parseWhEngineRequest(baseReq({
      read_plane: 'replica',
      query: { select: [{ op: 'sum', col: 'amount', alias: 's' }, { op: 'count', alias: 'c' }], where: PRUNE_WHERE },
    })),
    fetcher: recordingFetcher(calls),
    geoMode: 'spread',
    geoReader: () => Promise.resolve([geoRow()]),
  } as never);
  eqTrue('exactly ONE fetch — the replica (the pruned primary dispatches nothing)', calls.length === 1 && calls[0].shard === 'fmhostref');
  eq('coverage over the DISPATCHED denominator (1), never the pruned selection', res.coverage, '1/1');
  eqTrue('partial false (a selected.length mutant reports coverage 1/0)', res.partial === false);
  // merged through the wire ({s:100n, c:4} — the stub scalarEnv's sum '100'
  // finalizes to BIGINT 100n per the >2^53 law; count to number 4).
  eqTrue('replica data actually merged through the wire (numeric sum finalizes bigint 100n)', deepEq(res.result, { s: 100n, c: 4 }));
  eqTrue('a won replica plane is silent (no warnings)', res.warnings.length === 0);
  eqTrue('perShard names the replica, ok', res.perShard.length === 1 && res.perShard[0]?.shard === 'fmhostref' && res.perShard[0]?.ok === true);
});

Deno.test('geo exec P3-5b: geo fallback rides the fail-closed EMPTY path — warning carried, ZERO fetches', async () => {
  // Fallback + empty selection: the mode gate refuses the replica AND the
  // primary selection pruned to zero ⇒ emptyResponse is the exit. The
  // fail-closed empty result is NOT silent — the geo_fallback_primary
  // warning (RESERVED 'geo_replica' shard token, G1's mode_colocated
  // detail, est_rows 0) rides the empty response, and NOTHING is
  // dispatched: no replica probe (G1 precedes any geo dispatch), no primary
  // fanout (an empty population dispatches no shards). Expected:
  // coverage '0/0', ratio 1, partial false, perShard [], calls 0.
  const calls: { shard: string; url: string }[] = [];
  const res = await executeWhQuery({
    ...(BASE_ARGS as unknown as Record<string, unknown>),
    directoryRows: PRUNED_FLEET,
    shardKeyColumn: 'created_at',
    shardKeyType: 'range',
    req: parseWhEngineRequest(baseReq({
      read_plane: 'replica',
      query: { select: [{ op: 'sum', col: 'amount', alias: 's' }, { op: 'count', alias: 'c' }], where: PRUNE_WHERE },
    })),
    fetcher: recordingFetcher(calls),
    geoMode: 'colocated', // G1 fires before any geo read
    geoReader: () => Promise.resolve([geoRow()]),
  } as never);
  eq('warning enum token on the empty path (RESERVED shard + mode detail)', res.warnings[0], {
    shard: 'geo_replica',
    code: 'geo_fallback_primary',
    est_rows: 0,
    retried: false,
    detail: 'mode_colocated',
  });
  eqTrue('ZERO fetches dispatched (fail-closed empty path)', calls.length === 0);
  eq('empty coverage 0/0', res.coverage, '0/0');
  eqTrue('partial false — nothing dispatched, nothing partial', res.partial === false);
  eqTrue('perShard empty', res.perShard.length === 0);
  eqTrue('coverage_ratio honest at the empty denominator', res.coverage_ratio === 1);
});

Deno.test('geo exec P3-6 pin: scalar {W2} plan — primary handshake LIVE, replica plane never rides templates', async () => {
  // Post-flip contract pin (r47 re-audit P3-6; the wh-contract r47 errata is
  // normative): deriveTemplateHashes is KIND-matched, so a SCALAR
  // min/count(col) wh_probe_agg plan — a SELECT-path query (v0 has no RPC
  // dispatch) — derives {W2}; the errata law "a non-empty set ⟺ the query
  // rides the wh_query RPC branch". Pinned here:
  //   (i)  on the PRIMARY plane the §5.2 handshake CONSUMES the derived
  //        {W2}: an inventory lacking W2 degrades every shard
  //        template_missing BEFORE any fetch (fail-safe — the r44 handshake
  //        is LIVE for scalar aggregates by design, not vacuous), and an
  //        inventory satisfying {W2} opens the gate (the select path serves
  //        through the same wire — transport not observable per the errata);
  //   (ii) the REPLICA plane never runs the handshake: a {W2}-derived plan
  //        cannot even WIN the replica (G5 veto — template plans never ride
  //        the replica; DDL is NOT replicated), so the replica-plane
  //        equivalent request falls forward, the replica ref is NEVER
  //        fetched, and the fallback primary serve rides the SAME handshake
  //        gate (a won replica plane would skip the inventory round-trip
  //        entirely — engine guard `handshake !== undefined && !onReplica
  //        Plane`).
  // Geo fixture note: coverage must COVER the queried table (['wh_probe_agg'])
  // so the ladder reaches G5 — with the default ['orders'] row G4 would
  // fire first (coverage_miss), which is a different (already-pinned) cell.
  const w2 = deriveTemplateHashes(
    { table: 'wh_probe_agg', aggs: { m: { op: 'min', col: 'amount' }, c: { op: 'count', col: 'amount' } } },
    1,
  );
  eq('derive: scalar min/count(col) wh_probe_agg (select-path query) => {W2} (kind-matched)', w2, [ENGINE_TEMPLATE_MANIFEST[1].template_hash]);
  const scalarAggQuery = { select: [{ op: 'min', col: 'amount', alias: 'm' }, { op: 'count', col: 'amount', alias: 'c' }] };
  // r121: the sampled backstop requires limitK present — the sampled-forced
  // arms add limit 10 (≤ the manifest max_rows 1000, so the manifest
  // pre-refusal never fires) and the IN-BUCKET qid 'skip-me' (bucket 7).
  const scalarAggQuerySampled = { ...scalarAggQuery, limit: 10 };
  const w2Envelope = (shard: string): WhPartialEnvelope => ({
    v: 1,
    shard,
    table: 'wh_probe_agg',
    schema_version: 3,
    partial: {
      kind: 'scalar',
      aggs: { m: { op: 'min', col: 'amount' }, c: { op: 'count', col: 'amount' } },
      rows: [{ k: [], a: { m: '5', c: 2 } }],
      rowCount: 1,
      more: false,
    },
  });
  const w2Inventory = [{
    template_hash: ENGINE_TEMPLATE_MANIFEST[1].template_hash,
    qc_class: 'A',
    logical_table: 'wh_probe_agg',
    schema_version: 1,
    state: 'active',
    max_rows: 1000,
  }];

  // (i) SAMPLED-FORCED: the gate REFUSES on a {W2}-less inventory — per-shard
  // handshake runs, zero fetches, 0/3 partial (selected = 3, all excluded
  // pre-dispatch). PLUS the unsampled steady-state arm: zero inventory reads,
  // the fan-out proceeds untouched (the reactive fold, audit-B F5 accepted).
  const callsNoW2: { shard: string; url: string }[] = [];
  let inventoryReadsNoW2 = 0;
  const resNoW2 = await executeWhQuery({
    ...(BASE_ARGS as unknown as Record<string, unknown>),
    req: parseWhEngineRequest(baseReq({ table: 'wh_probe_agg', qid: 'skip-me', query: scalarAggQuerySampled })),
    fetcher: recordingFetcher(callsNoW2),
    handshake: { readTemplateInventory: () => { inventoryReadsNoW2++; return Promise.resolve([]); } },
    templateHashes: w2,
  } as never);
  eqTrue('handshake ran per primary shard on the FIRED sweep (the scalar plan GATES — not vacuous)', inventoryReadsNoW2 === 3);
  eqTrue('template_missing excludes BEFORE dispatch (zero fetches)', callsNoW2.length === 0);
  eq('coverage 0/3 partial', [resNoW2.coverage, resNoW2.partial], ['0/3', true]);
  eqTrue('every shard excluded template_missing', resNoW2.perShard.length === 3 && resNoW2.perShard.every((p) => p.ok === false && p.error === 'template_missing'));

  const callsNoW2Unsampled: { shard: string; url: string }[] = [];
  let inventoryReadsNoW2Unsampled = 0;
  const resNoW2Unsampled = await executeWhQuery({
    ...(BASE_ARGS as unknown as Record<string, unknown>),
    req: parseWhEngineRequest(baseReq({ table: 'wh_probe_agg', query: scalarAggQuery })), // bucket-14 qid, no limit
    fetcher: async (shard: string) => { callsNoW2Unsampled.push({ shard, url: '' }); return { ok: true as const, envelope: w2Envelope(shard) }; },
    handshake: { readTemplateInventory: () => { inventoryReadsNoW2Unsampled++; return Promise.resolve([]); } },
    templateHashes: w2,
  } as never);
  eqTrue('UNSAMPLED: ZERO inventory reads (folded steady state)', inventoryReadsNoW2Unsampled === 0);
  eqTrue('UNSAMPLED: the {W2}-less inventory is never consulted — the fan-out rides (3 fetches, 3/3)', callsNoW2Unsampled.length === 3 && resNoW2Unsampled.coverage === '3/3');

  // (i-b) SAMPLED-FORCED: the gate OPENS when the inventory satisfies {W2}:
  // the plan fans out on the select path and the scalar aggregate merges
  // ({m:'5', c:6} = min of 5/5/5, 2+2+2 counts).
  const callsOpen: { shard: string; url: string }[] = [];
  let inventoryReadsOpen = 0;
  const resOpen = await executeWhQuery({
    ...(BASE_ARGS as unknown as Record<string, unknown>),
    req: parseWhEngineRequest(baseReq({ table: 'wh_probe_agg', qid: 'skip-me', query: scalarAggQuerySampled })),
    fetcher: async (shard: string) => { callsOpen.push({ shard, url: '' }); return { ok: true as const, envelope: w2Envelope(shard) }; },
    handshake: { readTemplateInventory: () => { inventoryReadsOpen++; return Promise.resolve(w2Inventory); } },
    templateHashes: w2,
  } as never);
  eqTrue('eligible inventory: handshake ran, then the plan fanned out', inventoryReadsOpen === 3 && callsOpen.length === 3);
  eq('coverage 3/3', resOpen.coverage, '3/3');
  // merged through the wire: min-of-'5's ⇒ finalized numeric min is BIGINT
  // 5n (the >2^53 law — sums/mins finalize bigint, never Number), count 6.
  eqTrue('scalar aggregate merged through the select path (numeric min finalizes bigint 5n)', deepEq(resOpen.result, { m: 5n, c: 6 }));

  // (ii) SAMPLED-FORCED, the REPLICA-plane equivalent: G5 vetoes the {W2}
  // plan — the replica ref is never fetched; the FALLBACK primary serve
  // rides the same sampled gate (open inventory ⇒ a normal served wire + the
  // geo warning; the replica plane itself never consults the derived set
  // except as this veto, and a WON replica plane would never sample).
  const callsRep: { shard: string; url: string }[] = [];
  let inventoryReadsRep = 0;
  const resRep = await executeWhQuery({
    ...(BASE_ARGS as unknown as Record<string, unknown>),
    req: parseWhEngineRequest(baseReq({ table: 'wh_probe_agg', read_plane: 'replica', qid: 'skip-me', query: scalarAggQuerySampled })),
    fetcher: async (shard: string) => { callsRep.push({ shard, url: '' }); return { ok: true as const, envelope: w2Envelope(shard) }; },
    handshake: { readTemplateInventory: () => { inventoryReadsRep++; return Promise.resolve(w2Inventory); } },
    templateHashes: w2,
    geoMode: 'spread',
    geoReader: () => Promise.resolve([geoRow({ coverage: ['wh_probe_agg'] })]),
  } as never);
  eq('G5 veto: geo_fallback_primary / templates_not_replica_served with the KNOWN replica ref', resRep.warnings[0], {
    shard: 'fmhostref',
    code: 'geo_fallback_primary',
    est_rows: 0,
    retried: false,
    detail: 'templates_not_replica_served',
  });
  eqTrue('the replica ref is NEVER fetched (template plans never ride the replica)', callsRep.length === 3 && callsRep.every((c) => c.shard !== 'fmhostref'));
  eqTrue('handshake happens on the FALLBACK primary serve only (a won replica plane skips it entirely)', inventoryReadsRep === 3);
  eq('fallback primary serve is a normal 3/3 wire', resRep.coverage, '3/3');
});

// -----------------------------------------------------------------------------
// r57 wh_ryw v1 battery (design_r53_wh_ryw_lsn_poll.md §8: T1-T3 cells +
// m1/m2/m3/m5 mutant classes; T4 stays 0018-gated — it lands WITH the v2
// live-poll RPC or never). v1 = the as-built G6 stamped compare, degenerate
// lsn-poll (one sample, zero wait, BigInt floor). Existing lethal pins from
// the r47 census are NOT re-written (T1a's ladder cell lives in 'geo exec G6'
// above: applied 0/9 vs min 0/1A => lsn_behind; m2's null-serve kill is the
// same cell's null-applied arm) — these cells add the MISSING directions.
// The live-path FLIP is a deploy-time env (WH_RYW_V1, handler level,
// wh_entrypoint_test.ts) — the core machinery below is lever-agnostic.
// -----------------------------------------------------------------------------

Deno.test('wh_ryw T1b: floor met — applied 0/1F vs min 0/9 => replica SERVES (lex-mutant false-fail direction)', async () => {
  const calls: { shard: string; url: string }[] = [];
  // Hand-computed: 0x1F = 31 >= 0x9 = 9 => BigInt verdict AHEAD => serve.
  // TEXT order says '0/1F' < '0/9' ('1' 0x31 < '9' 0x39) — the lexicographic
  // mutant (m1 swap) false-FAILS a CAUGHT-UP replica => falls forward to the
  // primary => this cell goes RED (coverage 3/3 + a warning instead of 1/1).
  const res = await runWith(baseReq({ read_plane: 'replica', min_lsn: '0/9' }), {
    geoMode: 'spread',
    geoReader: () => Promise.resolve([geoRow({ applied_lsn: '0/1F' })]),
  }, calls);
  eqTrue('T1b: BigInt admits the caught-up replica (coverage 1/1)', res.coverage === '1/1');
  eqTrue('T1b: no fallback warning (fresh verdict, zero plane events)', res.warnings.length === 0);
  eqTrue('T1b: EXACTLY ONE fetch, to the replica (single-dispatch law)', calls.length === 1 && calls[0].shard === 'fmhostref');
});

Deno.test('wh_ryw T1c: garbage min_lsn (three halves) => 400 at parse, NEVER an lsn_unknown fallback', () => {
  // Design fixture: even against a WOULD-SERVE stamp (0/FFFFFFFF = the max
  // single-hi-halves floor), an unparseable min is a PARSE-plane 400 naming
  // min_lsn — never best-effort parsed into an lsn_unknown fallback (that
  // mutant would answer a caller BUG with a primary read instead of a 400).
  // The geo row is never consulted: parse precedes any I/O.
  throwsEngine(
    'T1c: 0/1/0 rejected 400 malformed at the format gate',
    () => parseWhEngineRequest(baseReq({ read_plane: 'replica', min_lsn: '0/1/0' })),
    'malformed',
    'min_lsn',
  );
  throwsEngine(
    'T1c: the 9-hex-half overflow form rejected too (0/1A2B3C4D5)',
    () => parseWhEngineRequest(baseReq({ read_plane: 'replica', min_lsn: '0/1A2B3C4D5' })),
    'malformed',
    'min_lsn',
  );
});

Deno.test('wh_ryw m1: Number-precision swap — 2^53 vs 2^53+1 collapses under float rounding', () => {
  // Hand-computed: applied 0x200000/0x0 = 2^53 = 9007199254740992; min
  // 0x200000/0x1 = 2^53 + 1 = 9007199254740993. BigInt verdict: BEHIND
  // (2^53 < 2^53+1) => fallback. A Number/double mutant rounds 2^53+1 DOWN
  // to 2^53 (the float spacing at 2^53 is 2 — ties-to-even) => equal => '>='
  // SERVES a BEHIND replica as read-your-writes — the exact violation the
  // gate exists to prevent => this cell goes RED. The 2^53-1 boundary pair
  // (all integers <= 2^53 are float-exact, so careful half-wise float math
  // survives it — it pins the law, not a kill) and the mirrors round out the
  // class: any LSN compare not done in BigInt dies somewhere in this cell +
  // the r47 trap/hi-dominance cells above.
  eqTrue('m1: applied 2^53 vs min 2^53+1 => BEHIND (float mutant would serve)', lsnAtLeast('200000/0', '200000/1') === false);
  eqTrue('m1 boundary: applied 2^53-1 vs min 2^53 => BEHIND', lsnAtLeast('1FFFFF/FFFFFFFF', '200000/0') === false);
  eqTrue('m1 mirror: applied 2^53+1 vs min 2^53 => AHEAD (mutant must not false-fail)', lsnAtLeast('200000/1', '200000/0') === true);
  eqTrue('m1 mirror 2: applied 2^53 vs min 2^53-1 => AHEAD', lsnAtLeast('200000/0', '1FFFFF/FFFFFFFF') === true);
  eqTrue('m1 anchor: both halves are true BigInts (no Number ever touches the math)', typeof parsePgLsn('200000/0')?.[0] === 'bigint' && typeof parsePgLsn('1FFFFF/FFFFFFFF')?.[1] === 'bigint');
});

Deno.test('wh_ryw T2: forked-timeline advisory — post-fork fresh stamp + pre-fork token => lsn_unknown-class fall-forward', async () => {
  const calls: { shard: string; url: string }[] = [];
  // The promotion-fork class (design §4.4 + R-C): the re-seeded subscriber
  // has applied NOTHING on the new timeline yet (applied_lsn NULL — the only
  // engine-visible post-fork stamp form; the token's FORMAT is unchanged so
  // old-timeline tokens are indistinguishable and ADVISORY-ONLY). The gate
  // treats the incomparable token exactly like lsn_unknown: fail-closed
  // fall-forward to the CURRENT write head (the promoted primary IS the best
  // available RYW), never blocked-on, never compare-anyway.
  const res = await runWith(baseReq({ read_plane: 'replica', min_lsn: '0/1F' }), {
    geoMode: 'spread',
    geoReader: () => Promise.resolve([geoRow({ applied_lsn: null })]),
  }, calls);
  eq('T2: lsn_unknown-class fallback warning carries the known replica ref', res.warnings[0], {
    shard: 'fmhostref',
    code: 'geo_fallback_primary',
    est_rows: 0,
    retried: false,
    detail: 'lsn_unknown',
  });
  eqTrue('T2: primary served (the write head — always satisfies RYW)', res.coverage === '3/3');
  eqTrue('T2: the replica was NEVER fetched (fail-closed, single-dispatch)', calls.every((c) => c.shard !== 'fmhostref'));
  // Re-mint channel (design §4.4 "epoch visible to the client for re-mint"):
  // the DISCOVERY wire's additive write_epoch echo (geo_resolve.ts
  // assembleGeoRoute/normalizeGeoWriteEpoch, battery-pinned in
  // geo_resolve_test.ts) — the engine adds NO epoch field of its own (D6:
  // zero new wire fields; the write wave owns re-mint UX, OQ5).
});

Deno.test('wh_ryw T3: population honesty — geo_sub_r2p-fed serving row resolves identically; view drop => no_serving_row', async () => {
  // R18/R1: the geo_sub (FM-local pg_cron 60s) and geo_sub_r2p (caller-pinned
  // self-stamp) populations are DISJOINT by construction — the view yields
  // <=1 serving row. The engine is STAMPER-AGNOSTIC by law: `subscription` is
  // a passthrough column, never a filter (mutant: an engine filtering
  // subscription === 'geo_sub' false-falls-forward on the r2p row => RED).
  const callsSub: { shard: string; url: string }[] = [];
  const resSub = await runWith(baseReq({ read_plane: 'replica', min_lsn: '0/FFFFFFFE' }), {
    geoMode: 'spread',
    geoReader: () => Promise.resolve([geoRow({ subscription: 'geo_sub' })]),
  }, callsSub);
  const callsR2p: { shard: string; url: string }[] = [];
  const resR2p = await runWith(baseReq({ read_plane: 'replica', min_lsn: '0/FFFFFFFE' }), {
    geoMode: 'spread',
    geoReader: () => Promise.resolve([geoRow({ subscription: 'geo_sub_r2p' })]),
  }, callsR2p);
  eqTrue('T3a: geo_sub row serves (equal floor, >= semantics)', resSub.coverage === '1/1' && resSub.warnings.length === 0);
  eqTrue('T3b: geo_sub_r2p row serves IDENTICALLY (stamper-agnostic engine)', resR2p.coverage === '1/1' && resR2p.warnings.length === 0);
  eqTrue('T3: one fetch each, to the replica, whichever stamper fed the row', callsSub.length === 1 && callsSub[0].shard === 'fmhostref' && callsR2p.length === 1 && callsR2p[0].shard === 'fmhostref');
  // NEITHER stamper alive => the 90s view predicate drops the row => the
  // reader returns [] => no_serving_row fall-forward. This is the ENGINE-side
  // mirror; the SQL half (the actual drop) is pinned by the staleness battery
  // + the view contract, NOT the engine (OQ2: the engine never does beat
  // math). m3's view-drop-ignore mutant serves from the empty set => RED.
  const callsDrop: { shard: string; url: string }[] = [];
  const resDrop = await runWith(baseReq({ read_plane: 'replica' }), {
    geoMode: 'spread',
    geoReader: () => Promise.resolve([]),
  }, callsDrop);
  eq('T3c: dropped row => no_serving_row with the RESERVED pseudo-shard', resDrop.warnings[0], {
    shard: 'geo_replica',
    code: 'geo_fallback_primary',
    est_rows: 0,
    retried: false,
    detail: 'no_serving_row',
  });
  eqTrue('T3c: primary served, replica never fetched', resDrop.coverage === '3/3' && callsDrop.every((c) => c.shard !== 'fmhostref'));
});

Deno.test('wh_ryw m5-note: core verdict paths are the as-built G6 ladder — no wait loop, no second sample', async () => {
  // The degenerate-poll classification law (D2/D3): deadline 0, samples 1.
  // The T4 equivalence cells are 0018-gated, but the CORE half of the law is
  // pinnable now: the adjudication reads the row EXACTLY ONCE (the geoReader
  // call count is the sample count — a bolted-on wait loop would re-read).
  let reads = 0;
  const calls: { shard: string; url: string }[] = [];
  const res = await runWith(baseReq({ read_plane: 'replica', min_lsn: '0/FFFFFFFE' }), {
    geoMode: 'spread',
    geoReader: () => {
      reads++;
      return Promise.resolve([geoRow()]);
    },
  }, calls);
  eqTrue('one sample (degenerate poll: samples=1)', reads === 1);
  eqTrue('fresh verdict serves the replica', res.coverage === '1/1');
});

// -----------------------------------------------------------------------------
// Harness report (hand-rolled runner, no external deps) — the gate that makes
// assertion failures FATAL (the mutant dance proved the battery is vacuous
// without it: eq/eqTrue only bump counters).
// -----------------------------------------------------------------------------
Deno.test('__report__', () => {
  console.log(`\nwh_geo_plane_test: ${passed} assertions passed, ${failed} failed`);
  if (failed > 0) throw new Error(`${failed} assertion(s) failed`);
});
