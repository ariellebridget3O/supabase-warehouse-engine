// =============================================================================
// _shared/wh_shard_channel_test.ts — r69 CORE self-cells for the shard
// SERVICE-KEY CHANNEL + RPC REAL FETCHER + the §6.2 wire adapter.
// =============================================================================
// Scope (design_r69_shard_channel.md §5 split-cells law — core committer ⊥
// battery author): the resolver arms (D4), the fail-closed env parse (D5),
// the fetcher arms (D7/D8/D9 + §6.1 call shape), adaptWireEnvelope (F-N1),
// compileShardRpcUrl / WH_RPC_ELIGIBLE_HASHES / rpcParams (§3.2), the
// checkHandshake matchedHash delta (AM-6/OQ-8), and the rpcMode-gated D2 +
// target-construction + adaptation path in executeWhQuery (F-N4 Δ0 shape).
// The FULL §5 battery is authored later against this frozen code.
//
// Offline + pure: every transport is an injected fake; no env reads, no
// network. deno test --no-check -q --allow-env --allow-read
// =============================================================================

import {
  adaptWireEnvelope,
  classifyFetchFailure,
  compileShardRpcUrl,
  defaultWhEngineTimers,
  executeWhQuery,
  rpcParams,
  WH_RPC_ELIGIBLE_HASHES,
  WhEngineError,
} from './wh_engine_core.ts';
import type {
  WhDirectoryRow,
  WhEngineRequest,
  WhGeoDirectoryRow,
  WhGeoReadDispatch,
  WhMergePlan,
  WhRpcSpec,
  WhShardFetcher,
  WireTemplateView,
} from './wh_engine_core.ts';
import { checkHandshake, deriveTemplateHashes, ENGINE_TEMPLATE_MANIFEST } from './wh_handshake.ts';
import type { DerivePlanView, HandshakePlanRef, TemplateInventoryRow, WhShardHandshake } from './wh_handshake.ts';
import {
  makeOwnRefBypassRawFetch,
  makeProxiedRawFetch,
  makeRpcShardFetcher,
  makeShardKeyResolver,
  parseShardKeyEnv,
  parseWhProxyMapValue,
  raceWhProxyKvBoot,
  WH_PROXY_KV_BOOT_TIMEOUT_MS,
} from './wh_shard_channel.ts';
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

function includes(name: string, haystack: string, needle: string): void {
  eqTrue(name, haystack.includes(needle));
}

function neverThrows(name: string, fn: () => unknown, pin?: (out: unknown) => void): void {
  try {
    const out = fn();
    passed++;
    console.log(`  ok  ${name} (no throw)`);
    if (pin) pin(out);
  } catch (err) {
    failed++;
    console.error(`FAIL  ${name} — THREW: ${err}`);
  }
}

async function rejectsWith(name: string, fn: () => Promise<unknown>, pin: (err: WhEngineError) => void): Promise<void> {
  try {
    await fn();
    failed++;
    console.error(`FAIL  ${name} — did not throw`);
  } catch (err) {
    if (err instanceof WhEngineError) {
      passed++;
      console.log(`  ok  ${name}`);
      pin(err);
    } else {
      failed++;
      console.error(`FAIL  ${name} — threw non-WhEngineError: ${err}`);
    }
  }
}

// -----------------------------------------------------------------------------
// Fixtures — hashes pinned from db/shard-templates/manifest.json (W1-W5).
// -----------------------------------------------------------------------------
const W1H = 'a934e7e062f59cff5a856afdc7aa743ec9be11c068c7e861ea856c36b40bdbfd';
const W2H = 'a095adaa148253aee8d1cc8e976f01b3579beeea5082f3862df4a908c20b2659';
const W3H = 'bca9dd2c591ed48a0fa5367179dd5deb1d752ed9141c23e6ad53083becf8ecac';
const W4H = 'dccfc317035961a6310c31194e86a01c715f384ac8cbdc33fb00f35f27eefcee';
const W5H = '9a8e922d064539e3418fd77dbc17479fe6a9b69bafe0f182128c675ef1f95234';

/** The manifest's template defs (the adapter's second arg in production) —
 *  the kind + agg defs + merge_ops slices the adapter consumes. */
const W1_TEMPLATE: WireTemplateView = {
  kind: 'rows',
  aggs: { x: { op: 'sum', col: 'amount' }, c: { op: 'count' } },
  merge_ops: ['groupby', 'sum', 'count'],
};
const W2_TEMPLATE: WireTemplateView = {
  kind: 'scalar',
  aggs: { min: { op: 'min', col: 'amount' }, max: { op: 'max', col: 'amount' }, c: { op: 'count_col', col: 'amount' } },
  merge_ops: ['min', 'max', 'count_col'],
};
const W3_TEMPLATE: WireTemplateView = {
  kind: 'scalar',
  aggs: { s: { op: 'sum', col: 'amount' }, c: { op: 'count_col', col: 'amount' } },
  merge_ops: ['avg_pair'],
};

/** The FLAT rows-kind §6.2 wire, verbatim the seeded fn's shape
 *  (0015_wh_query_rpc.sql:389-404 — the §4 worked example). */
function w1Wire(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    v: 1,
    table: 'wh_probe_agg',
    schema_version: 1,
    qc_class: 'QC2',
    kind: 'grouped',
    groupKeys: ['region'],
    aggs: { x: { op: 'sum', col: 'amount' }, c: { op: 'count' } },
    rows: [
      { k: ['eu'], a: { x: '1200.50', c: 14 } },
      { k: ['us'], a: { x: '900.25', c: 7 } },
    ],
    rowCount: 2,
    truncated: false,
    encoding: { x: 'text', c: 'number' },
    template_hash: W1H,
    template_timeout_ms: 8000,
    latencyMs: 12,
    ...overrides,
  };
}

/** The FLAT scalar-kind §6.2 wire (0015:418-432 — the single-row body object
 *  verbatim under `partial`, no rows). */
function w3Wire(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    v: 1,
    table: 'wh_probe_agg',
    schema_version: 1,
    qc_class: 'QC2',
    kind: 'scalar',
    aggs: { s: { op: 'sum', col: 'amount' }, c: { op: 'count_col', col: 'amount' } },
    partial: { s: '1200.50', c: 14 },
    rowCount: 1,
    truncated: false,
    encoding: { s: 'text', c: 'number' },
    template_hash: W3H,
    template_timeout_ms: 8000,
    latencyMs: 9,
    ...overrides,
  };
}

interface RawCall {
  url: string;
  init: { method: string; headers: Record<string, string>; body?: string };
}

/** Recording transport fake (the fetcher's rawFetch dep). */
function fakeRaw(opts: {
  status?: number;
  body?: string;
  headers?: Record<string, string>;
  reject?: boolean;
  failText?: boolean;
  calls?: RawCall[];
}) {
  return async (url: string, init: { method: string; headers: Record<string, string>; body?: string }) => {
    opts.calls?.push({ url, init });
    if (opts.reject) throw new Error('transport down');
    const status = opts.status ?? 200;
    return {
      ok: status >= 200 && status <= 299,
      status,
      headers: { get: (name: string) => opts.headers?.[name.toLowerCase()] ?? null },
      text: async () => {
        if (opts.failText) throw new Error('text stream died');
        return opts.body ?? '';
      },
    };
  };
}

function dirRow(shard: string): WhDirectoryRow {
  return {
    shard,
    key_min: null,
    key_max: null,
    hash_slot: null,
    state: 'serving',
    platform_status: 'ACTIVE_HEALTHY',
    schema_version: 1,
    last_health_at: '2026-09-28T00:00:00.000000Z',
  };
}

function invRow(hash: string): TemplateInventoryRow {
  return {
    template_hash: hash,
    qc_class: 'QC2',
    logical_table: 'wh_probe_agg',
    schema_version: 1,
    state: 'active',
    max_rows: 1000,
  };
}

const RPC_REQ: WhEngineRequest = {
  v: 1,
  qid: 'q-rpc',
  table: 'wh_probe_agg',
  query: { select: [{ op: 'sum', col: 'amount' }, { op: 'count' }], groupBy: ['region'] },
  coverage_mode: 'best_effort',
};

const RPC_PLAN: WhMergePlan = {
  table: 'wh_probe_agg',
  groupKeys: [{ col: 'region', type: 'text' }],
  aggs: {
    'sum(amount)': { op: 'sum', col: 'amount' },
    'count(*)': { op: 'count' },
  },
};

// =============================================================================
// 1. parseShardKeyEnv — D5 fail-closed parse (AM-11: defects via onDefect).
// =============================================================================
Deno.test('r69 parseShardKeyEnv: undefined/empty => empty map, NO defect log', () => {
  const defects: string[] = [];
  eq('undefined => empty map', parseShardKeyEnv(undefined, (m) => defects.push(m)).size, 0);
  eq("'' => empty map", parseShardKeyEnv('', (m) => defects.push(m)).size, 0);
  eq('no defect fired for the normal absent state', defects.length, 0);
});

Deno.test('r69 parseShardKeyEnv: valid object => verbatim keys/values (JWTs never trimmed)', () => {
  const defects: string[] = [];
  const raw = JSON.stringify({ blnkdash1: 'eyJhbGciOi.JIUzUxMi.padded-value', ' second-ref ': '  spaced-value  ' });
  const map = parseShardKeyEnv(raw, (m) => defects.push(m));
  eq('entries verbatim (whitespace-preserved)', [...map.entries()], [
    ['blnkdash1', 'eyJhbGciOi.JIUzUxMi.padded-value'],
    [' second-ref ', '  spaced-value  '],
  ]);
  eq('no defect', defects.length, 0);
});

Deno.test('r69 parseShardKeyEnv: duplicate keys last-wins (JSON.parse law)', () => {
  const raw = '{"dup-ref":"first-key","dup-ref":"second-key"}';
  const map = parseShardKeyEnv(raw, () => {});
  eq('last-wins', map.get('dup-ref'), 'second-key');
  eq('single entry', map.size, 1);
});

Deno.test('r69 parseShardKeyEnv: unparseable JSON => whole-map reject + ONE fixed defect-class log (never value fragments, never err.message)', () => {
  const defects: string[] = [];
  const map = parseShardKeyEnv('{"blnk":"eyJhbGciOiJIUzUxMi9 oops', (m) => defects.push(m));
  eq('fail-closed empty map', map.size, 0);
  eq('exactly ONE defect line', defects.length, 1);
  includes('log names the env', defects[0] ?? '', 'WH_SHARD_KEYS');
  includes('log names the defect class', defects[0] ?? '', 'unparseable-json');
  eqTrue('log carries NO value fragment', !(defects[0] ?? '').includes('eyJhbGciOiJIUzUxMi9'));
  eqTrue('log carries NO err.message echo', !(defects[0] ?? '').includes('Unexpected'));
});

Deno.test('r69 parseShardKeyEnv: non-object JSON (array / scalar / null) => whole-map reject + one log', () => {
  for (const raw of ['["blnk"]', '"blnk"', '42', 'null', 'true']) {
    const defects: string[] = [];
    const map = parseShardKeyEnv(raw, (m) => defects.push(m));
    eq(`raw ${raw} => empty map`, map.size, 0);
    eq(`raw ${raw} => one defect`, defects.length, 1);
    includes('class not-an-object', defects[0] ?? '', 'not-an-object');
  }
});

Deno.test('r69 parseShardKeyEnv: non-string value => WHOLE-MAP reject (even with a valid sibling) + one log', () => {
  const defects: string[] = [];
  const map = parseShardKeyEnv('{"good-ref":"key-a","bad-ref":42}', (m) => defects.push(m));
  eq('whole-map reject => empty (not partial salvage)', map.size, 0);
  eq('one defect line', defects.length, 1);
  includes('class non-string-value', defects[0] ?? '', 'non-string-value');
  eqTrue('log carries NO value fragment (the 42 never echoed)', !(defects[0] ?? '').includes('42'));
});

// =============================================================================
// 2. makeShardKeyResolver — D4 resolution order (AM-8 arms).
// =============================================================================
Deno.test('r69 resolver: own-ref exact NON-EMPTY case-preserved match => ownKey', () => {
  const resolve = makeShardKeyResolver({ ownRef: 'fmhostref', ownKey: 'own-service-key', remoteKeys: new Map() });
  eq('own ref resolves with the engine OWN key (co-hosted law)', resolve('fmhostref'), 'own-service-key');
});

Deno.test('r69 resolver: own-ref CASE VARIATION is a miss => remote map (case PIN)', () => {
  const resolve = makeShardKeyResolver({
    ownRef: 'fmhostref',
    ownKey: 'own-service-key',
    remoteKeys: new Map([['FMHOSTREF', 'case-key']]),
  });
  eq('FMHOSTREF !== fmhostref => remote hit', resolve('FMHOSTREF'), 'case-key');
});

Deno.test('r69 resolver: remote hit', () => {
  const resolve = makeShardKeyResolver({
    ownRef: 'fmhostref',
    ownKey: 'own-service-key',
    remoteKeys: new Map([['blnkremote', 'remote-key']]),
  });
  eq('remote key returned verbatim', resolve('blnkremote'), 'remote-key');
});

Deno.test('r69 resolver: remote miss => WhEngineError internal shard_key_missing naming the REF', () => {
  const resolve = makeShardKeyResolver({ ownRef: 'fmhostref', ownKey: 'own', remoteKeys: new Map() });
  let caught: unknown;
  try {
    resolve('ghostref');
  } catch (e) {
    caught = e;
  }
  eqTrue('threw', caught !== undefined);
  eq('code internal', (caught as WhEngineError).code, 'internal');
  eq('message names the ref, nothing else', (caught as Error).message, 'shard_key_missing: ghostref');
});

Deno.test('r69 resolver: empty remote map (absent env) => miss throws; lookup is EXACT (no trim, no case-fold)', () => {
  const resolve = makeShardKeyResolver({
    ownRef: 'fmhostref',
    ownKey: 'own',
    remoteKeys: parseShardKeyEnv(undefined),
  });
  let msg = '';
  try {
    resolve('blnkremote');
  } catch (e) {
    msg = (e as Error).message;
  }
  eq('absent-env miss throws shard_key_missing', msg, 'shard_key_missing: blnkremote');
  // mutant pin: a padded/case-folded key in the map must NOT match a plain lookup
  const padded = makeShardKeyResolver({ ownRef: '', ownKey: 'k', remoteKeys: new Map([['blnkremote ', 'k1']]) });
  let paddedMsg = '';
  try {
    padded('blnkremote');
  } catch (e) {
    paddedMsg = (e as Error).message;
  }
  eq('no trim-match', paddedMsg, 'shard_key_missing: blnkremote');
});

Deno.test('r69 resolver: ownRef "" DISABLES the own-ref arm (remote-or-throw, AM-8)', () => {
  const withRemote = makeShardKeyResolver({ ownRef: '', ownKey: 'own', remoteKeys: new Map([['fmhostref', 'remote-for-own']]) });
  eq('own ref resolves via remoteKeys when the arm is disabled', withRemote('fmhostref'), 'remote-for-own');
  const withoutRemote = makeShardKeyResolver({ ownRef: '', ownKey: 'own', remoteKeys: new Map() });
  let msg = '';
  try {
    withoutRemote('fmhostref');
  } catch (e) {
    msg = (e as Error).message;
  }
  eq('else throws naming the ref', msg, 'shard_key_missing: fmhostref');
});

Deno.test('r69 resolver: ownKey "" => the own-ref arm THROWS shard_key_missing (never returns "")', () => {
  const resolve = makeShardKeyResolver({ ownRef: 'fmhostref', ownKey: '', remoteKeys: new Map() });
  let msg = '';
  try {
    resolve('fmhostref');
  } catch (e) {
    msg = (e as Error).message;
  }
  eq('own-ref arm fails closed', msg, 'shard_key_missing: fmhostref');
});

Deno.test('r69 resolver: a "" remote VALUE is a miss (never returns "")', () => {
  const resolve = makeShardKeyResolver({ ownRef: '', ownKey: 'own', remoteKeys: new Map([['blnkremote', '']]) });
  let msg = '';
  try {
    resolve('blnkremote');
  } catch (e) {
    msg = (e as Error).message;
  }
  eq('empty remote value => throw', msg, 'shard_key_missing: blnkremote');
});

// =============================================================================
// 3. makeRpcShardFetcher — D7/D8/D9 arms + the §6.1 call shape.
// =============================================================================
Deno.test('r69 fetcher: 200 rows-kind wire => ok + RAW envelope + estRows = rowCount', async () => {
  const wire = w1Wire();
  const fetcher = makeRpcShardFetcher({
    resolveKey: () => 'sk',
    rawFetch: fakeRaw({ body: JSON.stringify(wire) }),
  });
  const res = await fetcher('blnkremote', compileShardRpcUrl('blnkremote'), { p_template_hash: W1H, p_params: {} });
  eqTrue('ok', res.ok === true);
  eq('envelope is the RAW wire (no engine stamping)', (res as { envelope: unknown }).envelope, wire);
  eq('estRows', (res as { estRows: number }).estRows, 2);
  eqTrue('no shard field fabricated by the fetcher', !('shard' in ((res as { envelope: Record<string, unknown> }).envelope)));
});

Deno.test('r69 fetcher: 200 scalar-kind wire (partial body verbatim) => ok + estRows 1', async () => {
  const wire = w3Wire();
  const fetcher = makeRpcShardFetcher({ resolveKey: () => 'sk', rawFetch: fakeRaw({ body: JSON.stringify(wire) }) });
  const res = await fetcher('blnkremote', compileShardRpcUrl('blnkremote'), { p_template_hash: W3H, p_params: {} });
  eqTrue('ok', res.ok === true);
  eq('raw scalar wire', (res as { envelope: unknown }).envelope, wire);
  eq('estRows', (res as { estRows: number }).estRows, 1);
});

Deno.test('r69 fetcher: envelope shape faults => envelope_invalid (crash-gate, deep gates stay engine-side)', async () => {
  const faults: [string, Record<string, unknown>][] = [
    ['v !== 1', { v: 2 }],
    ['truncated non-boolean', { truncated: 'no' }],
    ['rowCount non-number', { rowCount: '9' }],
    ['rows absent on rows-kind', { rows: undefined }],
    ['kind unknown', { kind: 'banana' }],
    ['aggs not an object', { aggs: 7 }],
    ['scalar without partial', { kind: 'scalar', partial: undefined }],
  ];
  for (const [name, patch] of faults) {
    const fetcher = makeRpcShardFetcher({
      resolveKey: () => 'sk',
      rawFetch: fakeRaw({ body: JSON.stringify(w1Wire(patch)) }),
    });
    const res = await fetcher('s', 'u', { p_template_hash: W1H, p_params: {} });
    eq(`fault ${name} => envelope_invalid`, (res as { warning?: { code?: string } }).warning?.code, 'envelope_invalid');
  }
  const garbage = makeRpcShardFetcher({ resolveKey: () => 'sk', rawFetch: fakeRaw({ body: 'not json {' }) });
  const gres = await garbage('s', 'u', { p_template_hash: W1H, p_params: {} });
  eq('non-JSON 2xx body => envelope_invalid', (gres as { warning?: { code?: string } }).warning?.code, 'envelope_invalid');
});

Deno.test('r69 fetcher: byte cap is UTF-8 BYTES (TextEncoder), boundary exact both directions', async () => {
  // 'é' = 2 UTF-8 bytes / 1 UTF-16 unit: 6 units = 12 BYTES. A UTF-16-measure
  // mutant (6 <= 10) would PASS this cap — the byte law must abort.
  const over = makeRpcShardFetcher({ resolveKey: () => 'sk', rawFetch: fakeRaw({ body: 'é'.repeat(6) }), maxPartialBytes: 10 });
  const overRes = await over('s', 'u', { p_template_hash: W1H, p_params: {} });
  eq('12 bytes > cap 10 => abort_on_oversize', (overRes as { warning?: { code?: string } }).warning?.code, 'abort_on_oversize');
  const exact = makeRpcShardFetcher({ resolveKey: () => 'sk', rawFetch: fakeRaw({ body: 'é'.repeat(6) }), maxPartialBytes: 12 });
  const exactRes = await exact('s', 'u', { p_template_hash: W1H, p_params: {} });
  eq('12 bytes == cap 12 => PASSES the cap (shape gate rejects the body, not the cap)', (exactRes as { warning?: { code?: string } }).warning?.code, 'envelope_invalid');
  const oneOver = makeRpcShardFetcher({ resolveKey: () => 'sk', rawFetch: fakeRaw({ body: 'é'.repeat(6) + 'x' }), maxPartialBytes: 12 });
  const oneOverRes = await oneOver('s', 'u', { p_template_hash: W1H, p_params: {} });
  eq('13 bytes > cap 12 => abort_on_oversize', (oneOverRes as { warning?: { code?: string } }).warning?.code, 'abort_on_oversize');
});

Deno.test('r69 fetcher: DEFAULT cap 65536 — exactly 65536 bytes passes, 65537 aborts', async () => {
  const base = {
    v: 1, table: 'x', schema_version: 1, qc_class: 'QC2', kind: 'scalar',
    aggs: { c: { op: 'count' } }, partial: { c: 7 }, rowCount: 1, truncated: false,
    encoding: { c: 'number' }, template_hash: W1H, template_timeout_ms: 8000, latencyMs: 1,
  };
  const build = (target: number): string => {
    const tableLen = target - (JSON.stringify(base).length - 1);
    return JSON.stringify({ ...base, table: 'w'.repeat(tableLen) });
  };
  const exactBody = build(65536);
  eq('fixture self-check: exactly 65536 UTF-8 bytes', new TextEncoder().encode(exactBody).length, 65536);
  const exact = makeRpcShardFetcher({ resolveKey: () => 'sk', rawFetch: fakeRaw({ body: exactBody }) });
  const exactRes = await exact('s', 'u', { p_template_hash: W1H, p_params: {} });
  eqTrue('65536 passes', exactRes.ok === true);
  eq('estRows from the wire', (exactRes as { estRows: number }).estRows, 1);
  const overBody = build(65537);
  const over = makeRpcShardFetcher({ resolveKey: () => 'sk', rawFetch: fakeRaw({ body: overBody }) });
  const overRes = await over('s', 'u', { p_template_hash: W1H, p_params: {} });
  eq('65537 aborts', (overRes as { warning?: { code?: string } }).warning?.code, 'abort_on_oversize');
});

Deno.test('r69 fetcher: select-arm 2xx => shard_path_select_disabled (degrade loudly, never lie)', async () => {
  const calls: RawCall[] = [];
  const fetcher = makeRpcShardFetcher({
    resolveKey: () => 'sk',
    rawFetch: fakeRaw({ body: '[{"region":"eu"}]', calls }),
  });
  const res = await fetcher('blnkremote', 'https://blnkremote.supabase.co/rest/v1/wh_probe_agg?select=region');
  eq('refusal code', (res as { warning?: { code?: string } }).warning?.code, 'shard_path_select_disabled');
  eq('select arm rode the GET', calls[0]?.init.method, 'GET');
});

Deno.test('r69 fetcher: key-resolver throw => shard_key_missing, transport NEVER called', async () => {
  const calls: RawCall[] = [];
  const fetcher = makeRpcShardFetcher({
    resolveKey: () => {
      throw new WhEngineError('internal', 'shard_key_missing: ghostref');
    },
    rawFetch: fakeRaw({ body: '{}', calls }),
  });
  const res = await fetcher('ghostref', 'https://ghostref.supabase.co/rest/v1/rpc/wh_query', { p_template_hash: W1H, p_params: {} });
  eq('warning code', (res as { warning?: { code?: string } }).warning?.code, 'shard_key_missing');
  eq('no transport call', calls.length, 0);
});

Deno.test('r69 fetcher: transport/text throw arms => network; the fetcher NEVER rejects (catch-inside is load-bearing)', async () => {
  const rejecting = makeRpcShardFetcher({ resolveKey: () => 'sk', rawFetch: fakeRaw({ reject: true }) });
  let settled = false;
  const r1 = await rejecting('s', 'u', { p_template_hash: W1H, p_params: {} });
  settled = true;
  eq('rawFetch rejection => network', (r1 as { warning?: { code?: string } }).warning?.code, 'network');
  eqTrue('no rejection escaped (settled)', settled);
  const textFail = makeRpcShardFetcher({ resolveKey: () => 'sk', rawFetch: fakeRaw({ failText: true }) });
  const r2 = await textFail('s', 'u');
  eq('text() failure => network', (r2 as { warning?: { code?: string } }).warning?.code, 'network');
});

Deno.test('r69 fetcher: rpc arm rides the §6.1 call shape EXACTLY (method/body/headers)', async () => {
  const calls: RawCall[] = [];
  const fetcher = makeRpcShardFetcher({
    resolveKey: () => 'shard-service-key',
    rawFetch: fakeRaw({ body: JSON.stringify(w1Wire()), calls }),
  });
  await fetcher('blnkremote', 'https://blnkremote.supabase.co/rest/v1/rpc/wh_query', { p_template_hash: W1H, p_params: { limit: 25 } });
  eq('one call', calls.length, 1);
  eq('POST', calls[0]?.init.method, 'POST');
  eq('url untouched (caller-pinned)', calls[0]?.url, 'https://blnkremote.supabase.co/rest/v1/rpc/wh_query');
  eq('body = the pinned rpc payload', calls[0]?.init.body, JSON.stringify({ p_template_hash: W1H, p_params: { limit: 25 } }));
  eq('headers exact', calls[0]?.init.headers, {
    'Accept-Profile': 'public',
    apikey: 'shard-service-key',
    Authorization: 'Bearer shard-service-key',
    'Content-Type': 'application/json',
  });
});

Deno.test('r69 fetcher: select arm = same auth MINUS Content-Type, no body', async () => {
  const calls: RawCall[] = [];
  const fetcher = makeRpcShardFetcher({ resolveKey: () => 'sk', rawFetch: fakeRaw({ body: '[]', calls }) });
  await fetcher('blnkremote', 'https://blnkremote.supabase.co/rest/v1/t?select=x');
  eq('GET', calls[0]?.init.method, 'GET');
  eq('headers exact (no Content-Type)', calls[0]?.init.headers, {
    'Accept-Profile': 'public',
    apikey: 'sk',
    Authorization: 'Bearer sk',
  });
  eqTrue('no body', calls[0]?.init.body === undefined);
});

Deno.test('r69 fetcher: D8 classification inputs (stamped 5xx-only advisory; WH code detail carrier)', async () => {
  const run = async (opts: { status: number; body?: string; headers?: Record<string, string> }) => {
    const f = makeRpcShardFetcher({ resolveKey: () => 'sk', rawFetch: fakeRaw(opts) });
    return await f('s', 'u', { p_template_hash: W1H, p_params: {} });
  };
  const wh400 = await run({ status: 400, body: JSON.stringify({ code: 'WH401', message: 'template_hash mismatch' }) });
  eq('400 + WH body => {httpStatus, code}', (wh400 as { warning?: Record<string, unknown> }).warning, { httpStatus: 400, code: 'WH401' });
  const pgrst = await run({ status: 400, body: JSON.stringify({ code: 'PGRST123', message: 'aggregates not enabled' }) });
  eq('400 non-WH body => {httpStatus} only (stamped ABSENT below 5xx)', (pgrst as { warning?: Record<string, unknown> }).warning, { httpStatus: 400 });
  const plain400 = await run({ status: 400, body: 'not json' });
  eq('400 non-JSON body => {httpStatus} only', (plain400 as { warning?: Record<string, unknown> }).warning, { httpStatus: 400 });
  const wh500 = await run({ status: 500, body: JSON.stringify({ code: 'WH500', message: 'wh_body_shape_violation' }), headers: { 'content-type': 'application/json' } });
  eq('500 + json content-type + WH500 => stamped true + code', (wh500 as { warning?: Record<string, unknown> }).warning, { httpStatus: 500, stamped: true, code: 'WH500' });
  const json500 = await run({ status: 500, body: '{"oops":true}', headers: { 'content-type': 'application/json' } });
  eq('500 + json, no WH code => stamped true only (gateway false-positive class is ADVISORY, AM-14)', (json500 as { warning?: Record<string, unknown> }).warning, { httpStatus: 500, stamped: true });
  const bare500 = await run({ status: 500, body: 'upstream', headers: { 'content-type': 'text/plain' } });
  eq('500 without json => stamped FALSE (explicit)', (bare500 as { warning?: Record<string, unknown> }).warning, { httpStatus: 500, stamped: false });
  // r69 a4battery FIX (harness-gate catch): the ORIGINAL expectation here was
  // {httpStatus: 402} "no WH parse outside 400-class" — but 402 IS 400-class
  // (400-499) and the design's carrier list is explicit: "WH400/WH401/WH402
  // on 400-class" (§3.4 arm 4, AM-5/AM-12). The impl was right; the cell's
  // class arithmetic was wrong. The stamped field stays ABSENT below 5xx.
  const c402 = await run({ status: 402, body: '{"code":"WH400"}' });
  eq('402 (400-class) + WH body => {httpStatus, code} (the WH400/401/402-on-400-class carrier list; stamped stays ABSENT below 5xx)', (c402 as { warning?: Record<string, unknown> }).warning, { httpStatus: 402, code: 'WH400' });
  const c429 = await run({ status: 429 });
  eq('429 => {httpStatus}', (c429 as { warning?: Record<string, unknown> }).warning, { httpStatus: 429 });
});

// =============================================================================
// 4. adaptWireEnvelope — the F-N1 algorithm (engine shape or null).
// =============================================================================
Deno.test('r69 adapter: W1 grouped remap — plan-named aggs, more := truncated, wire-only fields dropped, template_hash kept', () => {
  const out = adaptWireEnvelope(w1Wire(), W1_TEMPLATE, RPC_PLAN);
  eqTrue('adapted', out !== null);
  eq('engine shape verbatim', out, {
    v: 1,
    table: 'wh_probe_agg',
    schema_version: 1,
    template_hash: W1H,
    partial: {
      kind: 'grouped',
      groupKeys: ['region'],
      aggs: {
        'sum(amount)': { op: 'sum', col: 'amount' },
        'count(*)': { op: 'count' },
      },
      rows: [
        { k: ['eu'], a: { 'sum(amount)': '1200.50', 'count(*)': 14 } },
        { k: ['us'], a: { 'sum(amount)': '900.25', 'count(*)': 7 } },
      ],
      rowCount: 2,
      more: false,
    },
  });
  eqTrue('wire-only fields dropped', !('qc_class' in (out as Record<string, unknown>)) && !('encoding' in (out as Record<string, unknown>)) && !('template_timeout_ms' in (out as Record<string, unknown>)) && !('latencyMs' in (out as Record<string, unknown>)) && !('shard' in (out as Record<string, unknown>)));
  const truncated = adaptWireEnvelope(w1Wire({ truncated: true }), W1_TEMPLATE, RPC_PLAN);
  eq('more := wire.truncated', (truncated as { partial: { more: boolean } }).partial.more, true);
});

Deno.test('r69 adapter: extra-agg drop — wire aggs with no plan consumer are dropped (wh_merge P3 exact-set law)', () => {
  const out = adaptWireEnvelope(w1Wire(), W1_TEMPLATE, { aggs: { 'sum(amount)': { op: 'sum', col: 'amount' } } });
  eqTrue('adapted', out !== null);
  eq('only the plan-consumed agg survives', Object.keys((out as { partial: { aggs: Record<string, unknown> } }).partial.aggs), ['sum(amount)']);
});

Deno.test('r69 adapter: W2 count_col mapping — the plan lattice count(col) matches the count_col encoding (W2 mapping law)', () => {
  const out = adaptWireEnvelope(
    {
      v: 1, table: 'wh_probe_agg', schema_version: 1, qc_class: 'QC2', kind: 'scalar',
      aggs: { min: { op: 'min', col: 'amount' }, max: { op: 'max', col: 'amount' }, c: { op: 'count_col', col: 'amount' } },
      partial: { min: '12.50', max: '99.00', c: 14 }, rowCount: 1, truncated: false,
      encoding: {}, template_hash: W2H, template_timeout_ms: 8000, latencyMs: 3,
    },
    W2_TEMPLATE,
    { aggs: { 'min(amount)': { op: 'min', col: 'amount' }, 'max(amount)': { op: 'max', col: 'amount' }, 'count(amount)': { op: 'count', col: 'amount' } } },
  );
  eq('scalar remap with the count_col ≡ count equivalence', out, {
    v: 1,
    table: 'wh_probe_agg',
    schema_version: 1,
    template_hash: W2H,
    partial: {
      kind: 'scalar',
      aggs: {
        'min(amount)': { op: 'min', col: 'amount' },
        'max(amount)': { op: 'max', col: 'amount' },
        'count(amount)': { op: 'count', col: 'amount' },
      },
      rows: [{ k: [], a: { 'min(amount)': '12.50', 'max(amount)': '99.00', 'count(amount)': 14 } }],
      rowCount: 1,
      more: false,
    },
  });
});

Deno.test('r69 adapter: W3 avg fusion — the sum+count pair on the same col fuses into the wh_merge avg-pair shape {s, c}', () => {
  const out = adaptWireEnvelope(w3Wire(), W3_TEMPLATE, { aggs: { 'avg(amount)': { op: 'avg', col: 'amount' } } });
  eq('fused one-row scalar wrap', out, {
    v: 1,
    table: 'wh_probe_agg',
    schema_version: 1,
    template_hash: W3H,
    partial: {
      kind: 'scalar',
      aggs: { 'avg(amount)': { op: 'avg', col: 'amount' } },
      rows: [{ k: [], a: { 'avg(amount)': { s: '1200.50', c: 14 } } }],
      rowCount: 1,
      more: false,
    },
  });
});

Deno.test('r69 adapter: no-match => NULL (fail-closed, never fabricated); missing encoding key => NULL; garbage => NULL', () => {
  eqTrue(
    'plan agg the template cannot serve (sum over a foreign col) => null',
    adaptWireEnvelope(w1Wire(), W1_TEMPLATE, { aggs: { 'sum(other)': { op: 'sum', col: 'other' } } }) === null,
  );
  eqTrue(
    'avg against a template WITHOUT the avg_pair declaration (W2) => null — never a lone sum/count',
    adaptWireEnvelope(w3Wire(), W2_TEMPLATE, { aggs: { 'avg(amount)': { op: 'avg', col: 'amount' } } }) === null,
  );
  const missingKey = w1Wire();
  (missingKey.rows as Record<string, unknown>[])[0].a = { c: 14 }; // x absent
  eqTrue(
    'a row missing a served encoding key => null (never an empty-contribution launder)',
    adaptWireEnvelope(missingKey, W1_TEMPLATE, RPC_PLAN) === null,
  );
  eqTrue('array wire => null', adaptWireEnvelope([], W1_TEMPLATE, RPC_PLAN) === null);
  eqTrue('null wire => null', adaptWireEnvelope(null, W1_TEMPLATE, RPC_PLAN) === null);
  eqTrue('topk-kind wire => null (no plan-consumable aggregate set)', adaptWireEnvelope(w1Wire({ kind: 'topk', groupKeys: [] }), W1_TEMPLATE, RPC_PLAN) === null);
  eqTrue(
    'wire without template_hash still adapts (F14 stays engine-side); no template_hash key emitted',
    !('template_hash' in (adaptWireEnvelope(w1Wire({ template_hash: undefined }), W1_TEMPLATE, RPC_PLAN) as Record<string, unknown>)),
  );
});

// =============================================================================
// 5. §3.2 statics — compileShardRpcUrl / WH_RPC_ELIGIBLE_HASHES / rpcParams.
// =============================================================================
Deno.test('r69 compileShardRpcUrl: the byte-pinned §6.1 URL', () => {
  eq('exact string', compileShardRpcUrl('blnkremote'), 'https://blnkremote.supabase.co/rest/v1/rpc/wh_query');
});

Deno.test('r69 WH_RPC_ELIGIBLE_HASHES: W1/W2/W3 only (the null-guarded set; W4/W5-class excluded — F-N5)', () => {
  eq('contents', [...WH_RPC_ELIGIBLE_HASHES], [W1H, W2H, W3H]);
  eqTrue('W4 (topk) not eligible', !WH_RPC_ELIGIBLE_HASHES.includes(W4H));
  eqTrue('W5-class (unguarded dataset param) not eligible', !WH_RPC_ELIGIBLE_HASHES.includes(W5H));
});

Deno.test('r69 rpcParams: numeric limit echoes; limit:null and absent are ABSENT (AM-7)', () => {
  eq('limit 25 => {limit:25}', rpcParams(RPC_PLAN, { ...RPC_REQ.query, limit: 25 }), { limit: 25 });
  eq('limit null => {} (p_params.limit is NEVER null)', rpcParams(RPC_PLAN, { ...RPC_REQ.query, limit: null }), {});
  eq('limit absent => {}', rpcParams(RPC_PLAN, RPC_REQ.query), {});
});

// =============================================================================
// 6. checkHandshake matchedHash (AM-6/OQ-8).
// =============================================================================
Deno.test('r69 handshake verdict: matchedHash = first required hash in plan order; ineligible/zero-requirement verdicts unchanged', () => {
  const both = checkHandshake({ templateHashes: [W1H, W2H], limitK: null }, [invRow(W1H), invRow(W2H)], null);
  eq('eligible verdict carries the FIRST required hash (plan order)', both, { eligible: true, warning: null, matchedHash: W1H });
  const missing = checkHandshake({ templateHashes: [W1H, W2H], limitK: null }, [invRow(W1H)], null);
  eq('ineligible verdict UNCHANGED (no matchedHash)', missing, { eligible: false, warning: { code: 'template_missing', est_rows: null } });
  const emptyReq = checkHandshake({ templateHashes: [], limitK: 9999 }, [], null);
  eq('zero-requirement eligible verdict carries NO matchedHash', emptyReq, { eligible: true, warning: null });
});

// =============================================================================
// 7. executeWhQuery rpcMode plane — D2 gate + rpc targets + adaptation (F-N4).
// =============================================================================
function rpcPlaneArgs(overrides: Partial<Parameters<typeof executeWhQuery>[0]> = {}): Parameters<typeof executeWhQuery>[0] {
  return {
    req: RPC_REQ,
    columnTypes: { region: 'text', amount: 'numeric' },
    columnScales: { amount: 2 },
    directoryRows: [dirRow('shard-a'), dirRow('shard-b')],
    shardKeyColumn: '',
    shardKeyType: 'none',
    directoryVersion: 7,
    timers: defaultWhEngineTimers(),
    fetcher: (_shard: string, _url: string) => Promise.resolve({ ok: true as const, envelope: w1Wire(), estRows: 2 }),
    tableSchemaVersion: 1,
    handshake: { readTemplateInventory: async () => [invRow(W1H)] } satisfies WhShardHandshake,
    templateHashes: [W1H],
    rpcMode: true,
    ...overrides,
  };
}

Deno.test('r69 core e2e: rpcMode placements plane builds §6.1 rpc targets, adapts the flat wire, merges (loud coverage 2/2)', async () => {
  const seen: { shard: string; url: string; rpc?: WhRpcSpec }[] = [];
  const res = await executeWhQuery(rpcPlaneArgs({
    fetcher: ((shard: string, url: string, rpc?: WhRpcSpec) => {
      seen.push({ shard, url, ...(rpc !== undefined ? { rpc } : {}) });
      return Promise.resolve({ ok: true as const, envelope: w1Wire(), estRows: 2 });
    }) as WhShardFetcher,
  }));
  eq('targets carried the §6.1 rpc shape', seen, [
    { shard: 'shard-a', url: 'https://shard-a.supabase.co/rest/v1/rpc/wh_query', rpc: { p_template_hash: W1H, p_params: {} } },
    { shard: 'shard-b', url: 'https://shard-b.supabase.co/rest/v1/rpc/wh_query', rpc: { p_template_hash: W1H, p_params: {} } },
  ]);
  eq('rpc target url = compileShardRpcUrl', seen[0]?.url, compileShardRpcUrl('shard-a'));
  eq('both shards merged through the adapted wire', { coverage: res.coverage, partial: res.partial, warnings: res.warnings }, { coverage: '2/2', partial: false, warnings: [] });
  eq('merged rows (numeric scale 2, bigint carrier)', res.rows, [
    { k: ['eu'], aggs: { 'sum(amount)': 240100n, 'count(*)': 28 } },
    { k: ['us'], aggs: { 'sum(amount)': 180050n, 'count(*)': 14 } },
  ]);
  eq('perShard clean', res.perShard, [
    { shard: 'shard-a', ok: true, latencyMs: res.perShard[0]?.latencyMs, error: null },
    { shard: 'shard-b', ok: true, latencyMs: res.perShard[1]?.latencyMs, error: null },
  ]);
});

Deno.test('r69 core: rpcMode ABSENT (F-N4 Δ0 shape) => 2-arg select targets, adapter never runs', async () => {
  const seen: { shard: string; url: string; rpc?: WhRpcSpec }[] = [];
  const args = rpcPlaneArgs({
    fetcher: ((shard: string, url: string, rpc?: WhRpcSpec) => {
      seen.push({ shard, url, ...(rpc !== undefined ? { rpc } : {}) });
      return Promise.resolve({ ok: true as const, envelope: w1Wire(), estRows: 2 });
    }) as WhShardFetcher,
  });
  delete (args as Record<string, unknown>).rpcMode;
  // The unset path consumes the ENGINE shape — a flat wire would die loudly
  // in wh_merge (the Δ0 pin is the TARGET shape here, the suite pins the rest).
  const res = await executeWhQuery({
    ...args,
    fetcher: ((shard: string, url: string, rpc?: WhRpcSpec) => {
      seen.push({ shard, url, ...(rpc !== undefined ? { rpc } : {}) });
      return Promise.resolve({
        ok: true as const,
        envelope: {
          v: 1, shard, table: 'wh_probe_agg', schema_version: 1,
          partial: { kind: 'grouped' as const, groupKeys: ['region'], aggs: { 'sum(amount)': { op: 'sum' as const, col: 'amount' }, 'count(*)': { op: 'count' as const } }, rows: [{ k: ['eu'], a: { 'sum(amount)': '1200.50', 'count(*)': 14 } }], rowCount: 1, more: false },
        },
        estRows: 1,
      });
    }) as WhShardFetcher,
  });
  eq('select-shape targets, rpc ABSENT (byte-identical unset path)', seen, [
    { shard: 'shard-a', url: 'https://shard-a.supabase.co/rest/v1/wh_probe_agg?select=region,sum(amount),count()' },
    { shard: 'shard-b', url: 'https://shard-b.supabase.co/rest/v1/wh_probe_agg?select=region,sum(amount),count()' },
  ]);
  eq('merge proceeds over the engine shape', res.coverage, '2/2');
});

Deno.test('r69 core D2: hashless plan / where-carrying plan / W5-class-only plan => plan_untemplated pre-fan-out (rpcMode-gated)', async () => {
  const transportCalls: RawCall[] = [];
  const recording = ((shard: string, url: string, rpc?: WhRpcSpec) => {
    transportCalls.push({ url, init: { method: rpc ? 'POST' : 'GET', headers: {} } });
    return Promise.resolve({ ok: true as const, envelope: w1Wire(), estRows: 2 });
  }) as WhShardFetcher;
  await rejectsWith('hashless plan => plan_untemplated', () => executeWhQuery(rpcPlaneArgs({ templateHashes: [], fetcher: recording })), (e) => {
    eq('code', e.code, 'plan_untemplated');
    includes('message lists the table', e.message, "table 'wh_probe_agg'");
    includes('message lists derived_hashes.length', e.message, 'derives 0 template hash(es)');
    includes('message names the template-backed requirement', e.message, 'template-backed plans only');
  });
  await rejectsWith('where-carrying template plan => plan_untemplated (AM-2)', () => executeWhQuery(rpcPlaneArgs({
    req: { ...RPC_REQ, query: { ...RPC_REQ.query, where: [{ col: 'region', op: 'eq', value: 'eu' }] } },
    fetcher: recording,
  })), (e) => eq('code', e.code, 'plan_untemplated'));
  await rejectsWith('W5-class-only derived hash => plan_untemplated (OQ-4/F-N5)', () => executeWhQuery(rpcPlaneArgs({ templateHashes: [W5H], fetcher: recording })), (e) => eq('code', e.code, 'plan_untemplated'));
  eq('D2 fired PRE-fan-out (zero transport calls)', transportCalls.length, 0);
});

Deno.test('r69 core: rpc wire that cannot adapt => loud excluded (never fabricated); the honest shards still merge', async () => {
  const res = await executeWhQuery(rpcPlaneArgs({
    directoryRows: [dirRow('shard-a'), dirRow('shard-b')],
    fetcher: ((shard: string) =>
      Promise.resolve({ ok: true as const, envelope: shard === 'shard-a' ? w1Wire({ template_hash: W5H }) : w1Wire() }) as Promise<{ ok: true; envelope: Record<string, unknown>; estRows: number }>) as WhShardFetcher,
  }));
  // shard-a's wire claims an rpc-INELIGIBLE template hash — the manifest row
  // (W5) cannot serve the grouped plan's count(*) => adapter null => excluded.
  eq('the adapting shard still merges', res.coverage, '1/2');
  eq('partial + loud warning', res.partial, true);
  eq('warning names the excluded shard with the envelope_invalid detail', res.warnings.map((w) => ({ shard: w.shard, code: w.code })), [{ shard: 'shard-a', code: 'excluded' }]);
  eqTrue('detail carries the adaptation law', (res.warnings[0]?.detail ?? '').includes('does not adapt to the plan'));
});

Deno.test('r69 core sanity: compileShardRpcUrl/compileShardUrl identities + parseShardKeyEnv/resolver compose', () => {
  neverThrows('module graph composes offline', () => {
    const map = parseShardKeyEnv('{"r":"k"}');
    const resolve = makeShardKeyResolver({ ownRef: 'own', ownKey: 'ok', remoteKeys: map });
    return [resolve('r'), compileShardRpcUrl('r')];
  });
});

// =============================================================================
// 8. r69 §5 BATTERY (battery author — split-cells law, core committer ⊥
//    battery author). The cells the core self-cell batch (sections 1-7,
//    b2b08b5) did NOT cover, per the §5 gap census:
//   * resolver: byte-exact padded keys BOTH directions; remote-map case
//     sensitivity end-to-end (M1 mutant kill);
//   * fetcher: the D8→classify COMPOSITION (402/429/503 warnings are the
//     exact classifyFetchFailure inputs); the byte-faithful seeded-wire
//     fixtures cross-checked against db/shard-migrations/0015_wh_query_rpc.sql
//     (:389-404 rows-kind, :418-432 scalar-kind); the DEFAULT-cap non-ASCII
//     boundary (M4 mutant kill);
//   * engine core: replica/override targets never carry rpc (AM-13) with the
//     override select-arm refusal surfaced through the REAL fetcher; the D2
//     rpcMode gate's ABSENT direction (a hashless plan does NOT throw);
//     limit echo at the target-construction level (AM-7); whCode survives
//     classify into warnings[].detail on BOTH arms (AM-5/F-N2) unit + e2e;
//     the F-N3 multi-template impossibility census; the F-N5 manifest-FILE
//     provenance cross-check; the call-site shard stamp (merge-exclusion
//     attribution);
//   * mutants (§5 — direct behavioral pins per the repo convention: the
//     r53-era mutant discipline names the mutant each cell kills; this repo
//     has NO source-mutation harness): M1 case-fold, M2 own-ref prefix-match,
//     M3 fetcher body-mutation, M4 byte-cap off-by-one (both directions) +
//     UTF-16-unit confusion, M5 stamped body-peek, M6 parseShardKeyEnv
//     log-echo (err.message interpolation).
// r1 law: every expected value below is hand-computed from the fixtures and
// the pinned contracts — never read back off the implementation.
// =============================================================================

function batteryGeoRow(projectRef: string): WhGeoDirectoryRow {
  return {
    project_ref: projectRef,
    region: 'us-east-1',
    subscription: 'geo_sub',
    state: 'serving',
    applied_lsn: null,
    replay_ts: null,
    lag_bytes: null,
    coverage: ['wh_probe_agg'],
    last_health_at: '2026-09-29T00:00:00.000000Z',
    updated_at: '2026-09-29T00:00:00.000000Z',
  };
}

// ---------- resolver battery (§5) ----------
Deno.test('r69 battery resolver: whitespace-padded keys resolve BYTE-EXACT both directions (the never-trim JWT law, lookup-side pin)', () => {
  // Map side: a padded KEY is a distinct entry (core pins the parse is
  // verbatim; here the RESOLVER must look it up byte-exact).
  const resolve = makeShardKeyResolver({
    ownRef: 'fmhostref',
    ownKey: 'own',
    remoteKeys: new Map([['blnkremote', 'key-plain'], ['blnkpad ', 'key-pad']]),
  });
  eq('padded MAP key resolves only under the padded lookup', resolve('blnkpad '), 'key-pad');
  eq('plain key unaffected', resolve('blnkremote'), 'key-plain');
  // Lookup side: a padded LOOKUP of the plain key is a MISS and the thrown
  // message names the ref VERBATIM (space included) — a trimming/case-fold
  // mutant resolves it and REDs both pins.
  let paddedMsg = '';
  try {
    resolve('blnkremote ');
  } catch (e) {
    paddedMsg = (e as Error).message;
  }
  eq('padded LOOKUP of a plain key misses; the message names the ref byte-exact', paddedMsg, 'shard_key_missing: blnkremote ');
  // Value side: internal whitespace rides VERBATIM (service keys are JWTs —
  // never trimmed/case-normalized at any arm).
  const spaced = makeShardKeyResolver({
    ownRef: '',
    ownKey: 'k',
    remoteKeys: parseShardKeyEnv('{"padref":"  eyJ.hi. there  "}'),
  });
  eq('value with internal whitespace byte-exact', spaced('padref'), '  eyJ.hi. there  ');
});

Deno.test('r69 battery resolver M1: remote-map lookup is CASE-SENSITIVE end-to-end (case-fold mutant kill)', () => {
  // Mutant M1 (resolver case-fold: shard.toLowerCase() at the lookup, or a
  // key-normalizing parse) resolves the lowercase probes and returns the key
  // instead of throwing — every pin below flips RED under it.
  const resolve = makeShardKeyResolver({
    ownRef: 'fmhostref',
    ownKey: 'own',
    remoteKeys: new Map([['BLNKREMOTE', 'upper-key'], ['Mixed-Ref', 'mixed-key']]),
  });
  eq('UPPERCASE map key hits under the UPPERCASE lookup', resolve('BLNKREMOTE'), 'upper-key');
  eq('mixed-case map key hits under the exact mixed lookup', resolve('Mixed-Ref'), 'mixed-key');
  let lowerMsg = '';
  try {
    resolve('blnkremote');
  } catch (e) {
    lowerMsg = (e as Error).message;
  }
  eq('lowercase probe of an UPPERCASE entry MISSES', lowerMsg, 'shard_key_missing: blnkremote');
  let mixedMsg = '';
  try {
    resolve('mixed-ref');
  } catch (e) {
    mixedMsg = (e as Error).message;
  }
  eq('lowercased probe of a mixed entry MISSES', mixedMsg, 'shard_key_missing: mixed-ref');
  // The parse side feeds this map — key case survives it verbatim (a
  // case-folding parse mutant collapses the two probes below).
  const parsed = parseShardKeyEnv('{"MIXEDcase":"v"}');
  eq('parse preserves key case', parsed.get('MIXEDcase'), 'v');
  eqTrue('no case-folded alias entry exists', !parsed.has('mixedcase'));
});

Deno.test('r69 battery mutant M2: own-ref PREFIX-match (bln… vs blnk…) — exact equality only', () => {
  // Mutant M2 (startsWith/prefix own-ref match): 'blnkremote'.startsWith
  // ('blnk') routes the REMOTE ref into the own-ref arm and returns ownKey —
  // all three pins below flip RED under it.
  const prefix = makeShardKeyResolver({ ownRef: 'blnk', ownKey: 'own-key', remoteKeys: new Map() });
  eq('the exact own ref still resolves with ownKey', prefix('blnk'), 'own-key');
  let threw = '';
  try {
    prefix('blnkremote');
  } catch (e) {
    threw = (e as Error).message;
  }
  eq('a ref EXTENDING the ownRef is NOT the own shard (throws naming it)', threw, 'shard_key_missing: blnkremote');
  const withRemote = makeShardKeyResolver({ ownRef: 'blnk', ownKey: 'own-key', remoteKeys: new Map([['blnkremote', 'remote-key']]) });
  eq('the extending ref resolves from the REMOTE map, never the own key', withRemote('blnkremote'), 'remote-key');
});

// ---------- fetcher battery (§5) ----------
Deno.test('r69 battery fetcher: 402/429/503 warnings are the EXACT classifyFetchFailure inputs (D8→classify composition; gateway classes)', async () => {
  const warnOf = async (opts: { status: number; body?: string; headers?: Record<string, string> }) => {
    const f = makeRpcShardFetcher({ resolveKey: () => 'sk', rawFetch: fakeRaw(opts) });
    const res = await f('s', 'u', { p_template_hash: W1H, p_params: {} });
    return (res as { warning?: { code?: string; httpStatus?: number; stamped?: boolean } }).warning;
  };
  const w402 = await warnOf({ status: 402, body: '{"quota":"exceeded"}' });
  eq('402 warning = {httpStatus:402} EXACTLY (the D8 classify input)', w402, { httpStatus: 402 });
  eq('classify(402) => http_402 (the gateway quota class)', classifyFetchFailure(w402 as { httpStatus: number }), { code: 'http_402' });
  const w429 = await warnOf({ status: 429 });
  eq('429 warning = {httpStatus:429} EXACTLY', w429, { httpStatus: 429 });
  eq('classify(429) => http_429', classifyFetchFailure(w429 as { httpStatus: number }), { code: 'http_429' });
  const w503 = await warnOf({ status: 503, body: '{"unavailable":true}', headers: { 'content-type': 'application/json' } });
  eq('503 + json content-type => stamped true (the AM-14 gateway-5xx false-positive datum — advisory-only)', w503, { httpStatus: 503, stamped: true });
  eq('classify(503 stamped) => http_5xx + stamped (the shard-app retried-eligible class)', classifyFetchFailure(w503 as { httpStatus: number; stamped: boolean }), { code: 'http_5xx', stamped: true });
});

Deno.test('r69 battery fetcher: the seeded §6.2 wire byte-shape (0015:389-404 rows / :418-432 scalar) passes RAW — keys pinned against the migration text', async () => {
  // Byte-faithful fixtures: field names/order from the seeded fn's
  // jsonb_build_object emissions, values from the pinned manifest rows (the
  // W1 rows-kind encoding {x:text, c:number}; the W3 scalar-kind encoding
  // {s:text, c:number}) + §4 worked-example values (hand-computed estRows:
  // 2 and 1 from the fixtures' rowCount fields).
  const ROWS_WIRE: Record<string, unknown> = {
    v: 1,
    table: 'wh_probe_agg',
    schema_version: 1,
    qc_class: 'QC2',
    kind: 'grouped',
    groupKeys: ['region'],
    aggs: { x: { op: 'sum', col: 'amount' }, c: { op: 'count' } },
    rows: [
      { k: ['eu'], a: { x: '1200.50', c: 14 } },
      { k: ['us'], a: { x: '900.25', c: 7 } },
    ],
    rowCount: 2,
    truncated: false,
    encoding: { x: 'text', c: 'number' },
    template_hash: W1H,
    template_timeout_ms: 8000,
    latencyMs: 12,
  };
  const SCALAR_WIRE: Record<string, unknown> = {
    v: 1,
    table: 'wh_probe_agg',
    schema_version: 1,
    qc_class: 'QC2',
    kind: 'scalar',
    aggs: { s: { op: 'sum', col: 'amount' }, c: { op: 'count_col', col: 'amount' } },
    partial: { s: '1200.50', c: 14 },
    rowCount: 1,
    truncated: false,
    encoding: { s: 'text', c: 'number' },
    template_hash: W3H,
    template_timeout_ms: 8000,
    latencyMs: 9,
  };
  eq('rows-kind keys in seed emission order', Object.keys(ROWS_WIRE), [
    'v', 'table', 'schema_version', 'qc_class', 'kind', 'groupKeys', 'aggs', 'rows', 'rowCount', 'truncated', 'encoding', 'template_hash', 'template_timeout_ms', 'latencyMs',
  ]);
  eq('scalar-kind keys in seed emission order', Object.keys(SCALAR_WIRE), [
    'v', 'table', 'schema_version', 'qc_class', 'kind', 'aggs', 'partial', 'rowCount', 'truncated', 'encoding', 'template_hash', 'template_timeout_ms', 'latencyMs',
  ]);
  // File cross-check: the fixture key lists are EXTRACTED from the migration
  // source (the two return jsonb_build_object( blocks, in file order) —
  // byte-faithfulness to the SEED, not to this battery's transcription.
  let sql: string | null = null;
  try {
    sql = Deno.readTextFileSync(new URL('../../../db/shard-migrations/0015_wh_query_rpc.sql', import.meta.url));
  } catch {
    sql = null;
  }
  if (sql !== null) {
    // Line-anchored key extraction: each emission line is `      'key',  value,`
    // — the anchor keeps quoted VALUE literals ('scalar' is a VALUE in the
    // scalar block's kind line) out of the key list.
    const blocks = [...sql.matchAll(/return jsonb_build_object\(([\s\S]*?)\n    \);/g)].map((m) =>
      [...m[1].matchAll(/^\s*'([A-Za-z_]+)',/gm)].map((k) => k[1])
    );
    eq('migration file: exactly two envelope emission blocks', blocks.length, 2);
    eq('rows-kind block keys === fixture keys (file order)', blocks[0], Object.keys(ROWS_WIRE));
    eq('scalar-kind block keys === fixture keys (file order)', blocks[1], Object.keys(SCALAR_WIRE));
  } else {
    console.log('  note [fallback branch: migration file not readable — the emission-order literals above carry the pin]');
    eqTrue('emission-order literals pinned above', true);
  }
  // The fetcher passes each fixture through RAW (byte-identical, no engine
  // stamping) with estRows := rowCount (hand-computed: 2 and 1).
  const rowsFetch = makeRpcShardFetcher({ resolveKey: () => 'sk', rawFetch: fakeRaw({ body: JSON.stringify(ROWS_WIRE) }) });
  const rowsRes = await rowsFetch('blnkremote', compileShardRpcUrl('blnkremote'), { p_template_hash: W1H, p_params: {} });
  eqTrue('rows-kind ok', rowsRes.ok === true);
  eq('rows-kind envelope is the RAW fixture (no stamping — no shard/qc drift)', (rowsRes as { envelope: unknown }).envelope, ROWS_WIRE);
  eq('rows-kind estRows := rowCount', (rowsRes as { estRows: number }).estRows, 2);
  const scalarFetch = makeRpcShardFetcher({ resolveKey: () => 'sk', rawFetch: fakeRaw({ body: JSON.stringify(SCALAR_WIRE) }) });
  const scalarRes = await scalarFetch('blnkremote', compileShardRpcUrl('blnkremote'), { p_template_hash: W3H, p_params: {} });
  eqTrue('scalar-kind ok', scalarRes.ok === true);
  eq('scalar-kind envelope is the RAW fixture', (scalarRes as { envelope: unknown }).envelope, SCALAR_WIRE);
  eq('scalar-kind estRows := rowCount', (scalarRes as { estRows: number }).estRows, 1);
});

Deno.test('r69 battery fetcher M4: DEFAULT-cap non-ASCII boundary (byte-cap off-by-one both directions + UTF-16-unit confusion kill)', async () => {
  // Mutants M4a (`>` → `>=`: the exact-cap body aborts) and M4b (slack
  // off-by-one: cap+1 passes) both flip the ASCII pins below; the UTF-16-unit
  // mutant (code units instead of TextEncoder bytes) survives every ASCII
  // boundary and dies ONLY on the non-ASCII pins (units ≠ bytes).
  const asciiExact = makeRpcShardFetcher({ resolveKey: () => 'sk', rawFetch: fakeRaw({ body: 'x'.repeat(12) }), maxPartialBytes: 12 });
  const asciiExactRes = await asciiExact('s', 'u', { p_template_hash: W1H, p_params: {} });
  eq('12 bytes == cap 12 passes the cap (M4a REDs: an >= mutant aborts)', (asciiExactRes as { warning?: { code?: string } }).warning?.code, 'envelope_invalid');
  const asciiOver = makeRpcShardFetcher({ resolveKey: () => 'sk', rawFetch: fakeRaw({ body: 'x'.repeat(13) }), maxPartialBytes: 12 });
  const asciiOverRes = await asciiOver('s', 'u', { p_template_hash: W1H, p_params: {} });
  eq('13 bytes > cap 12 aborts (M4b REDs: any slack mutant passes)', (asciiOverRes as { warning?: { code?: string } }).warning?.code, 'abort_on_oversize');
  // At the DEFAULT cap: 'é' is 2 UTF-8 bytes / 1 UTF-16 unit — the unit
  // mutant reads 32768/32769 units and NEVER trips the 65536-byte cap.
  const bodyExact = 'é'.repeat(32768);
  eq('fixture self-check: units ≠ bytes (32768 units, 65536 bytes)', [bodyExact.length, new TextEncoder().encode(bodyExact).length], [32768, 65536]);
  const exact = makeRpcShardFetcher({ resolveKey: () => 'sk', rawFetch: fakeRaw({ body: bodyExact }) });
  const exactRes = await exact('s', 'u', { p_template_hash: W1H, p_params: {} });
  eq('65536 bytes == DEFAULT cap passes (M4a REDs)', (exactRes as { warning?: { code?: string } }).warning?.code, 'envelope_invalid');
  const bodyOver = 'é'.repeat(32769);
  eq('fixture self-check: 65538 bytes over the cap', new TextEncoder().encode(bodyOver).length, 65538);
  const over = makeRpcShardFetcher({ resolveKey: () => 'sk', rawFetch: fakeRaw({ body: bodyOver }) });
  const overRes = await over('s', 'u', { p_template_hash: W1H, p_params: {} });
  eq('65538 bytes > DEFAULT cap aborts (the UTF-16-unit mutant reads 32769 and PASSES — RED)', (overRes as { warning?: { code?: string } }).warning?.code, 'abort_on_oversize');
});

Deno.test('r69 battery mutant M3: fetcher body-mutation (params dropped / hash-only) — byte-exact body pin', async () => {
  // Mutant M3 (the POST body drops p_params, or serializes the hash only, or
  // re-orders the keys): the BYTE pin below is stronger than a deep-eq — any
  // re-serialization drift flips it RED.
  const calls: RawCall[] = [];
  const fetcher = makeRpcShardFetcher({
    resolveKey: () => 'sk-resolved',
    rawFetch: fakeRaw({ body: JSON.stringify(w1Wire()), calls }),
  });
  await fetcher('blnkremote', compileShardRpcUrl('blnkremote'), { p_template_hash: W2H, p_params: { limit: 7 } });
  eq('M3: the POST body is the byte-exact §6.1 payload', calls[0]?.init.body, `{"p_template_hash":"${W2H}","p_params":{"limit":7}}`);
  eq('M3: method stays POST (a GET-downgrade mutant REDs)', calls[0]?.init.method, 'POST');
  eq('M3: the key rides BOTH auth headers verbatim (a dropped-header mutant REDs)', [
    calls[0]?.init.headers.apikey,
    calls[0]?.init.headers.Authorization,
  ], ['sk-resolved', 'Bearer sk-resolved']);
});

Deno.test('r69 battery mutant M5: stamped-detect body-peek — 500-class pins both directions', async () => {
  const run = async (opts: { status: number; body: string; headers?: Record<string, string> }) => {
    const f = makeRpcShardFetcher({ resolveKey: () => 'sk', rawFetch: fakeRaw(opts) });
    const res = await f('s', 'u', { p_template_hash: W1H, p_params: {} });
    return (res as { warning?: Record<string, unknown> }).warning;
  };
  const textHeader = await run({ status: 500, body: '{"code":"WH500","message":"x"}', headers: { 'content-type': 'text/plain' } });
  eq('M5: 500 + JSON-parseable body + NON-json header => stamped FALSE and the WH body does NOT leak (body-peek mutant REDs)', textHeader, { httpStatus: 500, stamped: false });
  const jsonHeader = await run({ status: 500, body: 'garbage-not-json', headers: { 'content-type': 'application/json' } });
  eq('M5: 500 + json header + unparseable body => stamped TRUE (header presence decides — body-peek mutant REDs)', jsonHeader, { httpStatus: 500, stamped: true });
});

Deno.test('r69 battery mutant M6: parseShardKeyEnv log-echo (err.message interpolation) — the r57 echo law', () => {
  // Mutant M6 (the defect log interpolates err.message): V8's JSON.parse
  // error EMBEDS input fragments ("Unexpected token 'S', \"SECRET-…\" is not
  // valid JSON") — an interpolating mutant leaks the fragment and the
  // 'Unexpected' prefix; both pins flip RED under it. The fixed string names
  // the env + the defect class only.
  const defects: string[] = [];
  const map = parseShardKeyEnv('SECRET-VALUE-FRAGMENT is not json', (m) => defects.push(m));
  eq('M6: fail-closed empty map', map.size, 0);
  eq('M6: exactly ONE defect line', defects.length, 1);
  const msg = defects[0] ?? '';
  includes('M6: the log names the env + the defect class', msg, 'WH_SHARD_KEYS defect class unparseable-json');
  eqTrue('M6: NO value fragment echoed (the V8 message embeds the input — interpolation REDs here)', !msg.includes('SECRET-VALUE-FRAGMENT'));
  eqTrue("M6: no 'Unexpected' token echo (the V8 message prefix)", !msg.includes('Unexpected'));
});

// ---------- engine-core battery (§5) ----------
Deno.test('r69 battery core: a WON replica-plane target NEVER carries rpc (AM-13; G5 keeps template plans off the replica; rpcMode does not leak onto it)', async () => {
  const seen: { shard: string; url: string; rpc?: WhRpcSpec }[] = [];
  const res = await executeWhQuery(rpcPlaneArgs({
    req: { ...RPC_REQ, read_plane: 'replica' },
    templateHashes: [], // G5: a template plan never rides the replica plane
    geoMode: 'spread',
    geoReader: async () => [batteryGeoRow('fmhostref')],
    fetcher: ((shard: string, url: string, rpc?: WhRpcSpec) => {
      seen.push({ shard, url, ...(rpc !== undefined ? { rpc } : {}) });
      return Promise.resolve({ ok: false as const, warning: { code: 'network' } });
    }) as WhShardFetcher,
  }));
  eq('ONE target: the replica, select-shape, rpc ABSENT (rpcMode does not leak onto the won replica)', seen, [
    { shard: 'fmhostref', url: 'https://fmhostref.supabase.co/rest/v1/wh_probe_agg?select=region,sum(amount),count()' },
  ]);
  eqTrue('no rpc key at all', !('rpc' in (seen[0] as Record<string, unknown>)));
  eq('the replica is the whole coverage denominator (single-dispatch law)', res.coverage, '0/1');
  eq('the injected network warning is the only plane event (no fallback warning — the replica WON)', res.warnings, [
    { shard: 'fmhostref', code: 'network', est_rows: 0, retried: false },
  ]);
});

Deno.test('r69 battery core: geo-dispatch override targets NEVER carry rpc and stay OUT of D2 scope (AM-13 + F-N4 plane pin)', async () => {
  const seen: { shard: string; url: string; rpc?: WhRpcSpec }[] = [];
  const res = await executeWhQuery(rpcPlaneArgs({
    templateHashes: [W1H], // a TEMPLATE plan on the override plane: D2 does NOT fire there
    geoReadDispatch: {
      kind: 'engine_local',
      target: 'primaryref',
      warning: { shard: 'primaryref', code: 'geo_fallback_primary', est_rows: 0, retried: false, detail: 'engine_local' },
    } satisfies WhGeoReadDispatch,
    fetcher: ((shard: string, url: string, rpc?: WhRpcSpec) => {
      seen.push({ shard, url, ...(rpc !== undefined ? { rpc } : {}) });
      return Promise.resolve({ ok: false as const, warning: { code: 'network' } });
    }) as WhShardFetcher,
  }));
  eq('ONE target: the override, select-shape, rpc ABSENT (the placements D2 gate is out of scope here)', seen, [
    { shard: 'primaryref', url: 'https://primaryref.supabase.co/rest/v1/wh_probe_agg?select=region,sum(amount),count()' },
  ]);
  eqTrue('no rpc key at all', !('rpc' in (seen[0] as Record<string, unknown>)));
  eq('the loud B2 engine_local token rides verbatim', res.warnings[0], {
    shard: 'primaryref', code: 'geo_fallback_primary', est_rows: 0, retried: false, detail: 'engine_local',
  });
  eq('override coverage denominator is 1 (single-dispatch)', res.coverage, '0/1');
});

Deno.test('r69 battery core: the override select-arm refusal surfaces through the REAL fetcher (AM-13 disclosure — degrade loudly, never lie)', async () => {
  const calls: RawCall[] = [];
  const res = await executeWhQuery(rpcPlaneArgs({
    templateHashes: [W1H],
    geoReadDispatch: { kind: 'remote', target: 'primaryref' } satisfies WhGeoReadDispatch,
    fetcher: makeRpcShardFetcher({
      resolveKey: () => 'sk',
      rawFetch: fakeRaw({ body: '[{"region":"eu"}]', calls }),
    }),
  }));
  eq('the REAL fetcher rode the residual SELECT arm (GET — the target carries no rpc to POST)', calls, [
    {
      url: 'https://primaryref.supabase.co/rest/v1/wh_probe_agg?select=region,sum(amount),count()',
      init: { method: 'GET', headers: { 'Accept-Profile': 'public', apikey: 'sk', Authorization: 'Bearer sk' } },
    },
  ]);
  eq('the refusal degrades LOUD: engine-classified excluded carrying the select-arm detail (OQ-2 — never a lie, never a 5xx)', res.warnings, [
    { shard: 'primaryref', code: 'excluded', est_rows: 0, retried: false, detail: 'shard_path_select_disabled' },
  ]);
  eq('coverage 0/1 partial (the aggregate-dead override plane post-flip)', [res.coverage, res.partial], ['0/1', true]);
});

Deno.test('r69 battery core D2: rpcMode ABSENT keeps the D2 check ABSENT — a hashless plan does NOT throw plan_untemplated (F-N4 Δ0 direction)', async () => {
  const args = rpcPlaneArgs({ templateHashes: [] });
  delete (args as Record<string, unknown>).rpcMode;
  let res: Awaited<ReturnType<typeof executeWhQuery>> | null = null;
  let threw = '';
  try {
    res = await executeWhQuery({
      ...args,
      // The unset path consumes the ENGINE shape (the core Δ0 cell pins the
      // select-shape targets; HERE the pin is the absent D2 throw).
      fetcher: ((shard: string) =>
        Promise.resolve({
          ok: true as const,
          envelope: {
            v: 1,
            shard,
            table: 'wh_probe_agg',
            schema_version: 1,
            partial: {
              kind: 'grouped' as const,
              groupKeys: ['region'],
              aggs: { 'sum(amount)': { op: 'sum' as const, col: 'amount' }, 'count(*)': { op: 'count' as const } },
              rows: [{ k: ['eu'], a: { 'sum(amount)': '1200.50', 'count(*)': 14 } }],
              rowCount: 1,
              more: false,
            },
          },
          estRows: 1,
        })) as WhShardFetcher,
    });
  } catch (e) {
    threw = `${(e as Error).name}: ${(e as Error).message}`;
  }
  eq('no throw (the D2 check does not EXIST with rpcMode absent — F-N4)', threw, '');
  eq('the pre-r69 pipeline serves both shards', res?.coverage, '2/2');
});

Deno.test('r69 battery core: limit echo rides the rpc target p_params; limit:null stays ABSENT (AM-7 at target construction)', async () => {
  const recording = () => {
    const seen: { shard: string; url: string; rpc?: WhRpcSpec }[] = [];
    const fetcher = ((shard: string, url: string, rpc?: WhRpcSpec) => {
      seen.push({ shard, url, ...(rpc !== undefined ? { rpc } : {}) });
      return Promise.resolve({ ok: true as const, envelope: w1Wire(), estRows: 2 });
    }) as WhShardFetcher;
    return { seen, fetcher };
  };
  const lim = recording();
  await executeWhQuery(rpcPlaneArgs({
    req: { ...RPC_REQ, query: { ...RPC_REQ.query, limit: 25 } },
    fetcher: lim.fetcher,
  }));
  eq('limit 25 echoes into BOTH rpc targets', lim.seen.map((t) => t.rpc), [
    { p_template_hash: W1H, p_params: { limit: 25 } },
    { p_template_hash: W1H, p_params: { limit: 25 } },
  ]);
  const nul = recording();
  await executeWhQuery(rpcPlaneArgs({
    req: { ...RPC_REQ, query: { ...RPC_REQ.query, limit: null } },
    fetcher: nul.fetcher,
  }));
  eq('limit:null => p_params {} (AM-7: p_params.limit is NEVER null on the wire)', nul.seen.map((t) => t.rpc), [
    { p_template_hash: W1H, p_params: {} },
    { p_template_hash: W1H, p_params: {} },
  ]);
  eqTrue('no limit key at all', nul.seen.every((t) => !('limit' in (t.rpc?.p_params as Record<string, unknown>))));
});

Deno.test('r69 battery core: whCode survives classify into warnings[].detail on BOTH arms (AM-5/F-N2 unit pins) — r121: WH400/WH401 MAP to template_missing', () => {
  // Hand-computed: the generic-http arm computes {code:'excluded', detail:
  // 'http <n>'} / the 402/429/5xx arms their own outcomes — then the ONE
  // additive post-branch step replaces detail with the WH code. The branch
  // ORDER is untouched (402/429 still classify first) and the lift covers
  // BOTH arms (F-N2). r121 OPT-1b (design §1.3): the WH400/WH401 arms now MAP
  // to the §5.2-exempt template_missing class (detail KEPT, scoped to the
  // generic-4xx/code-only excluded arms); WH402/WH403 stay excluded.
  eq('400-class: {template_missing, detail WH400} (the r121 mapped-refusal record — unit-level)', classifyFetchFailure({ httpStatus: 400, code: 'WH400' }), { code: 'template_missing', detail: 'WH400' });
  eq('401-class: {template_missing, detail WH401}', classifyFetchFailure({ httpStatus: 401, code: 'WH401' }), { code: 'template_missing', detail: 'WH401' });
  eq('402 arm: http_402 FIRST, then the lift (order untouched, both arms enriched)', classifyFetchFailure({ httpStatus: 402, code: 'WH402' }), { code: 'http_402', detail: 'WH402' });
  eq('429 arm: http_429 + the lift', classifyFetchFailure({ httpStatus: 429, code: 'WH429' }), { code: 'http_429', detail: 'WH429' });
  eq('5xx arm: http_5xx + stamped rides + WH500 detail (the shard-integrity alarm class, AM-12)', classifyFetchFailure({ httpStatus: 503, stamped: true, code: 'WH500' }), { code: 'http_5xx', stamped: true, detail: 'WH500' });
  eq('guard: a NON-WH code does NOT ride the detail (plain http class, byte-identical)', classifyFetchFailure({ httpStatus: 400, code: 'PGRST123' }), { code: 'excluded', detail: 'http 400' });
  eqTrue('guard: WH[0-9]{3} exact — a WH40xx/WHx99 form never lifts', classifyFetchFailure({ httpStatus: 400, code: 'WH4096' }).detail === 'http 400');
});

Deno.test('r69 battery core: whCode + stamped ride the engine outcome through the REAL fetcher (AM-5/F-N2/AM-12 e2e)', async () => {
  const run = (shardAOpts: { status: number; body: string; headers?: Record<string, string> }) =>
    executeWhQuery(rpcPlaneArgs({
      fetcher: makeRpcShardFetcher({
        resolveKey: () => 'sk',
        rawFetch: async (url, init) => {
          if (url.includes('shard-a')) {
            return {
              ok: shardAOpts.status >= 200 && shardAOpts.status <= 299,
              status: shardAOpts.status,
              headers: { get: (n: string) => shardAOpts.headers?.[n.toLowerCase()] ?? null },
              text: async () => shardAOpts.body,
            };
          }
          return { ok: true, status: 200, headers: { get: () => null }, text: async () => JSON.stringify(w1Wire()) };
        },
      }),
    }));
  // 400-class: the WH body code is the DETAIL CARRIER — classify lifts it
  // onto warnings[].detail (the generic-http arm would otherwise eat it).
  // r121 OPT-1b: WH400 maps to the EXEMPT template_missing class (detail
  // kept; est_rows re-attached from the directory lane — these fixtures
  // carry no row_estimate, so 0), and the exclusion degrades, never 5xxs.
  const r400 = await run({ status: 400, body: '{"code":"WH400","message":"bad template"}' });
  eq('400 WH400: mapped to template_missing with the shard code as detail (r121 record)', r400.warnings, [
    { shard: 'shard-a', code: 'template_missing', est_rows: 0, retried: false, detail: 'WH400' },
  ]);
  eq('the honest shard still merges', [r400.coverage, r400.partial], ['1/2', true]);
  eq('perShard carries the mapped error', r400.perShard.map((p) => ({ shard: p.shard, ok: p.ok, error: p.error })), [
    { shard: 'shard-a', ok: false, error: 'template_missing' },
    { shard: 'shard-b', ok: true, error: null },
  ]);
  // 5xx-with-json: stamped rides perShard (AM-12: the §5.3 page doctrine
  // reaches the engine — advisory-only, never decision-gating).
  const r500 = await run({ status: 500, body: '{"code":"WH500","message":"wh_body_shape_violation"}', headers: { 'content-type': 'application/json' } });
  eq('500 WH500 + json: http_5xx warning with the WH500 detail', r500.warnings, [
    { shard: 'shard-a', code: 'http_5xx', est_rows: 0, retried: false, detail: 'WH500' },
  ]);
  const psA = r500.perShard.find((p) => p.shard === 'shard-a');
  eq('perShard stamped:true (advisory)', psA && { ok: psA.ok, error: psA.error, stamped: psA.stamped }, { ok: false, error: 'http_5xx', stamped: true });
});

Deno.test('r69 battery core census: every REAL plan op-set derives <= 1 template over the pinned manifest (F-N3/OQ-8 impossibility census)', () => {
  const AGG_DEFS: Record<string, { op: string; col?: string }> = {
    sum: { op: 'sum', col: 'amount' },
    count: { op: 'count' },
    count_col: { op: 'count', col: 'amount' },
    min: { op: 'min', col: 'amount' },
    max: { op: 'max', col: 'amount' },
    avg_pair: { op: 'avg', col: 'amount' },
  };
  const TOKENS = Object.keys(AGG_DEFS);
  const planFrom = (table: string, kind: 'rows' | 'scalar', tokens: readonly string[]): DerivePlanView => ({
    table,
    ...(kind === 'rows' ? { groupKeys: [{ col: 'region', type: 'text' }] as readonly unknown[] } : {}),
    aggs: Object.fromEntries(tokens.filter((t) => t !== 'groupby').map((t) => [t, AGG_DEFS[t]])),
  });
  const violations: string[] = [];
  let combos = 0;
  let exactlyOne = 0;
  const derivedHashes = new Set<string>();
  for (const table of ['wh_probe_agg', 'facts_blocks']) {
    for (const kind of ['rows', 'scalar'] as const) {
      // 63 non-empty agg subsets per (table, kind). The agg-less mask-0 shape
      // is NOT a real plan (parseWhEngineRequest requires a non-empty select;
      // validatePlan rejects an empty agg map) — and it is the ONE shape that
      // subset-matches two templates, pinned below as the F-N3 exclusion datum.
      for (let mask = 1; mask < 64; mask++) {
        const tokens = TOKENS.filter((_, i) => (mask & (1 << i)) !== 0);
        const derived = deriveTemplateHashes(planFrom(table, kind, tokens), undefined);
        combos++;
        for (const h of derived) derivedHashes.add(h);
        if (derived.length > 1) violations.push(`${table}/${kind}/${tokens.join('+')} => ${derived.length}`);
        if (derived.length === 1) exactlyOne++;
      }
    }
  }
  eq('combo count (2 tables x 2 kinds x 63 non-empty agg subsets)', combos, 252);
  eq('every plan op-set derives <= 1 template (the audits W2nW3 overlap hypothesis is FALSE at the pinned manifest)', violations, []);
  eq('non-vacuous: exactly-one combos (hand-counted: 3 W1-rows + 7 W2-scalar + 1 W3-avg + 3 W5-facts)', exactlyOne, 14);
  eqTrue('W4 (topk) is NEVER derived — plans cannot derive the topk/raw_rows tokens', !derivedHashes.has(W4H));
  eq('mask-0 datum: the agg-less scalar shape is the ONLY 2-template match ({} subset-matches both) — and it is NOT a plan', deriveTemplateHashes(planFrom('wh_probe_agg', 'scalar', []), undefined), [W2H, W3H]);
  eq('anchor W1: rows {groupby,sum,count} => [W1H]', deriveTemplateHashes(planFrom('wh_probe_agg', 'rows', ['sum', 'count']), undefined), [W1H]);
  eq('anchor W2: scalar {min,max,count_col} => [W2H]', deriveTemplateHashes(planFrom('wh_probe_agg', 'scalar', ['min', 'max', 'count_col']), undefined), [W2H]);
  eq('anchor W3: scalar {avg_pair} => [W3H]', deriveTemplateHashes(planFrom('wh_probe_agg', 'scalar', ['avg_pair']), undefined), [W3H]);
  eq('anchor W5: scalar {sum,count_col} on facts_blocks => [W5H]', deriveTemplateHashes(planFrom('facts_blocks', 'scalar', ['sum', 'count_col']), undefined), [W5H]);
});

Deno.test('r69 battery core: WH_RPC_ELIGIBLE_HASHES === the manifest FILE W1/W2/W3 hashes (F-N5 provenance cross-check)', () => {
  // The core cell pins the set against literals; THIS cell re-reads the
  // manifest FILE (wh_handshake_test.ts file-pin house style) so the
  // eligible set is pinned to the seed's real sha256 values, not to a
  // transcription of a transcription.
  let rows: { slug: string; template_hash: string }[] | null = null;
  try {
    rows = JSON.parse(Deno.readTextFileSync(new URL('../../../db/shard-templates/manifest.json', import.meta.url))) as { slug: string; template_hash: string }[];
  } catch {
    rows = null;
  }
  const byHash = ENGINE_TEMPLATE_MANIFEST.map((r) => r.template_hash);
  if (rows !== null) {
    const fileBySlug = Object.fromEntries(rows.map((r) => [r.slug, r.template_hash]));
    eq('eligible set === the FILE W1/W2/W3 hashes in manifest order', [...WH_RPC_ELIGIBLE_HASHES], [
      fileBySlug['W1_grouped_sum_count'],
      fileBySlug['W2_scalar_minmax'],
      fileBySlug['W3_scalar_avg_pair'],
    ]);
    eqTrue('W4 not eligible (file hash)', !WH_RPC_ELIGIBLE_HASHES.includes(fileBySlug['W4_topk']));
    eqTrue('W5-class not eligible (file hash)', !WH_RPC_ELIGIBLE_HASHES.includes(fileBySlug['W5_cold_agg']));
  } else {
    console.log('  note [fallback branch: manifest file not readable — the engine-manifest cross-check below carries the pin]');
    eq('eligible set === the ENGINE manifest W1/W2/W3 hashes in manifest order', [...WH_RPC_ELIGIBLE_HASHES], [byHash[0], byHash[1], byHash[2]]);
  }
  eqTrue('the eligible set is a SUBSET of the engine manifest (plan-honesty can never self-reject an rpc plan)', [...WH_RPC_ELIGIBLE_HASHES].every((h) => byHash.includes(h)));
});

Deno.test('r69 battery core: the call site stamps o.shard onto the adapted envelope (merge-exclusion attribution — an unstamped call site dies at the wh_merge shard gate instead)', async () => {
  const res = await executeWhQuery(rpcPlaneArgs({
    directoryRows: [dirRow('shard-a'), dirRow('shard-b')],
    fetcher: ((shard: string) =>
      Promise.resolve({
        ok: true as const,
        envelope: shard === 'shard-a'
          // A lying group-key arity (2 slots vs the plan's 1): the ADAPTER
          // passes it (the remap is arity-blind by design), wh_merge's P-law
          // does not — the exclusion must attribute to the STAMPED shard.
          ? w1Wire({ rows: [{ k: ['eu', 'extra'], a: { x: '1200.50', c: 14 } }, { k: ['us'], a: { x: '900.25', c: 7 } }] })
          : w1Wire(),
        estRows: 2,
      })) as WhShardFetcher,
  }));
  eq('the lying shard is excluded, the honest one merges', [res.coverage, res.partial], ['1/2', true]);
  eq('the exclusion warning names the STAMPED shard', res.warnings.map((w) => w.shard), ['shard-a']);
  eq('warning code excluded', res.warnings[0]?.code, 'excluded');
  eqTrue(
    'the detail is the wh_merge ARITY law — not the shard-gate envelope_invalid (the stamp demonstrably reached wh_merge; an unstamped call site would fail envelope.shard FIRST with a different detail and an undefined shard)',
    (res.warnings[0]?.detail ?? '').includes('group key arity 2 !== plan arity 1'),
  );
});

// =============================================================================
// r123 WH_PROXY rawFetch lever (design_r122_acct2_proxy.md §1.3/§1.4 + the
// r123 audit adoptions ⟫A5 validator mirror / ⟫A9 browser_headers / ⟫B4
// boot-once / ⟫B5 burst pin).
// =============================================================================
// Offline + pure: the SHELL owns every env read and the boot-once KV read;
// these cells pin the PURE halves (parseWhProxyMapValue — the engine-side
// FM-0019 validator mirror — and makeProxiedRawFetch — the spec transform)
// plus the seam integration through makeRpcShardFetcher (the D7/D8
// never-throw arms work UNCHANGED behind the proxy). The shell's inert-path
// byte-identical wiring is pinned statically in wh_entrypoint_test.ts
// (mirror of the rpcMode unset precedent).

const WH_PROXY_URL = 'https://bnwwjapoisfzkldtqzst.supabase.co/functions/v1/proxy';
const WH_PROXY_TOKEN = 'wh-proxy-test-token'; // test fake — a real deployment reads WH_PROXY_TOKEN
const WH_TARGET_RPC_URL = 'https://shardaexampleexampl1.supabase.co/rest/v1/rpc/wh_query';

Deno.test('r123 parseWhProxyMapValue: well-formed value => {url, refs} verbatim, ZERO defect logs', () => {
  const defects: string[] = [];
  const v = parseWhProxyMapValue(
    { url: WH_PROXY_URL, refs: ['bnwwjapoisfzkldtqzst', 'shardaexampleexampl1'] },
    (m) => defects.push(m),
  );
  eqTrue('valid value accepted', v !== null);
  eq('url verbatim', v?.url, WH_PROXY_URL);
  eq('refs verbatim (order preserved)', v?.refs, ['bnwwjapoisfzkldtqzst', 'shardaexampleexampl1']);
  eq('no defect log on the happy arm', defects.length, 0);
});

Deno.test('r123 parseWhProxyMapValue: non-object values (null/array/scalar) => not-an-object class, ONE fixed log', () => {
  const fixed = 'wh_shard_proxy_map defect class not-an-object: config value is not a JSON object — WH_PROXY_FETCHER=on stays inert (default platform fetch in use)';
  for (const bad of [null, 'https://x.supabase.co', 42, [], true]) {
    const defects: string[] = [];
    const v = parseWhProxyMapValue(bad, (m) => defects.push(m));
    eqTrue(`not-an-object ${show(bad)}: rejected`, v === null);
    eq(`not-an-object ${show(bad)}: the FIXED string (echo law)`, defects, [fixed]);
  }
});

Deno.test('r123 parseWhProxyMapValue: extra member / missing member / wrong-typed members => invalid-members class', () => {
  const fixed = 'wh_shard_proxy_map defect class invalid-members: config value is not an object with exactly {url: string, refs: string[]} — WH_PROXY_FETCHER=on stays inert (default platform fetch in use)';
  const cases: [string, unknown][] = [
    ['extra member', { url: WH_PROXY_URL, refs: ['bnwwjapoisfzkldtqzst'], extra: 1 }],
    ['missing refs', { url: WH_PROXY_URL }],
    ['missing url', { refs: ['bnwwjapoisfzkldtqzst'] }],
    ['url not a string', { url: 42, refs: ['bnwwjapoisfzkldtqzst'] }],
    ['refs not an array', { url: WH_PROXY_URL, refs: 'bnwwjapoisfzkldtqzst' }],
    ['refs entry not a string', { url: WH_PROXY_URL, refs: [42] }],
  ];
  for (const [name, bad] of cases) {
    const defects: string[] = [];
    const v = parseWhProxyMapValue(bad, (m) => defects.push(m));
    eqTrue(`${name}: rejected`, v === null);
    eq(`${name}: the FIXED invalid-members log`, defects, [fixed]);
  }
});

Deno.test('r123 parseWhProxyMapValue: url shape faults => bad-url class (raw byte shape — scheme, trailing dot, path, case, ref length)', () => {
  const cases: [string, string][] = [
    ['http scheme', 'http://bnwwjapoisfzkldtqzst.supabase.co/functions/v1/proxy'],
    ['trailing-dot host (⟫A7 raw-exact stance)', 'https://bnwwjapoisfzkldtqzst.supabase.co./functions/v1/proxy'],
    ['rest path not functions', 'https://bnwwjapoisfzkldtqzst.supabase.co/rest/v1/rpc/wh_query'],
    ['path suffix', 'https://bnwwjapoisfzkldtqzst.supabase.co/functions/v1/proxyX'],
    ['host ref short', 'https://shortref.supabase.co/functions/v1/proxy'],
    ['uppercase ref', 'https://BNWWJAPOISFZKLDTQZST.supabase.co/functions/v1/proxy'],
  ];
  for (const [name, url] of cases) {
    const defects: string[] = [];
    const v = parseWhProxyMapValue({ url, refs: ['bnwwjapoisfzkldtqzst'] }, (m) => defects.push(m));
    eqTrue(`bad-url ${name}: rejected`, v === null);
    eqTrue(`bad-url ${name}: ONE fixed log naming the class`, defects.length === 1 && defects[0]!.includes('defect class bad-url'));
  }
});

Deno.test('r123 parseWhProxyMapValue: ref-entry shape faults => bad-ref class', () => {
  const cases: [string, string[]][] = [
    ['19 chars', ['bnwwjapoisfzkldtqzs']],
    ['21 chars', ['bnwwjapoisfzkldtqzstt']],
    ['uppercase', ['BNWWJAPOISFZKLDTQZST']],
    ['dotted host', ['bnwwjapoisfzkldtqzst.supabase.co']],
  ];
  for (const [name, refs] of cases) {
    const defects: string[] = [];
    const v = parseWhProxyMapValue({ url: WH_PROXY_URL, refs }, (m) => defects.push(m));
    eqTrue(`bad-ref ${name}: rejected`, v === null);
    eqTrue(`bad-ref ${name}: ONE fixed log naming the class`, defects.length === 1 && defects[0]!.includes('defect class bad-ref'));
  }
});

Deno.test('r123 parseWhProxyMapValue: host-ref not in refs => url-not-in-refs class (consistency, fail-closed)', () => {
  const defects: string[] = [];
  const v = parseWhProxyMapValue({ url: WH_PROXY_URL, refs: ['shardaexampleexampl1'] }, (m) => defects.push(m));
  eqTrue('rejected', v === null);
  eqTrue('ONE fixed log naming the class', defects.length === 1 && defects[0]!.includes('defect class url-not-in-refs'));
});

Deno.test('r123 parseWhProxyMapValue: JWT-shaped string ANYWHERE => jwt-shaped-string class (token NEVER in config — ENFORCED)', () => {
  const cases: [string, unknown][] = [
    ['uppercase JWT refs entry', { url: WH_PROXY_URL, refs: ['eyJhbGciOiJIUzI1NiIsInR5'] }],
    // the lethal one: 20-char ALL-LOWERCASE eyJ-prefixed string that WOULD
    // pass ^[a-z0-9]{20}$ — the belt must catch it (class jwt, not bad-ref)
    ['lowercase 20-char eyJ ref', { url: WH_PROXY_URL, refs: ['eyJabcdefghijklmnopq'] }],
  ];
  for (const [name, value] of cases) {
    const defects: string[] = [];
    const v = parseWhProxyMapValue(value, (m) => defects.push(m));
    eqTrue(`jwt ${name}: rejected`, v === null);
    eqTrue(`jwt ${name}: ONE fixed log naming the class`, defects.length === 1 && defects[0]!.includes('defect class jwt-shaped-string'));
    eqTrue(`jwt ${name}: echo law — the log carries NO value fragment`, !defects[0]!.includes('eyJhb') && !defects[0]!.includes('eyJabc'));
  }
});

Deno.test('r123 proxiedRawFetch: spec shape — POST to the proxy URL, Bearer WH_PROXY_TOKEN, body EXACTLY {url, http_method, headers, body, cache:0, browser_headers:false}', async () => {
  const calls: RawCall[] = [];
  const proxied = makeProxiedRawFetch({
    proxyUrl: WH_PROXY_URL,
    proxyToken: WH_PROXY_TOKEN,
    fetchImpl: fakeRaw({ calls, status: 200, headers: { 'content-type': 'application/json' }, body: JSON.stringify(w1Wire()) }),
  });
  const fetcher = makeRpcShardFetcher({ resolveKey: () => 'sk-test-key', rawFetch: proxied });
  const rpc: WhRpcSpec = { p_template_hash: W1H, p_params: { p_min: 1 } };
  const out = await fetcher('shard-a', WH_TARGET_RPC_URL, rpc);
  eqTrue('the relayed 200 wire resolves (the D8 ok-arm unchanged behind the proxy)', out.ok === true);
  eq('exactly ONE transport call — at the PROXY url', calls.length, 1);
  const call = calls[0]!;
  eq('proxy request URL = the acct2 proxy fn (absolute)', call.url, WH_PROXY_URL);
  eq('outer method POST', call.init.method, 'POST');
  eq('outer Authorization = Bearer WH_PROXY_TOKEN (the dedicated plane)', call.init.headers['Authorization'], `Bearer ${WH_PROXY_TOKEN}`);
  eq('outer Content-Type = the spec envelope', call.init.headers['Content-Type'], 'application/json');
  eq('outer headers are EXACTLY the two spec-plane headers', Object.keys(call.init.headers).sort().join(','), 'Authorization,Content-Type');
  const spec = JSON.parse(call.init.body!) as Record<string, unknown>;
  eq('spec members EXACTLY {url, http_method, headers, body, cache, browser_headers}', Object.keys(spec).sort(), ['body', 'browser_headers', 'cache', 'headers', 'http_method', 'url']);
  eq('spec.url = the ABSOLUTE target url verbatim', spec.url, WH_TARGET_RPC_URL);
  eqTrue('spec.url is an absolute https url', typeof spec.url === 'string' && (spec.url as string).startsWith('https://'));
  eq('spec.http_method mirrors init.method', spec.http_method, 'POST');
  eq('spec.headers = the shard auth headers VERBATIM (shard creds ride the spec)', spec.headers, {
    'Accept-Profile': 'public',
    apikey: 'sk-test-key',
    Authorization: 'Bearer sk-test-key',
    'Content-Type': 'application/json',
  });
  eq('spec.body = the rpc body string verbatim', spec.body, JSON.stringify({ p_template_hash: W1H, p_params: { p_min: 1 } }));
  eq('spec.cache = the NUMBER 0 (defeats the ep 300s default TTL on both layers)', spec.cache, 0);
  eqTrue('spec.cache is typeof number (never the string "0")', typeof spec.cache === 'number');
  eq('spec.browser_headers = false (⟫A9 — no browser-masquerade injection)', spec.browser_headers, false);
});

Deno.test('r123 proxiedRawFetch: residual select GET arm — http_method GET, spec.body null, headers verbatim (no Content-Type)', async () => {
  const calls: RawCall[] = [];
  const proxied = makeProxiedRawFetch({ proxyUrl: WH_PROXY_URL, proxyToken: WH_PROXY_TOKEN, fetchImpl: fakeRaw({ calls, status: 200, body: '' }) });
  const fetcher = makeRpcShardFetcher({ resolveKey: () => 'sk-test-key', rawFetch: proxied });
  const out = await fetcher('shard-a', 'https://shardaexampleexampl1.supabase.co/rest/v1/wh_rows?select=%2A');
  eqTrue('the GET arm still degrades loudly at the fetcher (select-arm refusal unchanged)', out.ok === false && out.warning.code === 'shard_path_select_disabled');
  const spec = JSON.parse(calls[0]!.init.body!) as Record<string, unknown>;
  eq('spec.http_method GET', spec.http_method, 'GET');
  eq('spec.body null (init.body undefined ?? null)', spec.body, null);
  eq('spec.headers verbatim — NO Content-Type on the GET arm', spec.headers, {
    'Accept-Profile': 'public',
    apikey: 'sk-test-key',
    Authorization: 'Bearer sk-test-key',
  });
  eqTrue('outer transport stays POST + Bearer (the proxy plane is POST-only)', calls[0]!.init.method === 'POST' && calls[0]!.init.headers['Authorization'] === `Bearer ${WH_PROXY_TOKEN}`);
});

Deno.test('r123 passthrough: the proxy Response is consumed VERBATIM — 200 wire body relays unchanged (D8 + crash-gate work unchanged)', async () => {
  const wire = w1Wire();
  const proxied = makeProxiedRawFetch({
    proxyUrl: WH_PROXY_URL,
    proxyToken: WH_PROXY_TOKEN,
    fetchImpl: fakeRaw({ status: 200, headers: { 'content-type': 'application/json' }, body: JSON.stringify(wire) }),
  });
  const fetcher = makeRpcShardFetcher({ resolveKey: () => 'sk-test-key', rawFetch: proxied });
  const out = await fetcher('shard-a', WH_TARGET_RPC_URL, { p_template_hash: W1H, p_params: {} });
  eqTrue('ok arm', out.ok === true);
  eq('envelope = the proxied body, parsed VERBATIM (no re-shaping at the seam)', (out as { envelope: unknown }).envelope, wire);
  eq('estRows from the relayed rowCount', (out as { estRows?: number }).estRows, 2);
});

Deno.test('r123 passthrough: proxy-generated 403 JSON error relays as a non-2xx warning — no throw, no code misclassification', async () => {
  const proxied = makeProxiedRawFetch({
    proxyUrl: WH_PROXY_URL,
    proxyToken: WH_PROXY_TOKEN,
    fetchImpl: fakeRaw({ status: 403, headers: { 'content-type': 'application/json' }, body: JSON.stringify({ error: 'forbidden', code: 'PROXY_FORBIDDEN' }) }),
  });
  const fetcher = makeRpcShardFetcher({ resolveKey: () => 'sk-test-key', rawFetch: proxied });
  const out = await fetcher('shard-a', WH_TARGET_RPC_URL, { p_template_hash: W1H, p_params: {} });
  eqTrue('non-2xx warning arm (no exception)', out.ok === false);
  eq('warning = {httpStatus:403} EXACTLY (4xx: stamped ABSENT; PROXY_FORBIDDEN can never match ^WH[0-9]{3}$)', out.ok === false ? out.warning : undefined, { httpStatus: 403 });
});

Deno.test('r123 passthrough: proxy 502 with application/json → advisory stamped:true (the D8 header-PRESENCE law unchanged through the proxy)', async () => {
  const proxied = makeProxiedRawFetch({
    proxyUrl: WH_PROXY_URL,
    proxyToken: WH_PROXY_TOKEN,
    fetchImpl: fakeRaw({ status: 502, headers: { 'content-type': 'application/json' }, body: JSON.stringify({ error: 'bad gateway', code: 'DNS_FAILED' }) }),
  });
  const fetcher = makeRpcShardFetcher({ resolveKey: () => 'sk-test-key', rawFetch: proxied });
  const out = await fetcher('shard-a', WH_TARGET_RPC_URL, { p_template_hash: W1H, p_params: {} });
  eq('warning = {httpStatus:502, stamped:true} (DNS_FAILED never matches ^WH[0-9]{3}$ — no misroute)', out.ok === false ? out.warning : undefined, { httpStatus: 502, stamped: true });
});

Deno.test('r123 never-throw: the 403/429/502 sweep — the proxied fetcher NEVER throws, every arm a non-2xx warning', async () => {
  for (const status of [403, 429, 502]) {
    const proxied = makeProxiedRawFetch({
      proxyUrl: WH_PROXY_URL,
      proxyToken: WH_PROXY_TOKEN,
      fetchImpl: fakeRaw({ status, headers: { 'content-type': 'application/json' }, body: JSON.stringify({ error: 'proxy refusal', code: 'PROXY_DOWN' }) }),
    });
    const fetcher = makeRpcShardFetcher({ resolveKey: () => 'sk-test-key', rawFetch: proxied });
    let out: Awaited<ReturnType<typeof fetcher>> | undefined;
    let threw = false;
    try {
      out = await fetcher('shard-a', WH_TARGET_RPC_URL, { p_template_hash: W1H, p_params: {} });
    } catch {
      threw = true;
    }
    eqTrue(`proxy ${status}: NO exception`, !threw);
    eqTrue(`proxy ${status}: warning arm`, out?.ok === false);
    eq(`proxy ${status}: httpStatus relayed`, out && out.ok === false ? out.warning.httpStatus : undefined, status);
  }
});

Deno.test('r123 never-throw: transport rejection INSIDE the proxied fetch => the fetcher arm-0 network warning (seam contract preserved — the wrapper does NOT swallow)', async () => {
  const calls: RawCall[] = [];
  const proxied = makeProxiedRawFetch({ proxyUrl: WH_PROXY_URL, proxyToken: WH_PROXY_TOKEN, fetchImpl: fakeRaw({ reject: true, calls }) });
  const fetcher = makeRpcShardFetcher({ resolveKey: () => 'sk-test-key', rawFetch: proxied });
  let out: Awaited<ReturnType<typeof fetcher>> | undefined;
  let threw = false;
  try {
    out = await fetcher('shard-a', WH_TARGET_RPC_URL, { p_template_hash: W1H, p_params: {} });
  } catch {
    threw = true;
  }
  eqTrue('NO exception escapes the fetcher', !threw);
  eq('the SEAM contract catch classifies it (arm-0 network — swallowing in the wrapper would MISclassify a network fault as an http warning)', out && out.ok === false ? out.warning : undefined, { code: 'network' });
  eqTrue('the rejection happened at the PROXY transport (one call attempted)', calls.length === 1);
});

Deno.test('r123 ⟫B5 burst pin: RATE_LIMITED-shaped 429 (+Retry-After) → engine warning arm, NO throw, no WH-code misroute', async () => {
  const proxied = makeProxiedRawFetch({
    proxyUrl: WH_PROXY_URL,
    proxyToken: WH_PROXY_TOKEN,
    fetchImpl: fakeRaw({
      status: 429,
      headers: { 'content-type': 'application/json', 'retry-after': '60' },
      body: JSON.stringify({ error: 'rate limit exceeded', code: 'RATE_LIMITED' }),
    }),
  });
  const fetcher = makeRpcShardFetcher({ resolveKey: () => 'sk-test-key', rawFetch: proxied });
  let out: Awaited<ReturnType<typeof fetcher>> | undefined;
  let threw = false;
  try {
    out = await fetcher('shard-a', WH_TARGET_RPC_URL, { p_template_hash: W1H, p_params: {} });
  } catch {
    threw = true;
  }
  eqTrue('rate-limit burst: NO exception (the per-shard degrade arm — never a /query hard-fail by itself)', !threw);
  eq('warning = {httpStatus:429} EXACTLY — RATE_LIMITED can never match ^WH[0-9]{3}$ so the code field stays ABSENT (no D8 misroute; audit-B verified)', out && out.ok === false ? out.warning : undefined, { httpStatus: 429 });
});

// =============================================================================
// r123 P0/P3 fixes (the live-e2e RED closure + the fresh-eyes P3s).
// =============================================================================
// P0: makeOwnRefBypassRawFetch — the OWN-REF DIRECT carve-out (the co-hosted
// law: the engine's own shard is intra-project and NEVER rides the
// cross-account proxy; the v14 live e2e drew a relayed 401 "Invalid API key"
// for the own-ref credential through the acct2 proxy while the same
// credential succeeds direct). The bypass is EXACT — only the engine's own
// host bypasses — and preserves the never-throw seam. The SHELL wires it with
// the authoritative self-ref source it ALREADY holds (ownRef =
// ownProjectRefFromSupabaseUrl(SUPABASE_URL) — geo_write_fence.ts, the same
// value feeding the shard-key resolver; the co-hosted law). P3s:
// raceWhProxyKvBoot (hanging PostgREST ⇒ KV-absent, never a wedged boot) and
// the empty-refs reject (FM 0027 token-equivalence). The lever-gated shell
// wiring itself is pinned statically in wh_entrypoint_test.ts.

const WH_OWN_REF = 'blnkbdwpxjizgpggcdqj'; // the engine's OWN host ref (the r123 live e2e FM self-shard)
const WH_OWN_TARGET_RPC_URL = `https://${WH_OWN_REF}.supabase.co/rest/v1/rpc/wh_query`;

Deno.test('r123 P3 parseWhProxyMapValue: EMPTY refs array => empty-refs class (FM 0027 token-equivalence — non-empty required BOTH sides)', () => {
  const defects: string[] = [];
  const v = parseWhProxyMapValue({ url: WH_PROXY_URL, refs: [] }, (m) => defects.push(m));
  eqTrue('empty refs: rejected', v === null);
  eq('the FIXED empty-refs log (echo law)', defects, [
    'wh_shard_proxy_map defect class empty-refs: refs must be a NON-EMPTY array — WH_PROXY_FETCHER=on stays inert (default platform fetch in use)',
  ]);
  // the mirror stays token-equivalent WITHOUT widening: a non-empty valid
  // value is still accepted (the happy-path cell above pins that arm).
});

Deno.test('r123 P0 ownRefBypass: OWN host => the DEFAULT platform fetch receives (url, init) VERBATIM — the proxied transform is NOT invoked', async () => {
  const proxiedCalls: RawCall[] = [];
  const directCalls: RawCall[] = [];
  const bypass = makeOwnRefBypassRawFetch({
    ownRef: WH_OWN_REF,
    proxiedRawFetch: makeProxiedRawFetch({
      proxyUrl: WH_PROXY_URL,
      proxyToken: WH_PROXY_TOKEN,
      fetchImpl: fakeRaw({ calls: proxiedCalls, status: 200, body: JSON.stringify(w1Wire()) }),
    }),
    directFetch: fakeRaw({ calls: directCalls, status: 200, headers: { 'content-type': 'application/json' }, body: JSON.stringify(w1Wire()) }),
  });
  const out = await bypass(WH_OWN_TARGET_RPC_URL, { method: 'POST', headers: { apikey: 'sk-own' }, body: '{"p":1}' });
  eqTrue('the own-host fetch resolves via the DIRECT transport', (await out.text()).length > 0);
  eq('the proxied transform invoked ZERO times (the own-ref P0 regression: own traffic NEVER rides the proxy)', proxiedCalls.length, 0);
  eq('the direct transport invoked EXACTLY once', directCalls.length, 1);
  eq('direct call url = the TARGET url verbatim (no proxy rewrite)', directCalls[0]!.url, WH_OWN_TARGET_RPC_URL);
  eq('direct init VERBATIM (method)', directCalls[0]!.init.method, 'POST');
  eq('direct init VERBATIM (shard headers ride unchanged — the own service key stays on the isolate-internal path)', directCalls[0]!.init.headers, { apikey: 'sk-own' });
  eq('direct init VERBATIM (body)', directCalls[0]!.init.body, '{"p":1}');
});

Deno.test('r123 P0 ownRefBypass: REMOTE host => the proxied transform invoked (spec POST to the proxy fn) — the carve-out is EXACT', async () => {
  const proxiedCalls: RawCall[] = [];
  const directCalls: RawCall[] = [];
  const bypass = makeOwnRefBypassRawFetch({
    ownRef: WH_OWN_REF,
    proxiedRawFetch: makeProxiedRawFetch({
      proxyUrl: WH_PROXY_URL,
      proxyToken: WH_PROXY_TOKEN,
      fetchImpl: fakeRaw({ calls: proxiedCalls, status: 200, headers: { 'content-type': 'application/json' }, body: JSON.stringify(w1Wire()) }),
    }),
    directFetch: fakeRaw({ calls: directCalls, status: 200, body: '' }),
  });
  const out = await bypass(WH_TARGET_RPC_URL, { method: 'POST', headers: { apikey: 'sk-remote' }, body: '{"p":1}' });
  eqTrue('the remote fetch resolves through the proxy transform', (await out.text()).length > 0);
  eq('the direct transport invoked ZERO times (remote shards ride the acct2 egress pool — the lever purpose)', directCalls.length, 0);
  eq('the proxied transform invoked EXACTLY once — at the PROXY url', proxiedCalls.length, 1);
  eq('proxied call url = the acct2 proxy fn', proxiedCalls[0]!.url, WH_PROXY_URL);
  eq('the spec carries the REMOTE target verbatim', JSON.parse(proxiedCalls[0]!.init.body!).url, WH_TARGET_RPC_URL);
});

Deno.test('r123 P0 ownRefBypass: the carve-out is EXACT — lookalike hosts stay on the proxy (M2 prefix family: ref-extension, host-suffix, case, port, trailing-dot)', async () => {
  // Every lookalike below is a byte-EXACT miss on the needle
  // `https://<ownRef>.supabase.co/` (the trailing `/` is load-bearing — the
  // r69 M2 prefix-mutant class). A miss ⇒ PROXY (fail-closed conservative):
  // the bypass never grants a host the exact byte comparison did not name.
  const cases: [string, string][] = [
    ['ref-EXTENDED spelling (M2: ownRef prefix of a longer ref)', `https://${WH_OWN_REF}x.supabase.co/rest/v1/rpc/wh_query`],
    ['host-SUFFIX lookalike (⟫A1: not self-host either)', `https://${WH_OWN_REF}.supabase.co.evil.io/rest/v1/rpc/wh_query`],
    ['CASE variation (DNS-identical but byte-miss — engine builds urls lowercase)', `https://${WH_OWN_REF.toUpperCase()}.supabase.co/rest/v1/rpc/wh_query`],
    ['explicit PORT (default-443 byte shape only)', `https://${WH_OWN_REF}.supabase.co:8443/rest/v1/rpc/wh_query`],
    ['TRAILING-DOT host (⟫A7 raw-exact stance)', `https://${WH_OWN_REF}.supabase.co./rest/v1/rpc/wh_query`],
  ];
  for (const [name, url] of cases) {
    const proxiedCalls: RawCall[] = [];
    const directCalls: RawCall[] = [];
    const bypass = makeOwnRefBypassRawFetch({
      ownRef: WH_OWN_REF,
      proxiedRawFetch: makeProxiedRawFetch({ proxyUrl: WH_PROXY_URL, proxyToken: WH_PROXY_TOKEN, fetchImpl: fakeRaw({ calls: proxiedCalls, status: 200, body: '' }) }),
      directFetch: fakeRaw({ calls: directCalls, status: 200, body: '' }),
    });
    await bypass(url, { method: 'GET', headers: { apikey: 'sk' } });
    eqTrue(`lookalike ${name}: PROXIED (not bypassed)`, proxiedCalls.length === 1 && proxiedCalls[0]!.url === WH_PROXY_URL);
    eqTrue(`lookalike ${name}: the direct transport NEVER fired`, directCalls.length === 0);
  }
});

Deno.test('r123 P0 ownRefBypass: ownRef "" DISABLES the arm (AM-8 mirror) — every host proxied; the shell ALSO refuses to arm on "" (static pin)', async () => {
  const proxiedCalls: RawCall[] = [];
  const directCalls: RawCall[] = [];
  const bypass = makeOwnRefBypassRawFetch({
    ownRef: '',
    proxiedRawFetch: makeProxiedRawFetch({ proxyUrl: WH_PROXY_URL, proxyToken: WH_PROXY_TOKEN, fetchImpl: fakeRaw({ calls: proxiedCalls, status: 200, body: '' }) }),
    directFetch: fakeRaw({ calls: directCalls, status: 200, body: '' }),
  });
  await bypass(WH_OWN_TARGET_RPC_URL, { method: 'GET', headers: {} });
  eqTrue('empty ownRef: the OWN-host url goes to the PROXY (the arm can never match)', proxiedCalls.length === 1 && proxiedCalls[0]!.url === WH_PROXY_URL);
  eqTrue('empty ownRef: the direct transport NEVER fired', directCalls.length === 0);
  // Note: the shell never CONSTRUCTS the wrapper with ownRef '' — the arming
  // gate treats an unparseable SUPABASE_URL as a lever defect (inert, ONE
  // fixed-string log). This factory arm is the belt-and-braces backstop.
});

Deno.test('r123 P0 ownRefBypass: never-throw seam preserved on BOTH transports — rejections reach the fetcher arm-0 network warning (nothing swallowed by the wrapper)', async () => {
  // own host, DIRECT transport rejects
  {
    const bypass = makeOwnRefBypassRawFetch({
      ownRef: WH_OWN_REF,
      proxiedRawFetch: makeProxiedRawFetch({ proxyUrl: WH_PROXY_URL, proxyToken: WH_PROXY_TOKEN, fetchImpl: fakeRaw({ status: 200, body: '' }) }),
      directFetch: fakeRaw({ reject: true }),
    });
    const fetcher = makeRpcShardFetcher({ resolveKey: () => 'sk-own-key', rawFetch: bypass });
    let out: Awaited<ReturnType<typeof fetcher>> | undefined;
    let threw = false;
    try {
      out = await fetcher(WH_OWN_REF, WH_OWN_TARGET_RPC_URL, { p_template_hash: W1H, p_params: {} });
    } catch {
      threw = true;
    }
    eqTrue('own host + rejecting DIRECT transport: NO exception escapes the fetcher', !threw);
    eq('the SEAM contract catch classifies it (arm-0 network)', out && out.ok === false ? out.warning : undefined, { code: 'network' });
  }
  // remote host, PROXY transport rejects (the bypass must not add catching)
  {
    const bypass = makeOwnRefBypassRawFetch({
      ownRef: WH_OWN_REF,
      proxiedRawFetch: makeProxiedRawFetch({ proxyUrl: WH_PROXY_URL, proxyToken: WH_PROXY_TOKEN, fetchImpl: fakeRaw({ reject: true }) }),
      directFetch: fakeRaw({ status: 200, body: '' }),
    });
    const fetcher = makeRpcShardFetcher({ resolveKey: () => 'sk-remote-key', rawFetch: bypass });
    let out: Awaited<ReturnType<typeof fetcher>> | undefined;
    let threw = false;
    try {
      out = await fetcher('shard-a', WH_TARGET_RPC_URL, { p_template_hash: W1H, p_params: {} });
    } catch {
      threw = true;
    }
    eqTrue('remote host + rejecting PROXY transport: NO exception escapes the fetcher', !threw);
    eq('arm-0 network unchanged behind the bypass', out && out.ok === false ? out.warning : undefined, { code: 'network' });
  }
});

Deno.test('r123 P0 ownRefBypass INTEGRATION: the shell wiring shape — ONE resolver instance + the bypass wrapper; own shard DIRECT with the OWN key, remote shard PROXIED with the remote key', async () => {
  const proxiedCalls: RawCall[] = [];
  const directCalls: RawCall[] = [];
  // D6 single source of truth, exactly as the shell wires it: the resolver
  // feeds BOTH the fetcher's key resolution AND (transitively) the target
  // identity — the own-ref DIRECT carve-out keys off the same own ref.
  const resolveKey = makeShardKeyResolver({
    ownRef: WH_OWN_REF,
    ownKey: 'sk-own-service-key',
    remoteKeys: new Map([['shardaexampleexampl1', 'sk-remote-service-key']]),
  });
  const bypass = makeOwnRefBypassRawFetch({
    ownRef: WH_OWN_REF,
    proxiedRawFetch: makeProxiedRawFetch({
      proxyUrl: WH_PROXY_URL,
      proxyToken: WH_PROXY_TOKEN,
      fetchImpl: fakeRaw({ calls: proxiedCalls, status: 200, headers: { 'content-type': 'application/json' }, body: JSON.stringify(w1Wire()) }),
    }),
    directFetch: fakeRaw({ calls: directCalls, status: 200, headers: { 'content-type': 'application/json' }, body: JSON.stringify(w1Wire()) }),
  });
  const fetcher = makeRpcShardFetcher({ resolveKey, rawFetch: bypass });
  // the OWN shard: direct, own key, zero proxy involvement (the P0 regression cell)
  const ownOut = await fetcher(WH_OWN_REF, WH_OWN_TARGET_RPC_URL, { p_template_hash: W1H, p_params: {} });
  eqTrue('own shard: 200 envelope via the DIRECT transport', ownOut.ok === true);
  eq('own shard: direct call rides the OWN service key (isolate-internal path)', directCalls[0]!.init.headers['Authorization'], 'Bearer sk-own-service-key');
  eqTrue('own shard: the proxy was NEVER contacted', proxiedCalls.length === 0);
  // a REMOTE shard: proxied, remote key (the acct2 egress-pool purpose)
  const remoteOut = await fetcher('shardaexampleexampl1', WH_TARGET_RPC_URL, { p_template_hash: W1H, p_params: {} });
  eqTrue('remote shard: 200 envelope through the PROXY', remoteOut.ok === true);
  eqTrue('remote shard: the direct transport NEVER fired for it', directCalls.length === 1);
  const spec = JSON.parse(proxiedCalls[0]!.init.body!) as { headers: Record<string, string> };
  eq('remote shard: the spec carries the REMOTE service key verbatim', spec.headers['Authorization'], 'Bearer sk-remote-service-key');
});

Deno.test('r123 P3 raceWhProxyKvBoot: a KV read that NEVER settles => the timedOut() KV-absent sentinel resolves (lever inert path, no wedged boot)', async () => {
  const never = new Promise<{ data: unknown; error: unknown }>(() => {}); // the hanging-PostgREST fake
  let timedOutFired = 0;
  const out = await raceWhProxyKvBoot(
    never,
    5, // short injectable timeout — the 10s policy is pinned separately
    () => {
      timedOutFired++;
      return { data: null, error: null };
    },
  );
  eqTrue('the timeout arm resolves with the KV-ABSENT shape (the shell absent-row arm owns the inert log)', out.data === null && out.error === null);
  eq('the timedOut callback fired EXACTLY once', timedOutFired, 1);
});

Deno.test('r123 P3 raceWhProxyKvBoot: a read that settles FIRST wins — value verbatim, timedOut NEVER fires, the 10s timer CLEARED (no dangling op)', async () => {
  const read = new Promise<{ data: unknown; error: unknown }>((resolve) => {
    setTimeout(() => resolve({ data: { value: 'kv' }, error: null }), 5);
  });
  const out = await raceWhProxyKvBoot(read, WH_PROXY_KV_BOOT_TIMEOUT_MS, () => {
    throw new Error('timedOut must NOT fire when the read wins');
  });
  eq('the read value verbatim', out.data, { value: 'kv' });
  eq('the read error verbatim', out.error, null);
  // the finally-cleared 10s timer is what lets this test finish without the
  // sanitizer flagging a pending op — the clear is LOAD-BEARING here.
});

Deno.test('r123 P3 raceWhProxyKvBoot: a read that REJECTS before the timeout propagates UNCHANGED (the shell catch arm — nothing swallowed into the sentinel)', async () => {
  const boom = new Promise<{ data: unknown; error: unknown }>((_, reject) => {
    setTimeout(() => reject(new Error('postgrest reset by peer')), 5);
  });
  let threw = false;
  try {
    await raceWhProxyKvBoot(boom, WH_PROXY_KV_BOOT_TIMEOUT_MS, () => ({ data: null, error: null }));
  } catch {
    threw = true;
  }
  eqTrue('the rejection propagates (the shell try/catch keeps its fixed-string log arm)', threw);
});

Deno.test('r123 P3: WH_PROXY_KV_BOOT_TIMEOUT_MS is the shipped 10s policy (a hang degrades, never wedges)', () => {
  eq('the boot-read timeout const', WH_PROXY_KV_BOOT_TIMEOUT_MS, 10_000);
});

// -----------------------------------------------------------------------------
// Harness report (hand-rolled runner, no external deps) — the house gate the
// core batch OMITTED from this file (every sibling wh_* test file carries it:
// wh_engine_core_test.ts:931 / wh_handshake_test.ts:900 / wh_merge_test.ts:680
// / wh_canonical_test.ts:296). Without it the module-level `failed` counter is
// invisible to the deno exit code — a non-throwing eq failure (the COMMON
// case: an expectation flip with no escaped exception) leaves the suite
// "green". The battery's mutant dance caught it live: the M2 own-ref
// prefix-match mutant left the file at "ok | 58 passed" with three FAIL lines
// on stderr and a zero exit code. Runs LAST (declaration order) so it gates
// every cell above — core self-cells AND the battery.
// -----------------------------------------------------------------------------
Deno.test('__report__', () => {
  console.log(`\nwh_shard_channel_test: ${passed} assertions passed, ${failed} failed`);
  if (failed > 0) throw new Error(`${failed} assertion(s) failed`);
});
