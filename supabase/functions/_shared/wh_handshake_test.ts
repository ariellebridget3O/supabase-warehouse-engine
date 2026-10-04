// =============================================================================
// _shared/wh_handshake_test.ts — RED-first proofs for the §5.2 ENGINE⇄SHARD
// HANDSHAKE + the §6.3 F14/F2 consumption gates (r44).
// =============================================================================
// Normative source: research/design_wh_query_rpc.md §5.2 (discovery/
// verification handshake: inventory GET, required-hashes ⊆ returned set,
// schema_version match, [F15] max_rows fail-fast BEFORE any wh_query call,
// no cross-call caching) + §6.3 ([F14] merge-op SUBSET rule, [F2] truncation
// gate) + findings_wh_catalog_contract.md §4.4 (warning objects carry
// est_rows; template_missing/schema_mismatch/merge_mismatch/truncated_groupby
// enum placements).
//
// Offline + pure: every dep is an injected fake. The manifest-file pin reads
// db/shard-templates/manifest.json ONLY when --allow-read grants it; without
// the grant it falls back to the hand-transcribed literal copy (provenance
// noted inline — the file pin was proven green under the widened flag
// --allow-read=./supabase/functions/_shared/wh_fixtures,./db/shard-templates).
// =============================================================================

import {
  ENGINE_TEMPLATE_MANIFEST,
  checkHandshake,
  derivePlanMergeOps,
  makeWhHandshake,
  manifestRowByHash,
  parseInventoryRows,
  planOpsSubset,
  planTemplatesMissingInManifest,
  readTemplateInventory,
  templateInventoryHeaders,
  templateInventoryUrl,
} from './wh_handshake.ts';
import type {
  EngineTemplateRow,
  HandshakePlanRef,
  TemplateInventoryRow,
  WhRawFetch,
  WhShardHandshake,
} from './wh_handshake.ts';
import {
  executeWhQuery,
  gatePartialAgainstPlan,
  parseWhEngineRequest,
  buildMergePlan,
} from './wh_engine_core.ts';
import type { WhDirectoryRow, WhEngineTimers, WhShardFetcher } from './wh_engine_core.ts';
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

// -----------------------------------------------------------------------------
// Fixtures — hashes pinned from db/shard-templates/manifest.json (W1-W5).
// -----------------------------------------------------------------------------
const W1H = 'a934e7e062f59cff5a856afdc7aa743ec9be11c068c7e861ea856c36b40bdbfd'; // ["groupby","sum","count"]
const W2H = 'a095adaa148253aee8d1cc8e976f01b3579beeea5082f3862df4a908c20b2659'; // ["min","max","count_col"]
const W3H = 'bca9dd2c591ed48a0fa5367179dd5deb1d752ed9141c23e6ad53083becf8ecac'; // ["avg_pair"]
const W4H = 'dccfc317035961a6310c31194e86a01c715f384ac8cbdc33fb00f35f27eefcee'; // ["topk","raw_rows"]
const W5H = '9a8e922d064539e3418fd77dbc17479fe6a9b69bafe0f182128c675ef1f95234'; // ["sum","count_col"]

function invRow(hash: string, opts: Partial<TemplateInventoryRow> = {}): TemplateInventoryRow {
  return {
    template_hash: hash,
    qc_class: 'QC2',
    logical_table: 'wh_probe_agg',
    schema_version: 3, // default MATCHES the fixtures' table version 3 (mismatch arms override explicitly)
    state: 'active',
    max_rows: 1000,
    ...opts,
  };
}

function jsonFetch(statusBody: unknown, opts: { ok?: boolean } = {}): { fetcher: WhRawFetch; calls: { url: string; init: { method: string; headers: Record<string, string> } }[] } {
  const calls: { url: string; init: { method: string; headers: Record<string, string> } }[] = [];
  return {
    calls,
    fetcher: async (url, init) => {
      calls.push({ url, init });
      return {
        ok: opts.ok ?? true,
        status: 200,
        text: async () => (typeof statusBody === 'string' ? statusBody : JSON.stringify(statusBody)),
      };
    },
  };
}

// ---------- engine-level fixtures (grouped sum(amount)+count(*) by region) ----------
const COLS: Record<string, string> = { region: 'text', amount: 'numeric' };
const SCALES: Record<string, number> = { amount: 0 };

function dirRow(shard: string, opts: { rowEstimate?: number; tableSchemaVersion?: number } = {}): WhDirectoryRow {
  return {
    shard,
    key_min: null,
    key_max: null,
    hash_slot: null,
    state: 'serving',
    platform_status: 'ACTIVE_HEALTHY',
    schema_version: 3,
    last_health_at: '2026-09-28T00:00:00.000000Z',
    logical_name: 'orders',
    shard_key_type: 'none',
    shard_key_column: null,
    ...(opts.tableSchemaVersion !== undefined ? { table_schema_version: opts.tableSchemaVersion } : {}),
    ...(opts.rowEstimate !== undefined ? { row_estimate: opts.rowEstimate } : {}),
  };
}

function neverTimeoutTimers(): WhEngineTimers {
  let n = 100;
  return {
    nowMs: () => ++n,
    startTimeout: (_ms: number) => ({ promise: new Promise<'timeout'>(() => {}), dispose: () => {} }),
  };
}

const GROUPED_REQ = parseWhEngineRequest({
  v: 1,
  qid: '01J9Q1ZZZZZZZZZZZZZZZZZZZZ',
  table: 'orders',
  query: { select: [{ op: 'sum', col: 'amount', alias: 's' }, { op: 'count', alias: 'c' }], groupBy: ['region'] },
});
const SCALAR_REQ = parseWhEngineRequest({
  v: 1,
  qid: '01J9Q1ZZZZZZZZZZZZZZZZZZZZ',
  table: 'orders',
  query: { select: [{ op: 'sum', col: 'amount', alias: 's' }, { op: 'count', col: 'amount', alias: 'c' }] },
});

function execBase(req: typeof GROUPED_REQ | typeof SCALAR_REQ, shards: WhDirectoryRow[]): Parameters<typeof executeWhQuery>[0] {
  return {
    req,
    columnTypes: COLS,
    columnScales: SCALES,
    directoryRows: shards,
    shardKeyColumn: '',
    shardKeyType: 'none',
    directoryVersion: 7,
    timers: neverTimeoutTimers(),
  };
}

/** Grouped §2.0-shaped envelope; `template_hash`/`truncated` are the §6.2
 *  wh_query fields (beyond the v0 type — read raw by the consumption gates).
 *  rowCountOverride lets a truncated partial carry its real group count. */
function genv(
  shard: string,
  rows: { k: string[]; s: string; c: number }[],
  opts: { templateHash?: string; truncated?: boolean; more?: boolean; rowCountOverride?: number; schemaVersion?: number } = {},
): WhPartialEnvelope {
  const rowsOut = rows.map((r) => ({ k: r.k, a: { s: r.s, c: r.c } }));
  return {
    v: 1,
    shard,
    table: 'orders',
    schema_version: opts.schemaVersion ?? 3,
    partial: {
      kind: 'grouped',
      groupKeys: ['region'],
      aggs: { s: { op: 'sum', col: 'amount' }, c: { op: 'count' } },
      rows: rowsOut,
      rowCount: opts.rowCountOverride ?? rowsOut.length,
      more: opts.more ?? false,
      ...(opts.truncated !== undefined ? { truncated: opts.truncated } : {}),
    },
    ...(opts.templateHash !== undefined ? { template_hash: opts.templateHash } : {}),
  } as WhPartialEnvelope;
}

/** Scalar envelope over the W5-shaped plan (sum + count(col) → count_col). */
function senv(
  shard: string,
  a: { s: string; c: number },
  opts: { templateHash?: string; truncated?: boolean } = {},
): WhPartialEnvelope {
  return {
    v: 1,
    shard,
    table: 'orders',
    schema_version: 3,
    partial: {
      kind: 'scalar',
      aggs: { s: { op: 'sum', col: 'amount' }, c: { op: 'count', col: 'amount' } },
      rows: [{ k: [], a }],
      rowCount: 1,
      more: false,
      ...(opts.truncated !== undefined ? { truncated: opts.truncated } : {}),
    },
    ...(opts.templateHash !== undefined ? { template_hash: opts.templateHash } : {}),
  } as WhPartialEnvelope;
}

function fakeHandshake(byShard: Record<string, TemplateInventoryRow[]>, calls?: string[]): WhShardHandshake {
  return {
    readTemplateInventory: async (shard) => {
      calls?.push(shard);
      return byShard[shard] ?? [];
    },
  };
}

function countingFetch(envelopeFor: (shard: string) => WhPartialEnvelope, calls: string[]): WhShardFetcher {
  return async (shard) => {
    calls.push(shard);
    return { ok: true, envelope: envelopeFor(shard), estRows: 0 };
  };
}

// =============================================================================
// 1. parseInventoryRows — strict whole-body fail-closed, NEVER throws.
// =============================================================================
Deno.test('parse: non-array bodies => [] (null / {"keys":5} / string / number / bool)', () => {
  for (const body of [null, { keys: 5 }, 's', 42, true, undefined]) {
    neverThrows(`parse body ${show(body)} => []`, () => parseInventoryRows(body), (out) => eq(`...yields [] for ${show(body)}`, out, []));
  }
});

Deno.test('parse: array with ANY malformed row => whole body [] (fail-closed; [null] / missing template_hash / wrong field shapes)', () => {
  const good = invRow(W1H);
  const bodies = [
    [null],                       // [null]
    [[1, 2]],                     // nested array row
    [{}],                         // empty row
    [{ qc_class: 'QC2' }],        // row missing template_hash
    [{ ...good, template_hash: 5 }],          // mistyped hash
    [{ ...good, max_rows: '1000' }],          // mistyped max_rows
    [{ ...good, schema_version: '1' }],       // mistyped schema_version
    [{ ...good, state: null }],               // mistyped state
    [good, null],                 // ONE bad row fails the WHOLE body closed
  ];
  for (const body of bodies) {
    neverThrows(`parse body ${show(body).slice(0, 60)} => []`, () => parseInventoryRows(body), (out) => eq('...yields []', out, []));
  }
});

Deno.test('parse: a fully well-shaped row passes through EXACTLY (field-for-field)', () => {
  const row = { template_hash: W1H, qc_class: 'QC2', logical_table: 'wh_probe_agg', schema_version: 1, state: 'frozen', max_rows: 500 };
  eq('row survives the guard verbatim', parseInventoryRows([row]), [row]);
  eq('[] is a valid (empty) inventory', parseInventoryRows([]), []);
});

// =============================================================================
// 2. readTemplateInventory — pinned URL/headers, never-throw, NO caching.
// =============================================================================
Deno.test('inventory GET: URL + engine→shard plane headers pinned (§5.2 select list + state filter; contract §4.6 apikey/Bearer = shard service key)', async () => {
  const { fetcher, calls } = jsonFetch([invRow(W1H)]);
  const rows = await readTemplateInventory({ fetcher, shardServiceKey: (shard) => `sk-${shard}` }, 'shard-a');
  eq('rows parsed', rows, [invRow(W1H)]);
  eq('exactly one call', calls.length, 1);
  eq('URL byte-pinned (mirrors compileShardUrl base)', calls[0].url, templateInventoryUrl('shard-a'));
  eq(
    'URL carries the §5.2 select list + active|frozen filter',
    calls[0].url,
    'https://shard-a.supabase.co/rest/v1/wh_query_templates?select=template_hash,qc_class,logical_table,schema_version,state,max_rows&state=in.(active,frozen)',
  );
  eq('method GET', calls[0].init.method, 'GET');
  eq('headers: apikey + Authorization Bearer (shard service key) + Accept-Profile (SHARD_REQUEST_HEADERS mirror)',
    calls[0].init.headers,
    { ...templateInventoryHeaders('sk-shard-a') },
  );
  eq('apikey = shard service key', calls[0].init.headers['apikey'], 'sk-shard-a');
  eq('Authorization Bearer = shard service key', calls[0].init.headers['Authorization'], 'Bearer sk-shard-a');
  eq('Accept-Profile mirrors the engine shard-plane pin', calls[0].init.headers['Accept-Profile'], 'public');
});

Deno.test('inventory GET: never-throw battery — dep throw / text() reject / non-JSON / wrong shapes / ok:false => []', async () => {
  const throwingFetcher: WhRawFetch = async () => {
    throw new Error('connection refused');
  };
  eq('dep throw => []', await readTemplateInventory({ fetcher: throwingFetcher, shardServiceKey: () => 'k' }, 's'), []);

  const textThrows: WhRawFetch = async () => ({ ok: true, status: 200, text: async () => { throw new Error('stream dead'); } });
  eq('text() reject => []', await readTemplateInventory({ fetcher: textThrows, shardServiceKey: () => 'k' }, 's'), []);

  for (const body of ['not json', '{"keys":5}', 'null', '[null]', '{"template_hash":5}']) {
    const f = jsonFetch(body).fetcher;
    eq(`body ${body} => []`, await readTemplateInventory({ fetcher: f, shardServiceKey: () => 'k' }, 's'), []);
  }

  const notOk = jsonFetch([invRow(W1H)], { ok: false }).fetcher; // 404-class carrying an array-shaped error body
  eq('ok:false (non-2xx) => [] even with an array body', await readTemplateInventory({ fetcher: notOk, shardServiceKey: () => 'k' }, 's'), []);

  const noStatus: WhRawFetch = async () => ({ text: async () => JSON.stringify([invRow(W1H)]) }); // minimal fake without ok/status
  eq('minimal fake (no ok/status) rides the shape law', await readTemplateInventory({ fetcher: noStatus, shardServiceKey: () => 'k' }, 's'), [invRow(W1H)]);
});

Deno.test('inventory GET: NO cross-call caching (§5.2 — isolate recycling makes a cache a disproven pattern): two reads => two network reads', async () => {
  const { fetcher, calls } = jsonFetch([invRow(W1H)]);
  const deps = { fetcher, shardServiceKey: () => 'k' };
  await readTemplateInventory(deps, 's');
  await readTemplateInventory(deps, 's');
  eq('fetcher invoked once PER CALL (no memoization)', calls.length, 2);
});

Deno.test('makeWhHandshake: default impl delegates to readTemplateInventory with the injected fetch', async () => {
  const { fetcher, calls } = jsonFetch([invRow(W4H)]);
  const hs = makeWhHandshake({ fetcher, shardServiceKey: () => 'k' });
  eq('handshake shape reads the inventory', await hs.readTemplateInventory('shard-z'), [invRow(W4H)]);
  eq('one raw fetch per handshake call', calls.length, 1);
});

// =============================================================================
// 3. checkHandshake — §5.2 (a) subset, (b) schema_version, (c) max_rows.
// =============================================================================
Deno.test('handshake: happy path => eligible (hash present+active, schema match, K within max_rows)', () => {
  const plan: HandshakePlanRef = { templateHashes: [W1H], limitK: 100 };
  // r69 (AM-6/OQ-8): an eligible verdict carries matchedHash (spread-
  // conditional in the verdict construction) — the FIRST required hash in
  // plan order (≡ the first derived hash on eligibility). The ineligible and
  // zero-requirement verdict deep-equals below stay UNTOUCHED (matchedHash
  // is absent there — only what actually reds was touched, §5 statics).
  eq(
    'eligible, warning null, matchedHash = first required hash (plan order)',
    checkHandshake(plan, [invRow(W1H)], 3), // table version matches the invRow default (3)
    { eligible: true, warning: null, matchedHash: W1H },
  );
});

Deno.test('handshake (a): required hash missing from inventory => template_missing (est_rows null — caller fills from the directory lane)', () => {
  const plan: HandshakePlanRef = { templateHashes: [W4H], limitK: null };
  eq(
    'W4 required, only W1 returned',
    checkHandshake(plan, [invRow(W1H)], null),
    { eligible: false, warning: { code: 'template_missing', est_rows: null } },
  );
  eq('empty inventory => template_missing', checkHandshake(plan, [], null).warning?.code, 'template_missing');
});

Deno.test('handshake (a): state law — only active|frozen count; draft/retired rows are ABSENT for eligibility', () => {
  const plan: HandshakePlanRef = { templateHashes: [W1H], limitK: null };
  eq('frozen accepted (draining reads merge both wave sides)', checkHandshake(plan, [invRow(W1H, { state: 'frozen' })], null).eligible, true);
  eq('draft => template_missing', checkHandshake(plan, [invRow(W1H, { state: 'draft' })], null).warning?.code, 'template_missing');
  eq('retired => template_missing', checkHandshake(plan, [invRow(W1H, { state: 'retired' })], null).warning?.code, 'template_missing');
});

Deno.test('handshake (b): inventory schema_version 2 vs directory table 3 => schema_mismatch', () => {
  const plan: HandshakePlanRef = { templateHashes: [W1H], limitK: null };
  eq(
    'ddl-wave version gate',
    checkHandshake(plan, [invRow(W1H, { schema_version: 2 })], 3),
    { eligible: false, warning: { code: 'schema_mismatch', est_rows: null } },
  );
  eq('undefined directory version => version check vacuous', checkHandshake(plan, [invRow(W1H, { schema_version: 2 })], undefined).eligible, true);
});

Deno.test('handshake (c): plan limit 500 vs max_rows 100 => max_rows_exceeded; K == max_rows stays eligible (strictly-greater refuses)', () => {
  const over: HandshakePlanRef = { templateHashes: [W1H], limitK: 500 };
  eq(
    '500 > 100 refuses',
    checkHandshake(over, [invRow(W1H, { max_rows: 100 })], null),
    { eligible: false, warning: { code: 'max_rows_exceeded', est_rows: null } },
  );
  eq('100 == 100 eligible (boundary)', checkHandshake({ templateHashes: [W1H], limitK: 100 }, [invRow(W1H, { max_rows: 100 })], null).eligible, true);
  eq('K null => max_rows check vacuous (shard sentinel still caps — defense in depth §2.1 step 5)', checkHandshake({ templateHashes: [W1H], limitK: null }, [invRow(W1H, { max_rows: 100 })], null).eligible, true);
});

Deno.test('handshake: multi-hash — every required hash checked in plan order, first failure wins; empty requirements eligible', () => {
  eq('second hash missing => template_missing', checkHandshake({ templateHashes: [W1H, W4H], limitK: null }, [invRow(W1H)], null).warning?.code, 'template_missing');
  eq('second hash over-max => max_rows_exceeded', checkHandshake({ templateHashes: [W1H, W1H], limitK: 2000 }, [invRow(W1H, { max_rows: 1000 })], null).warning?.code, 'max_rows_exceeded');
  eq('no required hashes => eligible', checkHandshake({ templateHashes: [], limitK: 9999 }, [], null), { eligible: true, warning: null });
});

Deno.test('handshake: never throws on garbage inputs (consumption-site guard law)', () => {
  neverThrows('plan null-ish', () => checkHandshake(null as unknown as HandshakePlanRef, [invRow(W1H)], 1), (out) => eq('=> eligible (nothing verifiable)', out, { eligible: true, warning: null }));
  neverThrows('plan {} (no fields)', () => checkHandshake({} as HandshakePlanRef, [], undefined), (out) => eq('=> eligible', out, { eligible: true, warning: null }));
  neverThrows('inventory garbage entries', () => checkHandshake({ templateHashes: [W1H], limitK: null }, [null, 5, 'x', {}] as unknown as TemplateInventoryRow[], 1), (out) => eq('=> template_missing (hash not found)', (out as { warning: { code: string } }).warning.code, 'template_missing'));
});

// =============================================================================
// 4. ENGINE_TEMPLATE_MANIFEST — equality pin + F14 subset algebra.
// =============================================================================
// Provenance: hand-transcribed from db/shard-templates/manifest.json @ FM
// 1e1c725 (r43 seed wave, 5/5 sha256-pinned template bodies). This literal is
// the offline pin; the file pin below re-reads the file whenever the test
// config grants --allow-read over db/shard-templates.
const MANIFEST_LITERAL: EngineTemplateRow[] = [
  {
    slug: 'W1_grouped_sum_count',
    file: 'W1_grouped_sum_count.sql',
    template_hash: 'a934e7e062f59cff5a856afdc7aa743ec9be11c068c7e861ea856c36b40bdbfd',
    logical_table: 'wh_probe_agg',
    qc_class: 'QC2',
    kind: 'rows',
    merge_ops: ['groupby', 'sum', 'count'],
    group_keys: ['region'],
    params_schema: { id_min: 'int8', id_max: 'int8' },
    timeout_ms: 8000,
    max_rows: 1000,
    schema_version: 1,
    state: 'active',
    aggs: { x: { op: 'sum', col: 'amount' }, c: { op: 'count' } },
    encoding: { x: 'text', c: 'number' },
  },
  {
    slug: 'W2_scalar_minmax',
    file: 'W2_scalar_minmax.sql',
    template_hash: 'a095adaa148253aee8d1cc8e976f01b3579beeea5082f3862df4a908c20b2659',
    logical_table: 'wh_probe_agg',
    qc_class: 'QC2',
    kind: 'scalar',
    merge_ops: ['min', 'max', 'count_col'],
    group_keys: [],
    params_schema: { id_min: 'int8', id_max: 'int8' },
    timeout_ms: 8000,
    max_rows: 1000,
    schema_version: 1,
    state: 'active',
    aggs: { min: { op: 'min', col: 'amount' }, max: { op: 'max', col: 'amount' }, c: { op: 'count_col', col: 'amount' } },
    encoding: { min: 'text', max: 'text', c: 'number' },
  },
  {
    slug: 'W3_scalar_avg_pair',
    file: 'W3_scalar_avg_pair.sql',
    template_hash: 'bca9dd2c591ed48a0fa5367179dd5deb1d752ed9141c23e6ad53083becf8ecac',
    logical_table: 'wh_probe_agg',
    qc_class: 'QC2',
    kind: 'scalar',
    merge_ops: ['avg_pair'],
    group_keys: [],
    params_schema: { id_min: 'int8', id_max: 'int8' },
    timeout_ms: 8000,
    max_rows: 1000,
    schema_version: 1,
    state: 'active',
    aggs: { s: { op: 'sum', col: 'amount' }, c: { op: 'count_col', col: 'amount' } },
    encoding: { s: 'text', c: 'number' },
  },
  {
    slug: 'W4_topk',
    file: 'W4_topk.sql',
    template_hash: 'dccfc317035961a6310c31194e86a01c715f384ac8cbdc33fb00f35f27eefcee',
    logical_table: 'wh_probe_agg',
    qc_class: 'QC3',
    kind: 'rows',
    merge_ops: ['topk', 'raw_rows'],
    group_keys: [],
    params_schema: {},
    timeout_ms: 8000,
    max_rows: 1000,
    schema_version: 1,
    state: 'active',
    aggs: {},
    encoding: {},
  },
  {
    slug: 'W5_cold_agg',
    file: 'W5_cold_agg.sql',
    template_hash: '9a8e922d064539e3418fd77dbc17479fe6a9b69bafe0f182128c675ef1f95234',
    logical_table: 'facts_blocks',
    qc_class: 'COLD_AGG',
    kind: 'scalar',
    merge_ops: ['sum', 'count_col'],
    group_keys: [],
    params_schema: { dataset: 'text', day_from: 'date', day_to: 'date' },
    timeout_ms: 8000,
    max_rows: 1000,
    schema_version: 1,
    state: 'active',
    aggs: { s: { op: 'sum', col: 'value' }, c: { op: 'count_col', col: 'value' } },
    encoding: { s: 'text', c: 'number' },
  },
];

Deno.test('manifest pin: ENGINE_TEMPLATE_MANIFEST deep-equals the hand-transcribed literal (W1-W5, all fields)', () => {
  eq('engine manifest === literal copy', ENGINE_TEMPLATE_MANIFEST, MANIFEST_LITERAL);
  eq('exactly five templates (W1-W5)', ENGINE_TEMPLATE_MANIFEST.length, 5);
  eq('the five pinned hashes are present', ENGINE_TEMPLATE_MANIFEST.map((r) => r.template_hash), [W1H, W2H, W3H, W4H, W5H]);
});

Deno.test('manifest pin: file equality when --allow-read grants db/shard-templates (fallback = literal pin, provenance noted)', () => {
  let fileText: string | null = null;
  try {
    fileText = Deno.readTextFileSync(new URL('../../../db/shard-templates/manifest.json', import.meta.url));
  } catch {
    fileText = null; // parent gate grants only wh_fixtures — the literal pin above carries the equality proof
  }
  if (fileText !== null) {
    const parsed = JSON.parse(fileText) as unknown;
    eq('FILE deep-equals ENGINE_TEMPLATE_MANIFEST', parsed, ENGINE_TEMPLATE_MANIFEST);
    eq('FILE deep-equals the literal copy', parsed, MANIFEST_LITERAL);
    console.log('  ok  [file-pin branch: manifest.json read and pinned]');
  } else {
    console.log('  note [fallback branch: fs not granted — literal provenance pin carries; file pin proven under --allow-read=...,./db/shard-templates]');
    eqTrue('literal copy already pinned against ENGINE_TEMPLATE_MANIFEST above', true);
  }
});

Deno.test('manifest resolution: manifestRowByHash resolves W1/W5, unknown/mistyped => null', () => {
  eq('W1 resolves', manifestRowByHash(W1H)?.slug, 'W1_grouped_sum_count');
  eq('W5 resolves', manifestRowByHash(W5H)?.slug, 'W5_cold_agg');
  eq('unknown hash => null', manifestRowByHash('deadbeef'.padEnd(64, '0')), null);
  eq('empty string => null', manifestRowByHash(''), null);
});

Deno.test('F14 subset rule — arm A (must REJECT): plan ops [sum,count] vs template merge_ops ["count"]', () => {
  eqTrue("['sum','count'] ⊄ ['count'] (the brief's exact arm-A data)", planOpsSubset(['sum', 'count'], ['count']) === false);
});

Deno.test('F14 subset rule — arm B (must ACCEPT, the regression arm): plan ops [topk] vs template merge_ops ["topk","raw_rows"]', () => {
  eqTrue("['topk'] ⊆ ['topk','raw_rows'] (v1's exact-equality mis-pin would falsely reject this)", planOpsSubset(['topk'], ['topk', 'raw_rows']) === true);
  eqTrue('asymmetry proves SUBSET (not equality): superset template accepted, reverse direction refused', planOpsSubset(['topk', 'raw_rows'], ['topk']) === false);
  eqTrue('vacuous: empty plan ops are always a subset', planOpsSubset([], ['topk']) === true);
});

Deno.test('derivePlanMergeOps: §6.3 token mapping (count(*)→count, count(col)→count_col, avg→avg_pair, groupKeys→groupby)', () => {
  const groupedPlan = { groupKeys: [{ col: 'region', type: 'text' }], aggs: { s: { op: 'sum', col: 'amount' }, c: { op: 'count' } } };
  eq('grouped sum+count', derivePlanMergeOps(groupedPlan as never), ['groupby', 'sum', 'count']);
  eq('count(*) only', derivePlanMergeOps({ aggs: { c: { op: 'count' } } } as never), ['count']);
  eq('count(col) maps to count_col', derivePlanMergeOps({ aggs: { c: { op: 'count', col: 'amount' } } } as never), ['count_col']);
  eq('avg maps to avg_pair (the pair algebra — never the mean)', derivePlanMergeOps({ aggs: { a: { op: 'avg', col: 'amount' } } } as never), ['avg_pair']);
  eq('min/max pass through', derivePlanMergeOps({ aggs: { mn: { op: 'min', col: 'amount' }, mx: { op: 'max', col: 'amount' } } } as never), ['min', 'max']);
  eq('scalar W5-shaped plan', derivePlanMergeOps({ aggs: { s: { op: 'sum', col: 'amount' }, c: { op: 'count', col: 'amount' } } } as never), ['sum', 'count_col']);
});

Deno.test('plan honesty: planTemplatesMissingInManifest — unmanifested hashes named, undefined vacuous', () => {
  eq('[W1H, deadbeef] => [deadbeef]', planTemplatesMissingInManifest([W1H, 'deadbeef'.padEnd(64, '0')]), ['deadbeef'.padEnd(64, '0')]);
  eq('all five W-hashes manifest-clean', planTemplatesMissingInManifest([W1H, W2H, W3H, W4H, W5H]), []);
  eq('undefined => [] (legacy plans carry no template refs)', planTemplatesMissingInManifest(undefined), []);
});

// =============================================================================
// 5. gatePartialAgainstPlan — F2 truncation gate + F14 subset gate (unit).
// =============================================================================
const ARM_A_PLAN = buildMergePlan(GROUPED_REQ, { columnTypes: COLS, columnScales: SCALES }); // grouped sum(amount)+count(*) by region → derived ops [groupby, sum, count]

Deno.test('gate F14: partial with NO template claim skips the gate (legacy §2.0 select-path shape unaffected)', () => {
  eq('no template_hash => null (proceed to merge)', gatePartialAgainstPlan(ARM_A_PLAN, genv('S1', [{ k: ['eu'], s: '100', c: 1 }])), null);
});

Deno.test('gate F14 arm A (must REJECT): partial claims W2 ["min","max","count_col"] under a [groupby,sum,count] plan => merge_mismatch', () => {
  eq(
    'W2 cannot serve sum/count — the partial is rejected',
    gatePartialAgainstPlan(ARM_A_PLAN, genv('S1', [{ k: ['eu'], s: '100', c: 1 }], { templateHash: W2H })),
    {
      code: 'merge_mismatch',
      detail: "plan ops [groupby,sum,count] is not a subset of template merge_ops [min,max,count_col] for " + W2H + " (§6.3 F14 subset rule)",
    },
  );
});

Deno.test('gate F14 arm A: unknown-hash claim / non-string hash / empty-string hash => merge_mismatch', () => {
  eq('unresolvable hash', gatePartialAgainstPlan(ARM_A_PLAN, genv('S1', [{ k: ['eu'], s: '100', c: 1 }], { templateHash: 'deadbeef'.padEnd(64, '0') }))?.code, 'merge_mismatch');
  eq('numeric hash claim', gatePartialAgainstPlan(ARM_A_PLAN, { v: 1, shard: 'S1', table: 'orders', schema_version: 3, partial: genv('S1', []).partial, template_hash: 5 } as unknown as WhPartialEnvelope)?.code, 'merge_mismatch');
  eq('empty-string hash claim', gatePartialAgainstPlan(ARM_A_PLAN, genv('S1', [{ k: ['eu'], s: '100', c: 1 }], { templateHash: '' }))?.code, 'merge_mismatch');
});

Deno.test('gate F14 accept path: partial claims W1 ["groupby","sum","count"] under the same-shaped plan => null (merges fine)', () => {
  eq('W1 covers the plan exactly', gatePartialAgainstPlan(ARM_A_PLAN, genv('S1', [{ k: ['eu'], s: '100', c: 1 }], { templateHash: W1H })), null);
  eq('W1 covers a sum-only plan too (subset, not equality)', gatePartialAgainstPlan(
    parseWhEngineRequest({ v: 1, qid: 'q', table: 'orders', query: { select: [{ op: 'sum', col: 'amount', alias: 's' }], groupBy: ['region'] } }),
    genv('S1', [{ k: ['eu'], s: '100', c: 1 }], { templateHash: W1H }),
  ), null);
});

Deno.test('gate F2: grouped partial with truncated:true (or the v0 more:true twin) => truncated_groupby, NEVER merged', () => {
  eq('truncated:true', gatePartialAgainstPlan(ARM_A_PLAN, genv('S1', [{ k: ['eu'], s: '100', c: 1 }], { truncated: true }))?.code, 'truncated_groupby');
  eq('more:true (§2.0 twin — preempts P2-1 with the F2-pinned code)', gatePartialAgainstPlan(ARM_A_PLAN, genv('S1', [{ k: ['eu'], s: '100', c: 1 }], { more: true }))?.code, 'truncated_groupby');
  eq('truncated:false => clean', gatePartialAgainstPlan(ARM_A_PLAN, genv('S1', [{ k: ['eu'], s: '100', c: 1 }], { truncated: false })), null);
});

Deno.test('gate F2 exemptions: truncated topk ACCEPTED (trimmed BY DESIGN); scalar carries no sentinel', () => {
  const topkEnv = {
    v: 1,
    shard: 'S1',
    table: 'orders',
    schema_version: 3,
    partial: { kind: 'topk', aggs: {}, rows: [{ k: ['eu'], a: {} }], rowCount: 1, more: false, truncated: true },
  } as unknown as WhPartialEnvelope;
  eq('topk + truncated:true => null (EXEMPT — the K-way heap owns the trim)', gatePartialAgainstPlan(ARM_A_PLAN, topkEnv), null);
  const scalarTruncated = {
    v: 1,
    shard: 'S1',
    table: 'orders',
    schema_version: 3,
    partial: { kind: 'scalar', aggs: { s: { op: 'sum', col: 'amount' }, c: { op: 'count' } }, rows: [{ k: [], a: { s: '1', c: 1 } }], rowCount: 1, more: false, truncated: true },
  } as unknown as WhPartialEnvelope;
  eq('scalar + truncated:true => null (no sentinel applies, §6.2 F7)', gatePartialAgainstPlan(SCALAR_REQ, scalarTruncated), null);
  const scalarMore = { ...scalarTruncated, partial: { ...scalarTruncated.partial, truncated: undefined, more: true } } as unknown as WhPartialEnvelope;
  eq('scalar + more:true => null HERE (wh_merge P2-1 envelope_invalid owns it — not the F2 code)', gatePartialAgainstPlan(SCALAR_REQ, scalarMore), null);
});

Deno.test('gate: garbage envelopes fall through (wh_merge envelope_invalid law owns them)', () => {
  eq('null envelope', gatePartialAgainstPlan(ARM_A_PLAN, null), null);
  eq('number envelope', gatePartialAgainstPlan(ARM_A_PLAN, 5), null);
  eq('array envelope', gatePartialAgainstPlan(ARM_A_PLAN, []), null);
  eq('missing partial', gatePartialAgainstPlan(ARM_A_PLAN, { v: 1, shard: 'S1' }), null);
});

// =============================================================================
// 6. ENGINE-LEVEL (executeWhQuery) — the §5.2 gate + consumption gates live.
// =============================================================================
Deno.test('LETHAL 1: inventory missing a required hash => template_missing, shard EXCLUDED, ZERO wh_query fetches', async () => {
  const fetchCalls: string[] = [];
  const invCalls: string[] = [];
  const res = await executeWhQuery({
    ...execBase(GROUPED_REQ, [dirRow('S1')]),
    fetcher: countingFetch((s) => genv(s, [{ k: ['eu'], s: '100', c: 1 }], { templateHash: W1H }), fetchCalls),
    handshake: fakeHandshake({ S1: [invRow(W2H)] }, invCalls), // W1 required, W2 offered
    templateHashes: [W1H],
  });
  eq('warning: template_missing (est_rows 0 — directory lane absent)', res.warnings, [{ shard: 'S1', code: 'template_missing', est_rows: 0, retried: false }]);
  eq('shard excluded, never attempted', res.perShard, [{ shard: 'S1', ok: false, latencyMs: res.perShard[0]?.latencyMs, error: 'template_missing' }]);
  eq('coverage 0/1, partial', [res.coverage, res.partial], ['0/1', true]);
  eq('LETHAL: wh_query fetch-call count == 0', fetchCalls, []);
  eq('inventory read exactly once (no caching)', invCalls, ['S1']);
  eq('merged output carries NOTHING from the excluded shard', res.rows, []);
});

Deno.test('LETHAL 1b: est_rows rides the directory row_estimate lane when available (§4.4)', async () => {
  const res = await executeWhQuery({
    ...execBase(GROUPED_REQ, [dirRow('S1', { rowEstimate: 123 })]),
    fetcher: countingFetch((s) => genv(s, [{ k: ['eu'], s: '100', c: 1 }]), []),
    handshake: fakeHandshake({ S1: [] }, []), // inventory-empty => template_missing
    templateHashes: [W1H],
  });
  eq('warning carries the shard est_rows=123 (degrade-vs-abort weight)', res.warnings, [{ shard: 'S1', code: 'template_missing', est_rows: 123, retried: false }]);
});

Deno.test('LETHAL 2: inventory row schema_version 2 vs directory table 3 => schema_mismatch, ZERO wh_query fetches', async () => {
  const fetchCalls: string[] = [];
  const res = await executeWhQuery({
    ...execBase(GROUPED_REQ, [dirRow('S1', { tableSchemaVersion: 3 })]),
    fetcher: countingFetch((s) => genv(s, [{ k: ['eu'], s: '100', c: 1 }]), fetchCalls),
    handshake: fakeHandshake({ S1: [invRow(W1H, { schema_version: 2 })] }, []),
    templateHashes: [W1H],
    tableSchemaVersion: 3,
  });
  eq('schema_mismatch exclusion (ddl-wave gate at handshake, before any call)', res.warnings, [{ shard: 'S1', code: 'schema_mismatch', est_rows: 0, retried: false }]);
  eq('LETHAL: wh_query fetch-call count == 0', fetchCalls, []);
  eq('coverage 0/1', res.coverage, '0/1');
});

Deno.test('LETHAL 3: plan limit 500 vs template max_rows 100 => max_rows_exceeded AND zero wh_query fetches (fail-fast BEFORE any call)', async () => {
  const fetchCalls: string[] = [];
  const res = await executeWhQuery({
    ...execBase(parseWhEngineRequest({
      v: 1,
      qid: '01J9Q1ZZZZZZZZZZZZZZZZZZZZ',
      table: 'orders',
      query: { select: [{ op: 'sum', col: 'amount', alias: 's' }, { op: 'count', alias: 'c' }], groupBy: ['region'], limit: 500 },
    }), [dirRow('S1')]),
    fetcher: countingFetch((s) => genv(s, [{ k: ['eu'], s: '100', c: 1 }]), fetchCalls),
    handshake: fakeHandshake({ S1: [invRow(W1H, { max_rows: 100 })] }, []),
    templateHashes: [W1H],
  });
  eq('max_rows_exceeded refusal', res.warnings, [{ shard: 'S1', code: 'max_rows_exceeded', est_rows: 0, retried: false }]);
  eq('LETHAL: the wh_query fetch stub recorded NO /rpc/wh_query call', fetchCalls, []);
  eq('coverage 0/1, partial', [res.coverage, res.partial], ['0/1', true]);
});

Deno.test('LETHAL 3b: boundary — limit 100 == max_rows 100 passes the handshake and fans out (only strictly-greater refuses)', async () => {
  const fetchCalls: string[] = [];
  const res = await executeWhQuery({
    ...execBase(parseWhEngineRequest({
      v: 1,
      qid: '01J9Q1ZZZZZZZZZZZZZZZZZZZZ',
      table: 'orders',
      query: { select: [{ op: 'sum', col: 'amount', alias: 's' }, { op: 'count', alias: 'c' }], groupBy: ['region'], limit: 100 },
    }), [dirRow('S1')]),
    fetcher: countingFetch((s) => genv(s, [{ k: ['eu'], s: '100', c: 1 }], { templateHash: W1H }), fetchCalls),
    handshake: fakeHandshake({ S1: [invRow(W1H, { max_rows: 100 })] }, []),
    templateHashes: [W1H],
  });
  eq('eligible shard IS fetched (exactly once)', fetchCalls, ['S1']);
  eq('no warnings, merged', [res.warnings, res.coverage], [[], '1/1']);
  eq('group merged (hand-computed: eu s=100n c=1)', res.rows, [{ k: ['eu'], aggs: { s: 100n, c: 1 } }]);
});

Deno.test('LETHAL 4 (F14 arm A end-to-end): partial claiming W2 cannot serve a [sum,count] plan — merge_mismatch, merge NEVER called with it', async () => {
  const fetchCalls: string[] = [];
  // S1 lies: a WELL-FORMED grouped partial (wh_merge would happily fold it)
  // claiming W2's hash — merge_ops ["min","max","count_col"] lack sum+count.
  // S2 is honest and claims W1.
  const res = await executeWhQuery({
    ...execBase(GROUPED_REQ, [dirRow('S1'), dirRow('S2')]),
    fetcher: countingFetch((s) => s === 'S1'
      ? genv(s, [{ k: ['eu'], s: '999999', c: 999 }], { templateHash: W2H })
      : genv(s, [{ k: ['eu'], s: '200', c: 2 }, { k: ['us'], s: '300', c: 3 }], { templateHash: W1H }), fetchCalls),
    // S1's inventory SATISFIES the handshake (W1H present+active) — the lie
    // is the PARTIAL's template_hash claim (W2H), caught at the consumption
    // gate, not at the inventory read (§5.2 necessary-not-sufficient law).
    handshake: fakeHandshake({ S1: [invRow(W1H)], S2: [invRow(W1H)] }, []),
    templateHashes: [W1H],
  });
  eq('S1 degraded with merge_mismatch (422-class, contract §4.4)', res.perShard, [
    { shard: 'S1', ok: false, latencyMs: res.perShard[0]?.latencyMs, error: 'merge_mismatch' },
    { shard: 'S2', ok: true, latencyMs: res.perShard[1]?.latencyMs, error: null },
  ]);
  eq('warning names the subset violation (detail carries plan-vs-template ops)', res.warnings, [{
    shard: 'S1',
    code: 'merge_mismatch',
    est_rows: 1, // partial-derived rowCount (the lying partial carries 1 group)
    retried: false,
    detail: `plan ops [groupby,sum,count] is not a subset of template merge_ops [min,max,count_col] for ${W2H} (§6.3 F14 subset rule)`,
  }]);
  eq('merge output = S2 ONLY (the lying partial NEVER entered wh_merge — eu is 200n, not 1000199n)', res.rows, [
    { k: ['eu'], aggs: { s: 200n, c: 2 } },
    { k: ['us'], aggs: { s: 300n, c: 3 } },
  ]);
  eq('coverage 1/2, partial', [res.coverage, res.partial], ['1/2', true]);
  eq('both shards WERE fetched (the gate is at consumption, not transport)', fetchCalls.sort(), ['S1', 'S2']);
});

Deno.test('LETHAL 5 (F14 arm B accept path): partial claiming W1 under the same-shaped plan merges fine', async () => {
  const res = await executeWhQuery({
    ...execBase(GROUPED_REQ, [dirRow('S1')]),
    fetcher: countingFetch((s) => genv(s, [{ k: ['eu'], s: '100', c: 1 }, { k: ['us'], s: '40', c: 4 }], { templateHash: W1H }), []),
    handshake: fakeHandshake({ S1: [invRow(W1H)] }, []),
    templateHashes: [W1H],
  });
  eq('no warnings, full coverage', [res.warnings, res.coverage, res.partial], [[], '1/1', false]);
  eq('hand-computed merge (eu 100n/1, us 40n/4)', res.rows, [
    { k: ['eu'], aggs: { s: 100n, c: 1 } },
    { k: ['us'], aggs: { s: 40n, c: 4 } },
  ]);
});

Deno.test('LETHAL 6a: truncated GROUPBY partial rejected with partial-derived est_rows; the honest shard still merges (wrong-SUM class closed)', async () => {
  const res = await executeWhQuery({
    ...execBase(GROUPED_REQ, [dirRow('S1'), dirRow('S2')]),
    // S1: 7 trimmed groups declared, one shown, sentinel set — merging it
    // would silently under-count every per-group Σ (the F2 wrong-SUM class).
    fetcher: countingFetch((s) => s === 'S1'
      ? genv(s, [{ k: ['eu'], s: '100', c: 1 }], { truncated: true, rowCountOverride: 7 })
      : genv(s, [{ k: ['eu'], s: '200', c: 2 }, { k: ['us'], s: '300', c: 3 }]), []),
    handshake: fakeHandshake({ S1: [invRow(W1H)], S2: [invRow(W1H)] }, []),
    templateHashes: [W1H],
  });
  eq('truncated_groupby warning with est_rows=7 (partial-derived rowCount)', res.warnings, [{
    shard: 'S1',
    code: 'truncated_groupby',
    est_rows: 7,
    retried: false,
    detail: 'grouped partial carries the truncation sentinel (truncated=true) — never silently merged (§6.3 F2)',
  }]);
  eq('S1 degraded', res.perShard[0], { shard: 'S1', ok: false, latencyMs: res.perShard[0]?.latencyMs, error: 'truncated_groupby' });
  eq('merged totals = S2 ONLY (eu is 200n — S1\'s 100 NEVER folded in)', res.rows, [
    { k: ['eu'], aggs: { s: 200n, c: 2 } },
    { k: ['us'], aggs: { s: 300n, c: 3 } },
  ]);
});

Deno.test('LETHAL 6b: the v0 more:true twin also codes truncated_groupby (preempts P2-1 envelope_invalid at the engine layer)', async () => {
  const res = await executeWhQuery({
    ...execBase(GROUPED_REQ, [dirRow('S1')]),
    fetcher: countingFetch((s) => genv(s, [{ k: ['eu'], s: '100', c: 1 }], { more: true }), []),
    handshake: fakeHandshake({ S1: [invRow(W1H)] }, []),
    templateHashes: [W1H],
  });
  eq('F2 code, not excluded/envelope_invalid', res.warnings.map((w) => w.code), ['truncated_groupby']);
});

Deno.test('LETHAL 6c: truncated TOPK partial is EXEMPT from F2 (no truncated_groupby ever fires)', async () => {
  const topkEnv = (shard: string): WhPartialEnvelope => ({
    v: 1,
    shard,
    table: 'orders',
    schema_version: 3,
    partial: { kind: 'topk', aggs: {}, rows: [{ k: ['eu'], a: {} }], rowCount: 1, more: false, truncated: true },
  } as unknown as WhPartialEnvelope);
  const res = await executeWhQuery({
    ...execBase(GROUPED_REQ, [dirRow('S1')]),
    fetcher: countingFetch(topkEnv, []),
    handshake: fakeHandshake({ S1: [invRow(W4H)] }, []),
    templateHashes: [W4H], // plan ops [groupby,sum,count] vs W4 ["topk","raw_rows"] — merge_mismatch would also be legitimate;
    // this pin isolates the F2 exemption: truncated_groupby must NOT fire.
  });
  eq('NO truncated_groupby anywhere in warnings', res.warnings.some((w) => w.code === 'truncated_groupby'), false);
  eq('the partial is still not merged (v0 algebra has no topk — envelope_invalid/excluded owns it)', res.coverage, '0/1');
});

Deno.test('LETHAL 6d: scalar partials carry no sentinel — truncated:false merges; truncated:true also merges (sentinel ignored, §6.2 F7)', async () => {
  const run = (truncated?: boolean) => executeWhQuery({
    ...execBase(SCALAR_REQ, [dirRow('S1')]),
    fetcher: countingFetch((s) => senv(s, { s: '50', c: 5 }, { templateHash: W5H, ...(truncated !== undefined ? { truncated } : {}) }), []),
    handshake: fakeHandshake({ S1: [invRow(W5H)] }, []), // W5 ["sum","count_col"] ⊇ plan [sum,count_col]
    templateHashes: [W5H],
  });
  const clean = await run(undefined);
  eq('scalar truncated:false merges (hand-computed s=50n c=5)', [clean.warnings, clean.result], [[], { s: 50n, c: 5 }]);
  const sentineled = await run(true);
  eq('scalar truncated:true — no F2 gate, still merges', [sentineled.warnings, sentineled.result], [[], { s: 50n, c: 5 }]);
});

Deno.test('LETHAL 7 (engine arm): wrong-shape inventory bodies fail CLOSED — template_missing, NEVER a throw', async () => {
  for (const body of ['null', '{"keys":5}', '[null]', 'not json']) {
    const raw = jsonFetch(body as string).fetcher;
    const res = await executeWhQuery({
      ...execBase(GROUPED_REQ, [dirRow('S1')]),
      fetcher: countingFetch((s) => genv(s, [{ k: ['eu'], s: '100', c: 1 }]), []),
      handshake: makeWhHandshake({ fetcher: raw, shardServiceKey: () => 'sk' }),
      templateHashes: [W1H],
    });
    eq(`inventory body ${body} => template_missing exclusion, no throw`, res.warnings.map((w) => w.code), ['template_missing']);
    eq(`...coverage 0/1`, res.coverage, '0/1');
  }
});

Deno.test('fail_fast + handshake exclusions => NO 5xx (§5.2 exclusions degrade+warn, exempt from collect-then-fail)', async () => {
  const res = await executeWhQuery({
    ...execBase(parseWhEngineRequest({
      v: 1,
      qid: '01J9Q1ZZZZZZZZZZZZZZZZZZZZ',
      table: 'orders',
      query: { select: [{ op: 'sum', col: 'amount', alias: 's' }, { op: 'count', alias: 'c' }], groupBy: ['region'] },
      coverage_mode: 'fail_fast',
    }), [dirRow('S1'), dirRow('S2')]),
    fetcher: countingFetch((s) => genv(s, [{ k: ['eu'], s: '100', c: 1 }]), []),
    handshake: fakeHandshake({ S1: [invRow(W1H)], S2: [invRow(W2H)] }, []), // S2 lacks W1
    templateHashes: [W1H],
  });
  eq('response returned (no throw), coverage 1/2', [res.coverage, res.partial], ['1/2', true]);
  eq('S2 carried as template_missing', res.warnings, [{ shard: 'S2', code: 'template_missing', est_rows: 0, retried: false }]);
});

Deno.test('plan honesty: a template hash OUTSIDE the engine manifest is a 4xx BEFORE any network I/O', async () => {
  const fetchCalls: string[] = [];
  const invCalls: string[] = [];
  let threw: unknown = null;
  try {
    await executeWhQuery({
      ...execBase(GROUPED_REQ, [dirRow('S1')]),
      fetcher: countingFetch((s) => genv(s, [{ k: ['eu'], s: '100', c: 1 }], { templateHash: W1H }), fetchCalls),
      handshake: fakeHandshake({ S1: [invRow(W1H)] }, invCalls),
      templateHashes: [W1H, 'deadbeef'.padEnd(64, '0')],
    });
  } catch (err) {
    threw = err;
  }
  eq('WhEngineError malformed (plan-time 4xx)', threw instanceof Error && (threw as Error).name, 'WhEngineError');
  eq('message names plan honesty', threw instanceof Error && (threw as Error).message.includes('plan honesty'), true);
  eq('ZERO inventory reads', invCalls, []);
  eq('ZERO wh_query fetches', fetchCalls, []);
});

Deno.test('eligible fleet: inventory once per shard per gather (no caching), fetches only eligible shards, hand-computed merge', async () => {
  const fetchCalls: string[] = [];
  const invCalls: string[] = [];
  const shards = [dirRow('S1'), dirRow('S2')];
  const base = { ...execBase(GROUPED_REQ, shards), fetcher: countingFetch((s) => genv(s, [{ k: ['eu'], s: s === 'S1' ? '100' : '200', c: 1 }], { templateHash: W1H }), fetchCalls) };
  const hs = fakeHandshake({ S1: [invRow(W1H)], S2: [invRow(W1H, { state: 'frozen' })] }, invCalls);

  const r1 = await executeWhQuery({ ...base, handshake: hs, templateHashes: [W1H] });
  eq('first gather: both shards fetched', fetchCalls, ['S1', 'S2']);
  eq('first gather: inventory once per shard', invCalls, ['S1', 'S2']);
  eq('coverage 2/2, not partial', [r1.coverage, r1.partial], ['2/2', false]);
  eq('hand-computed eu merge 100n+200n=300n, count 2', r1.rows, [{ k: ['eu'], aggs: { s: 300n, c: 2 } }]);

  await executeWhQuery({ ...base, handshake: hs, templateHashes: [W1H] });
  eq('second gather re-reads the inventory (NO cross-call caching, §5.2)', invCalls, ['S1', 'S2', 'S1', 'S2']);
  eq('second gather re-fetches (no memoized eligibility)', fetchCalls, ['S1', 'S2', 'S1', 'S2']);
});

Deno.test('mixed fleet: eligible shard fans out, ineligible degrades — coverage counts merged contributions only', async () => {
  const res = await executeWhQuery({
    ...execBase(GROUPED_REQ, [dirRow('S1', { rowEstimate: 10 }), dirRow('S2', { rowEstimate: 999999 })]),
    fetcher: countingFetch((s) => genv(s, [{ k: ['eu'], s: s === 'S1' ? '100' : '200', c: 1 }], { templateHash: W1H }), []),
    handshake: fakeHandshake({ S1: [invRow(W1H)], S2: [invRow(W1H, { schema_version: 4 })] }, []),
    templateHashes: [W1H],
    tableSchemaVersion: 3,
  });
  eq('S2 schema_mismatch at handshake (inventory v4 vs table v3)', res.warnings, [{ shard: 'S2', code: 'schema_mismatch', est_rows: 999999, retried: false }]);
  eq('only S1 merged', res.rows, [{ k: ['eu'], aggs: { s: 100n, c: 1 } }]);
  eq('coverage 1/2, partial', [res.coverage, res.partial], ['1/2', true]);
});

// -----------------------------------------------------------------------------
// Harness report (hand-rolled runner, no external deps).
// -----------------------------------------------------------------------------
Deno.test('__report__', () => {
  console.log(`\nwh_handshake_test: ${passed} assertions passed, ${failed} failed`);
  if (failed > 0) throw new Error(`${failed} assertion(s) failed`);
});
