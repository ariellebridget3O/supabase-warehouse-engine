// =============================================================================
// _shared/wh_handshake_test.ts — RED-first proofs for the §5.2 ENGINE⇄SHARD
// HANDSHAKE + the §6.3 F14/F2 consumption gates (r44) + the r121 OPT-1b
// handshake-fold battery (design_r121_opt1b_handshake_fold.md §5 D6 ledger).
// =============================================================================
// r121 OPT-1b (§5): the per-query inventory sweep is FOLDED into wh_query
// per-call eligibility (classifyFetchFailure maps shard WH400/WH401 → exempt
// template_missing; the sweep survives ONLY as the 1-in-16 SAMPLED backstop,
// K_SAMPLING=7 over sha256(qid)[0]&15) — the engine-level pins below were
// RE-WRITTEN to the folded observables (LETHAL 1/1b/2/3/3b/7 + the
// fail_fast-exemption + fleet pins), and the 7 lethal ADD groups
// (mapping / manifest-max_rows / sampler 4-part / phases / empty-hash guard /
// unmapped fail_fast / F2-clamp-under-drift) are pinned r121-style.
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
  classifyFetchFailure,
  K_SAMPLING,
  qidSampleBucket,
  WhEngineError,
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
// r129 (design_r128_joinplans.md §2.3 — the W6 provenance cell; census §3.3):
// the join-class hash = sha256 of db/shard-templates/W6_colocated_join_agg.sql
// (the body IS the contract). APPENDED in manifest order — never widened
// silently; the eligible-set provenance lives in wh_shard_channel_test.ts.
const W6H = '7004f44de62a8e998ce1915348be0f0fc299ac1aae901966ae7080f7c2cc9576'; // ["groupby","sum","count","count_col"] + join{dim,left,right}

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

/** r121: a shard that REFUSES the wh_query call (the folded per-call
 *  eligibility check) — the warning rides classifyFetchFailure exactly as
 *  the real fetcher emits it. estRows is the TRANSPORT-side estimate; the
 *  mapped-exclusion record must re-attach the DIRECTORY lane instead. */
function refusingFetch(
  warning: { code?: string; httpStatus?: number; stamped?: boolean },
  calls: string[],
  estRows = 0,
): WhShardFetcher {
  return async (shard) => {
    calls.push(shard);
    return { ok: false, warning, estRows } as const;
  };
}

// ---- r121 OPT-1b sampler fixtures (design §1.5; hand-computed sha256) ----
// bucket(qid) = sha256(qid utf8)[0] & 15; fires iff === K_SAMPLING (7).
//   'skip-me'        → 0x97 & 15 = 7  (FIRES)
//   'r121-sample-26' → 0x17 & 15 = 7  (FIRES)
//   'r121-sample-68' → 0xd7 & 15 = 7  (FIRES)
//   'r121-sample-76' → 0x97 & 15 = 7  (FIRES)
//   'r121-in-1'      → 0xb8 & 15 = 8  (out)
//   'r121-skip-0'    → 0x8d & 15 = 13 (out)
//   'r121-skip-1'    → 0x2f & 15 = 15 (out)
//   'qid-7'          → 0xc5 & 15 = 5  (out)
//   '01J9Q1…' (the legacy fixture qid) → 0x5e & 15 = 14 (out — the old
//   per-query-sweep pins inverted exactly because this qid is UNSAMPLED).
const SAMPLED_QID = 'skip-me';
const OUTBUCKET_QID = 'r121-in-1';

/** The grouped plan WITH a limit: the sampled backstop requires limitK
 *  present (the manifest pre-refusal lane shares the planRef), so every
 *  sampled-forced variant below uses this request (bucket 7 → fires). */
const SAMPLED_REQ = parseWhEngineRequest({
  v: 1,
  qid: SAMPLED_QID,
  table: 'orders',
  query: { select: [{ op: 'sum', col: 'amount', alias: 's' }, { op: 'count', alias: 'c' }], groupBy: ['region'], limit: 100 },
});
/** Identical plan, OUT-of-bucket qid (bucket 8): isolates the sampler as the
 *  only difference between the sampled/unsampled arms. */
const OUTBUCKET_REQ = parseWhEngineRequest({
  v: 1,
  qid: OUTBUCKET_QID,
  table: 'orders',
  query: { select: [{ op: 'sum', col: 'amount', alias: 's' }, { op: 'count', alias: 'c' }], groupBy: ['region'], limit: 100 },
});
/** Counting timers (every nowMs call advances 3ms) — the phases pin's
 *  hand-computed lower bounds ride this. */
function stepTimers(): WhEngineTimers {
  let n = 0;
  return {
    nowMs: () => (n += 3),
    startTimeout: (_ms: number) => ({ promise: new Promise<'timeout'>(() => {}), dispose: () => {} }),
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
// r129 (design_r128_joinplans.md §2.3 — the W6 provenance cell): the literal
// gains the W6_colocated_join_agg row APPENDED at index 5 (append-only law —
// the geo-plane cells address rows 0-4 by index), 16 fields incl. the
// additive join binding {dim:'wh_probe_dim', left:'region', right:'region'}
// — deep-equal to the engine row (wh_handshake.ts) and the FILE row.
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
  // r129 (design_r128_joinplans.md §2.3): the join class — hand-transcribed
  // from the manifest.json W6 entry (16 fields; the ONLY row carrying the
  // additive join binding). qc QC6 rides the lint QC_CLASSES extension.
  {
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
  },
];

Deno.test('manifest pin: ENGINE_TEMPLATE_MANIFEST deep-equals the hand-transcribed literal (W1-W5 + r129 W6, all fields)', () => {
  eq('engine manifest === literal copy', ENGINE_TEMPLATE_MANIFEST, MANIFEST_LITERAL);
  // r129 re-pin (census §3.3): LENGTH 5 → 6, W6 APPENDED at index 5 —
  // provenance = design_r128_joinplans.md §2.3 (never widen silently).
  eq('exactly six templates (W1-W5 + W6)', ENGINE_TEMPLATE_MANIFEST.length, 6);
  eq('the six pinned hashes are present (W6 appended in manifest order)', ENGINE_TEMPLATE_MANIFEST.map((r) => r.template_hash), [W1H, W2H, W3H, W4H, W5H, W6H]);
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
Deno.test('LETHAL 1 (folded): ineligible shard IS POSTed, refuses WH400 => mapped template_missing; ZERO inventory GETs on the unsampled path', async () => {
  const fetchCalls: string[] = [];
  const invCalls: string[] = [];
  const res = await executeWhQuery({
    ...execBase(GROUPED_REQ, [dirRow('S1')]),
    fetcher: refusingFetch({ httpStatus: 400, code: 'WH400' }, fetchCalls, 7),
    handshake: fakeHandshake({ S1: [invRow(W2H)] }, invCalls), // W1 required, W2 offered — the drift the shard itself now reports
    templateHashes: [W1H],
  });
  eq('mapped refusal: template_missing class, WH400 detail KEPT, est_rows re-attached from the DIRECTORY lane (0 — no estimate; the transport estRows 7 never wins)', res.warnings, [{ shard: 'S1', code: 'template_missing', est_rows: 0, retried: false, detail: 'WH400' }]);
  eq('shard POSTed then excluded (the fold: eligibility is enforced shard-side per call — the RT was happening anyway)', res.perShard, [{ shard: 'S1', ok: false, latencyMs: res.perShard[0]?.latencyMs, error: 'template_missing' }]);
  eq('coverage 0/1, partial', [res.coverage, res.partial], ['0/1', true]);
  eq('FOLDED law: the wh_query POST happened (the old "ZERO wh_query fetches" pin is inverted)', fetchCalls, ['S1']);
  eq('unsampled steady state: ZERO inventory GETs (fixture qid bucket 14 ≠ 7)', invCalls, []);
  eq('merged output carries NOTHING from the refused shard', res.rows, []);
});

Deno.test('LETHAL 1b (folded): the mapped warning re-attaches est_rows from the directory row_estimate lane (§4.4 degrade-vs-abort weight)', async () => {
  const fetchCalls: string[] = [];
  const res = await executeWhQuery({
    ...execBase(GROUPED_REQ, [dirRow('S1', { rowEstimate: 123 })]),
    fetcher: refusingFetch({ httpStatus: 400, code: 'WH400' }, fetchCalls, 999), // transport estRows 999 must NOT win
    handshake: fakeHandshake({ S1: [] }, []), // wired but UNSED on the unsampled path
    templateHashes: [W1H],
  });
  eq('warning carries the shard est_rows=123 (hand-computed directory lane; detail keeps WH400)', res.warnings, [{ shard: 'S1', code: 'template_missing', est_rows: 123, retried: false, detail: 'WH400' }]);
  eq('the refusal POST happened first (folded law)', fetchCalls, ['S1']);
});

Deno.test('LETHAL 2 (folded, SAMPLED-FORCED): in-bucket qid fires the inventory GET; schema mismatch excludes PRE-execute (zero POSTs)', async () => {
  const fetchCalls: string[] = [];
  const invCalls: string[] = [];
  const res = await executeWhQuery({
    ...execBase(SAMPLED_REQ, [dirRow('S1', { tableSchemaVersion: 3 })]),
    fetcher: countingFetch((s) => genv(s, [{ k: ['eu'], s: '100', c: 1 }]), fetchCalls),
    handshake: fakeHandshake({ S1: [invRow(W1H, { schema_version: 2 })] }, invCalls),
    templateHashes: [W1H],
    tableSchemaVersion: 3,
  });
  eq('schema_mismatch exclusion at the fired sweep (before any call)', res.warnings, [{ shard: 'S1', code: 'schema_mismatch', est_rows: 0, retried: false }]);
  eq('LETHAL: the fired sweep still gates — ZERO wh_query POSTs', fetchCalls, []);
  eq('inventory GET fired exactly once (bucket 7)', invCalls, ['S1']);
  eq('coverage 0/1', res.coverage, '0/1');
});

Deno.test('LETHAL 2 (folded, UNSAMPLED): the schema gate demotes to the ENVELOPE gate AFTER the POST (same plan, out-of-bucket qid)', async () => {
  const fetchCalls: string[] = [];
  const invCalls: string[] = [];
  const res = await executeWhQuery({
    ...execBase(OUTBUCKET_REQ, [dirRow('S1', { tableSchemaVersion: 3 })]),
    fetcher: countingFetch((s) => genv(s, [{ k: ['eu'], s: '100', c: 1 }], { schemaVersion: 2 }), fetchCalls),
    handshake: fakeHandshake({ S1: [invRow(W1H, { schema_version: 2 })] }, invCalls),
    templateHashes: [W1H],
    tableSchemaVersion: 3,
  });
  eq('schema_mismatch exclusion (consumption-site envelope gate — never merges, audit-B F5 accepted)', res.warnings, [{ shard: 'S1', code: 'schema_mismatch', est_rows: 0, retried: false }]);
  eq('the POST happened first (unsampled: no inventory audit)', fetchCalls, ['S1']);
  eq('ZERO inventory GETs (steady state)', invCalls, []);
  eq('coverage 0/1', res.coverage, '0/1');
});

Deno.test('LETHAL 3 (manifest-source): limit 1001 > manifest max_rows 1000 => max_rows_exceeded refusal, ZERO shard POSTs, ZERO inventory (F15 with zero network)', async () => {
  const fetchCalls: string[] = [];
  const invCalls: string[] = [];
  const res = await executeWhQuery({
    ...execBase(parseWhEngineRequest({
      v: 1,
      qid: '01J9Q1ZZZZZZZZZZZZZZZZZZZZ',
      table: 'orders',
      query: { select: [{ op: 'sum', col: 'amount', alias: 's' }, { op: 'count', alias: 'c' }], groupBy: ['region'], limit: 1001 },
    }), [dirRow('S1')]),
    fetcher: countingFetch((s) => genv(s, [{ k: ['eu'], s: '100', c: 1 }]), fetchCalls),
    handshake: fakeHandshake({ S1: [invRow(W1H)] }, invCalls),
    templateHashes: [W1H],
  });
  eq('max_rows_exceeded refusal (manifest-side — the pinned ENGINE_TEMPLATE_MANIFEST.max_rows is the refusal source now)', res.warnings, [{ shard: 'S1', code: 'max_rows_exceeded', est_rows: 0, retried: false }]);
  eq('LETHAL: perShard latencyMs == 0 EXACTLY (no round trip happened — hand-computed law)', res.perShard, [{ shard: 'S1', ok: false, latencyMs: 0, error: 'max_rows_exceeded' }]);
  eq('LETHAL: the wh_query fetch stub recorded NO call', fetchCalls, []);
  eq('ZERO inventory GETs (the pre-refusal is mutually exclusive with the sampled sweep)', invCalls, []);
  eq('coverage 0/1, partial', [res.coverage, res.partial], ['0/1', true]);
});

Deno.test('LETHAL 3b (manifest boundary): limit 1000 == manifest max_rows 1000 passes (only strictly-greater refuses) and the POST happens', async () => {
  const fetchCalls: string[] = [];
  const invCalls: string[] = [];
  const res = await executeWhQuery({
    ...execBase(parseWhEngineRequest({
      v: 1,
      qid: '01J9Q1ZZZZZZZZZZZZZZZZZZZZ',
      table: 'orders',
      query: { select: [{ op: 'sum', col: 'amount', alias: 's' }, { op: 'count', alias: 'c' }], groupBy: ['region'], limit: 1000 },
    }), [dirRow('S1')]),
    fetcher: countingFetch((s) => genv(s, [{ k: ['eu'], s: '100', c: 1 }], { templateHash: W1H }), fetchCalls),
    handshake: fakeHandshake({ S1: [invRow(W1H, { max_rows: 1000 })] }, invCalls),
    templateHashes: [W1H],
  });
  eq('boundary == included: the shard IS fetched (exactly once)', fetchCalls, ['S1']);
  eq('unsampled: the sampled backstop never ran', invCalls, []);
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
  eq('S1 degraded with merge_mismatch (422-class, contract §4.4) — r129 re-pin (census §2.1 #3): the OK entry (S2) carries the additive partial_rows/partial_bytes (ok-arm-only law: the S1 error entry stays byte-identical); bytes computed from the SAME deterministic genv fixture the fetcher returned (R-B2 — never from a run variance)', res.perShard, [
    { shard: 'S1', ok: false, latencyMs: res.perShard[0]?.latencyMs, error: 'merge_mismatch' },
    {
      shard: 'S2',
      ok: true,
      latencyMs: res.perShard[1]?.latencyMs,
      error: null,
      partial_rows: 2,
      partial_bytes: new TextEncoder().encode(JSON.stringify(
        genv('S2', [{ k: ['eu'], s: '200', c: 2 }, { k: ['us'], s: '300', c: 3 }], { templateHash: W1H }).partial,
      )).length,
    },
  ]);
  // Byte-convention hand-check (wh_engine_core.ts:1951-1969 convention of
  // record): the S2 grouped partial serializes to EXACTLY 206 UTF-8 bytes.
  eq('r129 byte-convention hand-check: the S2 grouped partial is 206 bytes', res.perShard[1]?.partial_bytes, 206);
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

Deno.test('LETHAL 7 (folded engine arm, SAMPLED-FORCED): wrong-shape inventory bodies fail CLOSED — template_missing, NEVER a throw', async () => {
  for (const body of ['null', '{"keys":5}', '[null]', 'not json']) {
    const raw = jsonFetch(body as string).fetcher;
    const fetchCalls: string[] = [];
    const res = await executeWhQuery({
      ...execBase(SAMPLED_REQ, [dirRow('S1')]),
      fetcher: countingFetch((s) => genv(s, [{ k: ['eu'], s: '100', c: 1 }]), fetchCalls),
      handshake: makeWhHandshake({ fetcher: raw, shardServiceKey: () => 'sk' }),
      templateHashes: [W1H],
    });
    eq(`inventory body ${body} => template_missing exclusion, no throw`, res.warnings.map((w) => w.code), ['template_missing']);
    eq(`...the fired sweep still gates: ZERO POSTs`, fetchCalls, []);
    eq(`...coverage 0/1`, res.coverage, '0/1');
  }
});

Deno.test('fail_fast + SAMPLED handshake exclusions => NO 5xx (§5.2 exclusions degrade+warn, exempt from collect-then-fail)', async () => {
  const res = await executeWhQuery({
    ...execBase(parseWhEngineRequest({
      v: 1,
      qid: SAMPLED_QID,
      table: 'orders',
      query: { select: [{ op: 'sum', col: 'amount', alias: 's' }, { op: 'count', alias: 'c' }], groupBy: ['region'], limit: 100 },
      coverage_mode: 'fail_fast',
    }), [dirRow('S1'), dirRow('S2')]),
    fetcher: countingFetch((s) => genv(s, [{ k: ['eu'], s: '100', c: 1 }]), []),
    handshake: fakeHandshake({ S1: [invRow(W1H)], S2: [invRow(W2H)] }, []), // S2 lacks W1
    templateHashes: [W1H],
  });
  eq('response returned (no throw), coverage 1/2', [res.coverage, res.partial], ['1/2', true]);
  eq('S2 carried as template_missing (the fired sweep\'s exclusion is fail_fast-exempt)', res.warnings, [{ shard: 'S2', code: 'template_missing', est_rows: 0, retried: false }]);
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

Deno.test('eligible fleet (SAMPLED-FORCED): inventory once per shard per fired sweep (no caching), fetches only eligible shards, hand-computed merge; the SAME qid re-fires (stateless sampler)', async () => {
  const fetchCalls: string[] = [];
  const invCalls: string[] = [];
  const shards = [dirRow('S1'), dirRow('S2')];
  const base = { ...execBase(SAMPLED_REQ, shards), fetcher: countingFetch((s) => genv(s, [{ k: ['eu'], s: s === 'S1' ? '100' : '200', c: 1 }], { templateHash: W1H }), fetchCalls) };
  const hs = fakeHandshake({ S1: [invRow(W1H)], S2: [invRow(W1H, { state: 'frozen' })] }, invCalls);

  const r1 = await executeWhQuery({ ...base, handshake: hs, templateHashes: [W1H] });
  eq('first gather: both shards fetched', fetchCalls, ['S1', 'S2']);
  eq('first gather: inventory once per shard (the fired sweep sweeps the WHOLE selected population)', invCalls, ['S1', 'S2']);
  eq('coverage 2/2, not partial', [r1.coverage, r1.partial], ['2/2', false]);
  eq('hand-computed eu merge 100n+200n=300n, count 2', r1.rows, [{ k: ['eu'], aggs: { s: 300n, c: 2 } }]);

  await executeWhQuery({ ...base, handshake: hs, templateHashes: [W1H] });
  eq('second gather re-reads the inventory (NO cross-call caching, §5.2; the same qid re-fires — the sampler is stateless)', invCalls, ['S1', 'S2', 'S1', 'S2']);
  eq('second gather re-fetches (no memoized eligibility)', fetchCalls, ['S1', 'S2', 'S1', 'S2']);
});

Deno.test('mixed fleet (SAMPLED-FORCED): eligible shard fans out, ineligible degrades at the fired sweep — coverage counts merged contributions only', async () => {
  const res = await executeWhQuery({
    ...execBase(SAMPLED_REQ, [dirRow('S1', { rowEstimate: 10 }), dirRow('S2', { rowEstimate: 999999 })]),
    fetcher: countingFetch((s) => genv(s, [{ k: ['eu'], s: s === 'S1' ? '100' : '200', c: 1 }], { templateHash: W1H }), []),
    handshake: fakeHandshake({ S1: [invRow(W1H)], S2: [invRow(W1H, { schema_version: 4 })] }, []),
    templateHashes: [W1H],
    tableSchemaVersion: 3,
  });
  eq('S2 schema_mismatch at the fired sweep (inventory v4 vs table v3, est_rows from the directory lane)', res.warnings, [{ shard: 'S2', code: 'schema_mismatch', est_rows: 999999, retried: false }]);
  eq('only S1 merged', res.rows, [{ k: ['eu'], aggs: { s: 100n, c: 1 } }]);
  eq('coverage 1/2, partial', [res.coverage, res.partial], ['1/2', true]);
});

// =============================================================================
// 7. r121 OPT-1b ADD groups (design §5 D6 ledger; B = manifest max_rows is
//    covered by the LETHAL 3/3b rewrites above).
// =============================================================================

// ---- ADD #1 (§5.1): the mapping pin — THE mutation target (M1) ----
Deno.test('r121 ADD (mapping pin, the mutation target): shard WH400 refusal maps to the FULL template_missing record — est_rows re-attached from the DIRECTORY lane, detail kept, latency measured', async () => {
  const fetchCalls: string[] = [];
  const invCalls: string[] = [];
  const res = await executeWhQuery({
    ...execBase(GROUPED_REQ, [dirRow('S1', { rowEstimate: 4242 })]),
    fetcher: refusingFetch({ httpStatus: 400, code: 'WH400' }, fetchCalls, 999999), // transport estRows must LOSE to the directory lane
    handshake: fakeHandshake({}, invCalls),
    templateHashes: [W1H],
  });
  eq('FULL mapped exclusion record (byte-pinned; the transport estRows 999999 never wins)', res.warnings, [
    { shard: 'S1', code: 'template_missing', est_rows: 4242, retried: false, detail: 'WH400' },
  ]);
  eqTrue('perShard: ok false, error template_missing, latencyMs > 0 (the refusal RT measured)', res.perShard.length === 1 && res.perShard[0]?.ok === false && (res.perShard[0]?.latencyMs ?? 0) > 0 && res.perShard[0]?.error === 'template_missing');
  eq('unsampled: ZERO inventory GETs (the mapping IS the eligibility check)', invCalls, []);
  eq('coverage 0/1, partial (degrade, never 5xx)', [res.coverage, res.partial], ['0/1', true]);
});

Deno.test('r121 ADD (mapping fail_fast twin): WH400 refusal => template_missing is EXEMPT from collect-then-fail (NO 5xx)', async () => {
  const res = await executeWhQuery({
    ...execBase(parseWhEngineRequest({
      v: 1,
      qid: '01J9Q1ZZZZZZZZZZZZZZZZZZZZ',
      table: 'orders',
      query: { select: [{ op: 'sum', col: 'amount', alias: 's' }, { op: 'count', alias: 'c' }], groupBy: ['region'] },
      coverage_mode: 'fail_fast',
    }), [dirRow('S1', { rowEstimate: 5 })]),
    fetcher: refusingFetch({ httpStatus: 400, code: 'WH400' }, [], 0),
    templateHashes: [W1H],
  });
  eq('NO 5xx — the response returned (mapped refusal rides the exempt class)', res.coverage, '0/1');
  eq('the mapped refusal warning (est_rows 5 from the directory lane, detail kept)', res.warnings, [
    { shard: 'S1', code: 'template_missing', est_rows: 5, retried: false, detail: 'WH400' },
  ]);
});

// ---- ADD #3 (§5.3): the sampler 4-part pin (mutation target M2) ----
Deno.test('r121 ADD (sampler i+iv): qidSampleBucket determinism — hand-computed sha256(qid)[0]&15 vectors + the K_SAMPLING=7 import identity', async () => {
  eq('K_SAMPLING is the exported constant 7', K_SAMPLING, 7);
  // IN-BUCKET (fire): sha256 first bytes 0x97 / 0x17 / 0xd7 / 0x97 → &15 = 7
  for (const [qid, want] of [['skip-me', 7], ['r121-sample-26', 7], ['r121-sample-68', 7], ['r121-sample-76', 7]] as const) {
    eq(`bucket('${qid}') === ${want} (in-bucket, FIRES)`, await qidSampleBucket(qid), want);
  }
  // OUT-BUCKET (never fires): 0xb0→0, 0xb8→8, 0x8d→13, 0x2f→15, 0xc5→5.
  // NOTE: 'r121-sample-5' was listed in-bucket in the leg-1 brief, but the
  // real Web Crypto sha256 first byte is 0xb0 → bucket 0 — pinned at the
  // TRUE computed value (defect noted in the ledger report).
  for (const [qid, want] of [['r121-sample-5', 0], ['r121-in-1', 8], ['r121-skip-0', 13], ['r121-skip-1', 15], ['qid-7', 5]] as const) {
    eq(`bucket('${qid}') === ${want} (out-bucket, never fires)`, await qidSampleBucket(qid), want);
  }
  eqTrue('the fired bucket IS the pinned constant (predicate identity)', (await qidSampleBucket('skip-me')) === K_SAMPLING);
});

Deno.test('r121 ADD (sampler iii): statelessness — the same qid always yields the same bucket/decision (no isolate state, recycle-safe)', async () => {
  const a = await qidSampleBucket('skip-me');
  const b = await qidSampleBucket('skip-me');
  eq('in-bucket qid: repeat computation identical', a, b);
  const c = await qidSampleBucket('r121-in-1');
  const d = await qidSampleBucket('r121-in-1');
  eq('out-bucket qid: repeat computation identical', c, d);
  eqTrue('the out-bucket qid genuinely differs from the fired bucket', c !== K_SAMPLING);
});

Deno.test('r121 ADD (sampler ii, recording fake): in-bucket qid => inventory GET fired EXACTLY once per shard; out-bucket => ZERO GETs (POSTs ride unchanged)', async () => {
  const invIn: string[] = [];
  const fetchIn: string[] = [];
  await executeWhQuery({
    ...execBase(SAMPLED_REQ, [dirRow('S1'), dirRow('S2')]),
    fetcher: countingFetch((s) => genv(s, [{ k: ['eu'], s: '100', c: 1 }], { templateHash: W1H }), fetchIn),
    handshake: fakeHandshake({ S1: [invRow(W1H)], S2: [invRow(W1H)] }, invIn),
    templateHashes: [W1H],
  });
  eq('in-bucket: one inventory GET per shard (the sampled backstop sweeps the whole population)', invIn, ['S1', 'S2']);
  eq('in-bucket: eligible verdicts fan out (one POST per shard)', fetchIn, ['S1', 'S2']);

  const invOut: string[] = [];
  const fetchOut: string[] = [];
  await executeWhQuery({
    ...execBase(OUTBUCKET_REQ, [dirRow('S1'), dirRow('S2')]),
    fetcher: countingFetch((s) => genv(s, [{ k: ['eu'], s: '100', c: 1 }], { templateHash: W1H }), fetchOut),
    handshake: fakeHandshake({ S1: [invRow(W1H)], S2: [invRow(W1H)] }, invOut),
    templateHashes: [W1H],
  });
  eq("out-bucket: ZERO inventory GETs (the fold's steady state)", invOut, []);
  eq('out-bucket: POSTs happen unchanged', fetchOut, ['S1', 'S2']);
});

// ---- ADD #4 (§5.4): the phases pin ----
Deno.test('r121 ADD (phases pin): injected counting timers — pre_chain threaded, handshake_ms == 0 unsampled / > 0 sampled, fanout_ms measured; phases ABSENT on error envelopes; present on the empty-selection success', async () => {
  const okEnv = countingFetch((s) => genv(s, [{ k: ['eu'], s: '100', c: 1 }], { templateHash: W1H }), []);

  // (1) UNSAMPLED success: exact hand-computed pre_chain (threaded), handshake 0, fanout measured.
  const res1 = await executeWhQuery({
    ...execBase(OUTBUCKET_REQ, [dirRow('S1')]),
    timers: stepTimers(),
    fetcher: okEnv,
    templateHashes: [W1H],
    timings: { preChainMs: 123 },
  });
  eq('unsampled: pre_chain_ms is the ENTRYPOINT-THREADED value (exact 123)', res1.phases?.pre_chain_ms, 123);
  eq('unsampled: handshake_ms == 0 (the fold steady state)', res1.phases?.handshake_ms, 0);
  eqTrue('unsampled: fanout_ms measured (>= one counting step — the runFanout block wall)', (res1.phases?.fanout_ms ?? 0) >= 1);

  // (2) SAMPLED success: handshake_ms > 0 (the fired sweep is measured).
  const res2 = await executeWhQuery({
    ...execBase(SAMPLED_REQ, [dirRow('S1')]),
    timers: stepTimers(),
    fetcher: okEnv,
    handshake: fakeHandshake({ S1: [invRow(W1H)] }, []),
    templateHashes: [W1H],
    timings: { preChainMs: 123 },
  });
  eq('sampled: pre_chain_ms still the threaded 123', res2.phases?.pre_chain_ms, 123);
  eqTrue('sampled: handshake_ms > 0 (the fired sweep block measured)', (res2.phases?.handshake_ms ?? 0) > 0);
  eqTrue('sampled: fanout_ms >= 0', (res2.phases?.fanout_ms ?? -1) >= 0);

  // (3) ERROR envelope: fail_fast 5xx — phases can never ride it (post-assembly injection is bypassed by the throw).
  let threw: unknown = null;
  try {
    await executeWhQuery({
      ...execBase(parseWhEngineRequest({
        v: 1,
        qid: '01J9Q1ZZZZZZZZZZZZZZZZZZZZ',
        table: 'orders',
        query: { select: [{ op: 'sum', col: 'amount', alias: 's' }, { op: 'count', alias: 'c' }], groupBy: ['region'] },
        coverage_mode: 'fail_fast',
      }), [dirRow('S1')]),
      timers: stepTimers(),
      fetcher: refusingFetch({ httpStatus: 400, code: 'WH402' }, [], 0),
      templateHashes: [W1H],
    });
  } catch (err) {
    threw = err;
  }
  eq('fail_fast threw WhEngineError internal', threw instanceof WhEngineError && (threw as WhEngineError).code, 'internal');
  eqTrue('phases ABSENT on the error payload', threw !== null && !('phases' in (threw as object)));

  // (4) EMPTY-SELECTION success: phases present with EXACT zeros for the never-ran blocks.
  const pruneCols: Record<string, string> = { region: 'text', amount: 'numeric', created_at: 'timestamptz' };
  const prunedFleet = [
    { ...dirRow('S1'), key_min: '2026-01-01', key_max: '2026-06-30' },
    { ...dirRow('S2'), key_min: '2026-07-01', key_max: '2026-12-31' },
  ];
  const res4 = await executeWhQuery({
    req: parseWhEngineRequest({
      v: 1,
      qid: '01J9Q1ZZZZZZZZZZZZZZZZZZZZ',
      table: 'orders',
      query: {
        select: [{ op: 'sum', col: 'amount', alias: 's' }, { op: 'count', alias: 'c' }],
        groupBy: ['region'],
        where: [{ col: 'created_at', op: 'lt', value: '2025-06-30' }], // prunes BOTH shards (hi < key_min on every row)
      },
    }),
    columnTypes: pruneCols,
    columnScales: SCALES,
    directoryRows: prunedFleet,
    shardKeyColumn: 'created_at',
    shardKeyType: 'range',
    directoryVersion: 7,
    timers: stepTimers(),
    fetcher: okEnv,
    timings: { preChainMs: 123 },
  });
  eq('empty-selection success carries the EXACT phases {123, 0, 0} (hand-computed: the handshake/fanout blocks never ran)', res4.phases, { pre_chain_ms: 123, handshake_ms: 0, fanout_ms: 0 });
  eq('empty coverage 0/0', res4.coverage, '0/0');
});

// ---- r124 OPT-3 (design §1, audit A ⟫A-3): the sub-span conditional-spread
// pin — timings {dirMs, fenceMs} spread INTO phases; legacy {preChainMs}-
// only callers keep the exact {123, 0, 0} shape (the (4) pin above).
Deno.test('r124 ADD (sub-span spread): threaded {dirMs, fenceMs} land on the phases EXACTLY (dir_ms/fence_ms); absent keys stay absent', async () => {
  const pruneCols: Record<string, string> = { region: 'text', amount: 'numeric', created_at: 'timestamptz' };
  const prunedFleet = [
    { ...dirRow('S1'), key_min: '2026-01-01', key_max: '2026-06-30' },
    { ...dirRow('S2'), key_min: '2026-07-01', key_max: '2026-12-31' },
  ];
  const req = {
    v: 1,
    qid: '01J9Q1ZZZZZZZZZZZZZZZZZZZZ',
    table: 'orders',
    query: {
      select: [{ op: 'sum', col: 'amount', alias: 's' }, { op: 'count', alias: 'c' }],
      groupBy: ['region'],
      where: [{ col: 'created_at', op: 'lt', value: '2025-06-30' }], // prunes BOTH shards
    },
  };
  const res = await executeWhQuery({
    req: parseWhEngineRequest(req),
    columnTypes: pruneCols,
    columnScales: SCALES,
    directoryRows: prunedFleet,
    shardKeyColumn: 'created_at',
    shardKeyType: 'range',
    directoryVersion: 7,
    timers: stepTimers(),
    fetcher: countingFetch((s) => genv(s, [{ k: ['eu'], s: '100', c: 1 }], { templateHash: W1H }), []),
    timings: { preChainMs: 123, dirMs: 4, fenceMs: 9 },
  });
  eq(
    'sub-spans: the EXACT threaded phases {123, 0, 0, dir_ms 4, fence_ms 9} (conditional spread, no extra keys)',
    res.phases,
    { pre_chain_ms: 123, handshake_ms: 0, fanout_ms: 0, dir_ms: 4, fence_ms: 9 },
  );
});

// ---- ADD #5 (§5.5): the empty-hash guard pin ----
Deno.test('r121 ADD (empty-hash guard): an UNTEMPLATED plan fires ZERO inventory GETs even with an in-bucket qid (the vacuous audit is retired)', async () => {
  const fetchCalls: string[] = [];
  const invCalls: string[] = [];
  const res = await executeWhQuery({
    ...execBase(SAMPLED_REQ, [dirRow('S1'), dirRow('S2')]), // in-bucket qid + limit present — only the hash guard can block
    fetcher: countingFetch((s) => genv(s, [{ k: ['eu'], s: '100', c: 1 }]), fetchCalls),
    handshake: fakeHandshake({}, invCalls),
    // templateHashes ABSENT — the untemplated plan (templateHashes.length === 0)
  });
  eq('ZERO inventory GETs regardless of the bucket (the planRef.templateHashes.length > 0 guard)', invCalls, []);
  eq('the fan-out proceeded normally (no gate on an untemplated plan)', fetchCalls, ['S1', 'S2']);
  eq('no warnings, full coverage', [res.warnings, res.coverage], [[], '2/2']);
});

// ---- ADD #6 (§5.6): the unmapped fail_fast pin (unchanged-class byte-pins) ----
Deno.test('r121 ADD (unmapped classes byte-pin): WH402/WH403 stay excluded; 5xx/network keep the transport class — the mapping never re-masks', () => {
  eq('WH402 (hash-shape) => {excluded, detail WH402} — NOT template_missing', classifyFetchFailure({ httpStatus: 400, code: 'WH402' }), { code: 'excluded', detail: 'WH402' });
  eq('WH403 (registry integrity) => {excluded, detail WH403}', classifyFetchFailure({ httpStatus: 400, code: 'WH403' }), { code: 'excluded', detail: 'WH403' });
  eq('WH500 riding 5xx => http_5xx (transport class wins, never re-masked)', classifyFetchFailure({ httpStatus: 503, code: 'WH500' }), { code: 'http_5xx', stamped: false, detail: 'WH500' });
  eq('WH400 riding 5xx => http_5xx + WH400 detail (the refusal code never masks a 5xx — design §1.3a more-honest law)', classifyFetchFailure({ httpStatus: 502, code: 'WH400' }), { code: 'http_5xx', stamped: false, detail: 'WH400' });
  eq('network => network', classifyFetchFailure({ code: 'network' }), { code: 'network' });
});

Deno.test('r121 ADD (unmapped fail_fast): WH402 refusal => excluded => fail_fast 5xx (unchanged class, collect-then-fail)', async () => {
  let threw: unknown = null;
  try {
    await executeWhQuery({
      ...execBase(parseWhEngineRequest({
        v: 1,
        qid: '01J9Q1ZZZZZZZZZZZZZZZZZZZZ',
        table: 'orders',
        query: { select: [{ op: 'sum', col: 'amount', alias: 's' }, { op: 'count', alias: 'c' }], groupBy: ['region'] },
        coverage_mode: 'fail_fast',
      }), [dirRow('S1')]),
      fetcher: refusingFetch({ httpStatus: 400, code: 'WH402' }, [], 0),
      templateHashes: [W1H],
    });
  } catch (err) {
    threw = err;
  }
  eq('fail_fast 5xx (excluded is NOT exempt — only template_missing/schema_mismatch/max_rows_exceeded are)', threw instanceof WhEngineError && (threw as WhEngineError).code, 'internal');
  eq('perShard carries the excluded classification', (threw as WhEngineError).perShard, [
    { shard: 'S1', ok: false, latencyMs: (threw as WhEngineError).perShard?.[0]?.latencyMs, error: 'excluded' },
  ]);
});

// ---- ADD #7 (§5.7): the F2-clamp-under-drift pin (documented vocabulary case) ----
Deno.test('r121 ADD (F2-clamp-under-drift): registry max_rows < manifest — the sampled sweep sees the drift, the shard CLAMPS (truncated:true) and F2 codes truncated_groupby', async () => {
  const fetchCalls: string[] = [];
  const invCalls: string[] = [];
  const res = await executeWhQuery({
    ...execBase(parseWhEngineRequest({
      v: 1,
      qid: SAMPLED_QID,
      table: 'orders',
      query: { select: [{ op: 'sum', col: 'amount', alias: 's' }, { op: 'count', alias: 'c' }], groupBy: ['region'], limit: 500 },
    }), [dirRow('S1')]),
    // The shard executes, its sentinel clamps at the REGISTRY cap (500 < the
    // 800 groups the data would yield) and stamps truncated:true.
    fetcher: countingFetch((s) => genv(s, [{ k: ['eu'], s: '100', c: 1 }], { truncated: true, rowCountOverride: 800, templateHash: W1H }), fetchCalls),
    // Registry drift: the live inventory pins max_rows 500 (manifest says 1000).
    handshake: fakeHandshake({ S1: [invRow(W1H, { max_rows: 500 })] }, invCalls),
    templateHashes: [W1H],
  });
  eq('sampled sweep read the drifted registry (one GET; K 500 == registry max_rows 500 stays ELIGIBLE at the boundary)', invCalls, ['S1']);
  eq('the POST happened (manifest 1000 >= 500 — no pre-refusal on the manifest lane)', fetchCalls, ['S1']);
  eq('the clamped partial NEVER merges: F2 truncated_groupby, est_rows partial-derived 800 (the §6.3 F2 vocabulary under drift)', res.warnings, [{
    shard: 'S1',
    code: 'truncated_groupby',
    est_rows: 800,
    retried: false,
    detail: 'grouped partial carries the truncation sentinel (truncated=true) — never silently merged (§6.3 F2)',
  }]);
  eq('coverage 0/1, partial', [res.coverage, res.partial], ['0/1', true]);
  eq('rows carry nothing from the clamped shard', res.rows, []);
});

// -----------------------------------------------------------------------------
// Harness report (hand-rolled runner, no external deps).
// -----------------------------------------------------------------------------
Deno.test('__report__', () => {
  console.log(`\nwh_handshake_test: ${passed} assertions passed, ${failed} failed`);
  if (failed > 0) throw new Error(`${failed} assertion(s) failed`);
});
