// =============================================================================
// _shared/wh_entrypoint_test.ts — r40 smoke pins for the warehouse-engine wiring
// =============================================================================
// The r39 wiring shipped with ZERO direct coverage (the entrypoint lived in
// index.ts's Deno.serve — not importable offline). The fresh-eyes review
// (maxxing-r40-wiring-review) found three P0s behind that blind spot; this
// battery pins the handler NOW living in wh_entrypoint.ts (extracted, deps
// injected, deploy.ts r16-F7 pattern):
//   * auth: 401 auth_kind discriminator shapes, auth PRECEDES routing,
//     missing-WHE_BEARER_TOKEN 500 (§4.2)
//   * /health L2: config-probe-only shape (r40 P2-1), unauthenticated
//   * /query malformed path: invalid JSON, §4.3 parse rejects — the P0-1 pin
//     (parseWhEngineRequest is CALLED; {} / v:2 / having / bad-ident die 400)
//   * /query P2-6 deploy gate: hasRealFetcher=false => 500 BEFORE any
//     directory work (a deployed stub must never serve plausible empty 200s)
//   * success path: envelope assembly, per-table projection (P0-3 pin:
//     foreign-table rows never reach the fetcher), P0-2 pin (shard from
//     project_ref mapping), tier_warm 404 + directory_version on the error
//     body (P2-3), fail_fast 500 payload, generic-500 scrub (P3-4)
//   * wh_directory_reader (r120 OPT-1): the atomic {version, rows} read — ONE
//     rpc POST (exactly-once pin), mapped rows, and every loud arm (rpc error,
//     non-object payload, config-row-missing F7, non-integer version P2-5,
//     non-array rows, fleet-shape bound F2 + boundary-ok) + the F3
//     plain-Error law. The pagination loop + P2-4 double-probe pins are
//     RETIRED with the machinery (one Postgres snapshot by construction)
//   * static pins: index.ts deploy-gate wiring of the shipped shell (§4.1;
//     the fleet-api discovery-route pins stay with fleet-manager)
//   * r57 wh_ryw v1: the WH_RYW_V1 flip lever (default OFF — a min_lsn-pinned
//     /query 400s naming the lever; gate ON = the r47 errata behavior; G6a
//     eventual-consistency reads unaffected; additive-only default path) + the
//     m5 dependency-graph cell (the engine imports NOTHING from the relay path)
//
// Runs offline:  deno test --no-check -q --allow-env --allow-read supabase/functions/_shared/wh_entrypoint_test.ts
// =============================================================================

import { handleWhEngineRequest, rywGateEnabledFromEnv, FLIP_hasRealFetcher } from './wh_entrypoint.ts';
import type { WhEngineDeps } from './wh_entrypoint.ts';
import { makeDirectoryReader, toWhDirectoryRow, coerceDirectoryVersion } from './wh_directory_reader.ts';
import { signDirectorySnapshot, signSnapshotBlob, verifyDirectorySnapshot } from './wh_snapshot.ts';
import {
  // r124 OPT-3: the both-fail precedence discriminator arm THROWS this from
  // the injected directory fake (a WhEngineError is relayed RAW on the wire,
  // unlike a plain Error which scrubs — the pair makes the dir-wins
  // precedence observable through the error envelope).
  WhEngineError,
} from './wh_engine_core.ts';
import type { WhDirectoryRow, WhGeoDirectoryRow, WhShardFetcher, WhEngineTimers } from './wh_engine_core.ts';
import type { WhPartialEnvelope } from './wh_types.ts';
import type { TemplateInventoryRow, WhShardHandshake } from './wh_handshake.ts';
import type { WhFenceRead, WhFenceValues } from './geo_write_fence.ts';
import indexSrc from '../warehouse-engine/index.ts' with { type: 'text' };

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

function ok(name: string, cond: boolean, detail = 'condition false'): void {
  if (cond) {
    passed++;
    console.log(`  ok  ${name}`);
  } else {
    failed++;
    failures.push(`${name}\n      ${detail}`);
    console.error(`FAIL  ${name}\n      ${detail}`);
  }
}

async function withEnv(key: string, value: string | undefined, body: () => Promise<void>): Promise<void> {
  const saved = Deno.env.get(key);
  try {
    if (value === undefined) Deno.env.delete(key);
    else Deno.env.set(key, value);
    await body();
  } finally {
    if (saved === undefined) Deno.env.delete(key);
    else Deno.env.set(key, saved);
  }
}

// -----------------------------------------------------------------------------
// Fixtures
// -----------------------------------------------------------------------------

const TOKEN = 'test-fleet-token';

function dirRow(
  shard: string,
  table: string,
  opts: { keyMin?: string | null; keyMax?: string | null; hashSlot?: number | null; schemaVersion?: number } = {},
): WhDirectoryRow {
  return {
    shard,
    key_min: opts.keyMin ?? null,
    key_max: opts.keyMax ?? null,
    hash_slot: opts.hashSlot ?? null,
    state: 'serving',
    platform_status: 'ACTIVE_HEALTHY',
    schema_version: opts.schemaVersion ?? 3,
    last_health_at: '2026-09-28T00:00:00.000000Z',
    logical_name: table,
    shard_key_type: 'none',
    shard_key_column: null,
    table_schema_version: opts.schemaVersion ?? 3,
  };
}

const ORDERS_ROWS = [dirRow('shard-a', 'orders'), dirRow('shard-b', 'orders')];
const EVENTS_ROWS = [dirRow('shard-c', 'events')];

function scalarEnv(shard: string, table: string, count = 5, schemaVersion = 3): WhPartialEnvelope {
  return {
    v: 1,
    shard,
    table,
    schema_version: schemaVersion,
    partial: {
      kind: 'scalar',
      aggs: { c: { op: 'count' } },
      rows: [{ k: [], a: { c: count } }],
      rowCount: 1,
      more: false,
    },
  };
}

function okFetch(envelopeFor: (shard: string) => WhPartialEnvelope, seen?: string[]): WhShardFetcher {
  return async (shard: string) => {
    seen?.push(shard);
    return { ok: true, envelope: envelopeFor(shard), estRows: 10 };
  };
}

function failFetch(warning: { code?: string; httpStatus?: number; stamped?: boolean }): WhShardFetcher {
  return async () => ({ ok: false, warning, estRows: 0 });
}

interface Counters { probe: number; read: number; fence: number }

function makeDeps(opts: {
  rows?: WhDirectoryRow[];
  version?: number;
  fetcher?: WhShardFetcher;
  hasRealFetcher?: boolean;
  probeThrows?: Error;
  readThrows?: Error;
  snapshotKey?: string;
  handshake?: WhShardHandshake;
  // r57 wh_ryw: geo-plane fakes (wired ONLY when either is provided — the
  // pre-r57 dep shape stays byte-identical for every existing cell).
  geoMode?: string | null;
  geoRows?: WhGeoDirectoryRow[];
  // r57 wh_ryw: the flip lever injection (absent => the handler default OFF).
  rywGateEnabled?: boolean;
  // r69 battery: the RPC-plane mode flag injection (absent => the handler
  // default OFF — the same additive shape as rywGateEnabled above).
  rpcMode?: boolean;
  // ---- r124 OPT-3/A8 battery hooks (additive — absent => the pre-r124 dep
  // shape; every existing cell is untouched) ----
  // A8: the generated build stamp (present/absent arms on the /health L2
  // shape — the shell threads it in prod; tests inject it here).
  engineBuild?: string;
  // OPT-3: the r49 fence fake (wrapped by the harness counter — pins read
  // counters.fence, never the raw fn) + the dispatch identity.
  fetchFenceConfig?: () => Promise<WhFenceRead>;
  ownProjectRef?: string;
  // OPT-3: a CONTROLLABLE read impl (the parallel-firing proof resolves it
  // manually); the harness counter still wraps it. Takes precedence over
  // readThrows when both are set.
  readImpl?: () => Promise<{ rows: WhDirectoryRow[]; version: number }>;
  // OPT-3: sub-span invariant timers (the step-clock arms inject a counting
  // nowMs; absent => the handler default real timers).
  timers?: WhEngineTimers;
} = {}): { deps: WhEngineDeps; counters: Counters } {
  const counters: Counters = { probe: 0, read: 0, fence: 0 };
  const rows = opts.rows ?? ORDERS_ROWS;
  const version = opts.version ?? 42;
  const deps: WhEngineDeps = {
    probeDirectoryVersion: async () => {
      counters.probe++;
      if (opts.probeThrows) throw opts.probeThrows;
      return version;
    },
    readDirectory: async () => {
      counters.read++;
      if (opts.readImpl !== undefined) return opts.readImpl();
      if (opts.readThrows) throw opts.readThrows;
      return { rows, version };
    },
    fetcher: opts.fetcher ?? okFetch((s) => scalarEnv(s, 'orders')),
    hasRealFetcher: opts.hasRealFetcher ?? true,
    ...(opts.handshake !== undefined ? { handshake: opts.handshake } : {}),
    ...(opts.snapshotKey !== undefined ? { snapshotKey: opts.snapshotKey } : {}),
    ...(opts.geoMode !== undefined || opts.geoRows !== undefined
      ? {
        // r47 read-plane fakes: unreadable mode => null (G1 fail-closed);
        // rows default [] (G3 no_serving_row). readGeoDirectory throws like
        // the real reader on law breakage — fakes stay well-formed.
        readGeoMode: async () => opts.geoMode ?? null,
        ...(opts.geoRows !== undefined ? { readGeoDirectory: async () => opts.geoRows } : {}),
      }
      : {}),
    ...(opts.rywGateEnabled !== undefined ? { rywGateEnabled: opts.rywGateEnabled } : {}),
    ...(opts.rpcMode !== undefined ? { rpcMode: opts.rpcMode } : {}),
    // r124 OPT-3/A8 hooks: the fence fake is WRAPPED so pins observe the
    // consult count (the increment happens synchronously at invocation —
    // observable before the promise settles); the stamp + timers ride the
    // same spread-conditional shape as every optional dep above.
    ...(opts.fetchFenceConfig !== undefined
      ? {
        fetchFenceConfig: async () => {
          counters.fence++;
          return opts.fetchFenceConfig!();
        },
      }
      : {}),
    ...(opts.ownProjectRef !== undefined ? { ownProjectRef: opts.ownProjectRef } : {}),
    ...(opts.engineBuild !== undefined ? { engineBuild: opts.engineBuild } : {}),
    ...(opts.timers !== undefined ? { timers: opts.timers } : {}),
  };
  return { deps, counters };
}

function req(path: string, opts: {
  method?: string;
  token?: string | null;
  apikey?: string | null;
  body?: unknown;
  rawBody?: string;
} = {}): Request {
  const headers: Record<string, string> = {};
  if (opts.token !== null) headers['authorization'] = `Bearer ${opts.token ?? TOKEN}`;
  if (opts.apikey !== null) headers['apikey'] = opts.apikey ?? TOKEN;
  if (opts.body !== undefined || opts.rawBody !== undefined) headers['content-type'] = 'application/json';
  return new Request(`https://ref.supabase.co/functions/v1/warehouse-engine${path}`, {
    method: opts.method ?? 'GET',
    headers,
    ...(opts.body !== undefined ? { body: JSON.stringify(opts.body) } : {}),
    ...(opts.rawBody !== undefined ? { body: opts.rawBody } : {}),
  });
}

const COUNT_REQ = {
  v: 1,
  qid: 'q-1',
  table: 'orders',
  query: { select: [{ op: 'count', alias: 'c' }] },
  coverage_mode: 'best_effort',
};

// -----------------------------------------------------------------------------
// Auth (§4.2)
// -----------------------------------------------------------------------------
async function authPins(): Promise<void> {
  console.log('auth §4.2');
  await withEnv('WHE_BEARER_TOKEN', TOKEN, async () => {
    const { deps } = makeDeps();
    const missing = await handleWhEngineRequest(req('/query', { method: 'POST', token: null, body: COUNT_REQ }), deps);
    eq('missing bearer -> 401', missing.status, 401);
    const mb = await missing.json();
    eq('missing_bearer shape', mb, { v: 1, error: { code: 'malformed', message: 'auth rejected before route dispatch (missing_bearer)', auth_kind: 'missing_bearer' } });

    const invalid = await handleWhEngineRequest(req('/query', { method: 'POST', token: 'wrong-token', body: COUNT_REQ }), deps);
    eq('wrong token -> 401', invalid.status, 401);
    eq('invalid_token auth_kind', (await invalid.json()).error.auth_kind, 'invalid_token');

    const noApikey = await handleWhEngineRequest(req('/query', { method: 'POST', apikey: null, body: COUNT_REQ }), deps);
    eq('missing apikey -> 401', noApikey.status, 401);
    eq('bad_apikey auth_kind', (await noApikey.json()).error.auth_kind, 'bad_apikey');

    // auth precedes routing: an unauthenticated hit to an unknown route is a
    // 401, never the 400 no-route fallback.
    const preRoute = await handleWhEngineRequest(req('/bogus', { token: null }), deps);
    eq('auth precedes routing (unknown route -> 401)', preRoute.status, 401);

    const junkApikey = await handleWhEngineRequest(req('/query', { method: 'POST', apikey: 'zzz-not-validated', body: COUNT_REQ }), deps);
    ok('apikey presence-only at fn level (erratum §4.2 gateway-only reading)', junkApikey.status === 200, `status ${junkApikey.status}`);
  });

  await withEnv('WHE_BEARER_TOKEN', undefined, async () => {
    const { deps } = makeDeps();
    const noSecret = await handleWhEngineRequest(req('/query', { method: 'POST', body: COUNT_REQ }), deps);
    eq('server without WHE_BEARER_TOKEN -> 500 internal', noSecret.status, 500);
    eq('no-secret shape', (await noSecret.json()).error.code, 'internal');
  });
}

// -----------------------------------------------------------------------------
// OPTIONS + /health (P2-1)
// -----------------------------------------------------------------------------
async function optionsHealthPins(): Promise<void> {
  console.log('options + /health');
  await withEnv('WHE_BEARER_TOKEN', TOKEN, async () => {
    const { deps } = makeDeps();
    const pre = await handleWhEngineRequest(req('/query', { method: 'OPTIONS', token: null }), deps);
    eq('OPTIONS -> 204 without auth', pre.status, 204);
    eq('CORS allow-headers', pre.headers.get('access-control-allow-headers'), 'Authorization, Content-Type, apikey');

    // P2-1: /health is a config-probe ONLY — readDirectory must never run.
    const { deps: hDeps, counters } = makeDeps();
    const h = await handleWhEngineRequest(req('/health', { token: null }), hDeps);
    eq('/health unauthenticated 200', h.status, 200);
    // r124 A8 (⟫B-3): the L2 shape RE-PINNED for the additive engine_build
    // field — the dep ABSENT (the default makeDeps shape) renders null. The
    // stamp can never 500 the probe (a static-import constant cannot throw).
    eq('/health L2 shape (re-pinned r124: additive engine_build, absent => null)', await h.json(), { v: 1, ok: true, directory_version: 42, engine_build: null });
    eq('/health did not read the directory (P2-1)', counters.read, 0);
    eq('/health probed config once', counters.probe, 1);

    // r124 A8 present/absent arms (⟫B-3): the stamp rides ONLY when threaded
    // (in prod: `make stamp` wrote a non-empty sha7 and the shell spreads it).
    const { deps: stampDeps } = makeDeps({ engineBuild: 'abc1234' });
    const hs = await handleWhEngineRequest(req('/health', { token: null }), stampDeps);
    eq('/health stamp PRESENT: the sha7 rides additively', (await hs.json()).engine_build, 'abc1234');
    eq('/health stamp present: status still 200 unauthenticated', hs.status, 200);

    const { deps: fDeps } = makeDeps({ probeThrows: new Error('config probe failed') });
    const hf = await handleWhEngineRequest(req('/health', { token: null }), fDeps);
    eq('/health probe failure -> 500 ok:false', hf.status, 500);
    const hfj = await hf.json();
    eq('/health failure shape', [hfj.v, hfj.ok, hfj.error.code, hfj.error.message], [1, false, 'internal', 'config probe failed']);
  });
}

// -----------------------------------------------------------------------------
// /query malformed path (P0-1 pin — parseWhEngineRequest IS called)
// -----------------------------------------------------------------------------
async function queryMalformedPins(): Promise<void> {
  console.log('/query malformed (§4.3 parse gate, P0-1)');
  await withEnv('WHE_BEARER_TOKEN', TOKEN, async () => {
    const base = () => makeDeps({ hasRealFetcher: true });

    const badJson = await handleWhEngineRequest(req('/query', { method: 'POST', rawBody: '{not json' }), base().deps);
    eq('invalid JSON -> 400', badJson.status, 400);
    const bj = await badJson.json();
    eq('invalid JSON shape (qid null, no auth_kind)', [bj.v, bj.qid, bj.error.code, bj.error.auth_kind], [1, null, 'malformed', undefined]);

    const empty = await handleWhEngineRequest(req('/query', { method: 'POST', body: {} }), base().deps);
    eq('body {} -> 400 malformed (P0-1: parse runs)', empty.status, 400);
    eq('{} message names the v gate', (await empty.json()).error.message.includes('v'), true);

    const v2 = await handleWhEngineRequest(req('/query', { method: 'POST', body: { ...COUNT_REQ, v: 2 } }), base().deps);
    eq('v:2 -> 400 (never best-effort parse)', v2.status, 400);
    ok('v:2 message pins the law', (await v2.json()).error.message.includes('best-effort'), 'message');

    const having = await handleWhEngineRequest(req('/query', { method: 'POST', body: { ...COUNT_REQ, query: { select: [{ op: 'count' }], having: {} } } }), base().deps);
    eq('having -> 400 planner-honesty reject', having.status, 400);
    ok('having reject names the clause', (await having.json()).error.message.includes('HAVING'), 'message');

    const badTable = await handleWhEngineRequest(req('/query', { method: 'POST', body: { ...COUNT_REQ, table: 'orders; drop' } }), base().deps);
    eq('non-identifier table -> 400 IDENT_RE', badTable.status, 400);

    const badSnap = await handleWhEngineRequest(req('/query', { method: 'POST', body: { ...COUNT_REQ, directory_snapshot: 5 } }), base().deps);
    eq('non-string directory_snapshot -> 400', badSnap.status, 400);

    const badTypes = await handleWhEngineRequest(req('/query', { method: 'POST', body: { ...COUNT_REQ, column_types: 'not an object' } }), base().deps);
    eq('column_types non-object -> 400 (erratum v1 extension validation)', badTypes.status, 400);
  });
}

// -----------------------------------------------------------------------------
// /query P2-6 deploy gate + success path + P0-3 projection
// -----------------------------------------------------------------------------
async function queryPathPins(): Promise<void> {
  console.log('/query gate + success + projection');
  await withEnv('WHE_BEARER_TOKEN', TOKEN, async () => {
    const gated = makeDeps({ hasRealFetcher: false });
    const g = await handleWhEngineRequest(req('/query', { method: 'POST', body: COUNT_REQ }), gated.deps);
    eq('hasRealFetcher=false -> 500 (P2-6)', g.status, 500);
    ok('gate message names the probe freeze', (await g.json()).error.message.includes('PAT-gated design freeze'), 'message');
    eq('gate fires before directory work', [gated.counters.probe, gated.counters.read], [0, 0]);

    const seen: string[] = [];
    const okRun = makeDeps({ fetcher: okFetch((s) => scalarEnv(s, 'orders'), seen) });
    const good = await handleWhEngineRequest(req('/query', { method: 'POST', body: COUNT_REQ }), okRun.deps);
    eq('valid count query -> 200', good.status, 200);
    const gj = await good.json();
    eq('200 envelope core fields', [gj.v, gj.qid, gj.directory_version, gj.coverage, gj.partial, gj.result['c']], [1, 'q-1', 42, '2/2', false, 10]);
    eq('perShard present and healthy', gj.perShard.map((p: { shard: string; ok: boolean }) => [p.shard, p.ok]), [['shard-a', true], ['shard-b', true]]);
    eq('fetcher saw mapped shards (P0-2: project_ref->shard)', seen, ['shard-a', 'shard-b']);

    // r44 §5.2 plumbing pin — RE-WRITTEN r121 OPT-1b (the fold): the
    // per-query inventory sweep is GONE from the critical path. Two folded
    // arms:
    //   (a) the DEFAULT hashless plan (table orders derives no templates):
    //       the EMPTY-HASH guard means ZERO inventory reads at ANY qid —
    //       the vacuous select-path audit is retired (design §1.6) — and
    //       the fan-out proceeds unchanged (200, normal merge).
    //   (b) a TEMPLATED plan (wh_probe_agg scalar min/count(col) derives
    //       {W2}) with an IN-BUCKET qid + limit: the SAMPLED backstop fires,
    //       reads the inventory ONCE per candidate shard BEFORE any
    //       wh_query call, and an eligible verdict lets fan-out proceed.
    const hsSeen: string[] = [];
    const hsDeps = makeDeps({
      fetcher: okFetch((s) => scalarEnv(s, 'orders'), hsSeen),
      handshake: {
        readTemplateInventory: async (shard: string): Promise<TemplateInventoryRow[]> => {
          hsSeen.push(`inv:${shard}`);
          return [];
        },
      },
    });
    const hsRes = await handleWhEngineRequest(req('/query', { method: 'POST', body: COUNT_REQ }), hsDeps.deps);
    eq('handshake-wired hashless query still 200 (normal merge)', hsRes.status, 200);
    const hj = await hsRes.json();
    eq('folded default: ZERO inventory reads on the hashless plan (empty-hash guard — the per-query sweep is retired)', hsSeen, ['shard-a', 'shard-b']);
    eq('folded default: no warnings, coverage 2/2 complete', [hj.warnings, hj.coverage, hj.partial], [[], '2/2', false]);

    const hsSeen2: string[] = [];
    const w2Hash = 'a095adaa148253aee8d1cc8e976f01b3579beeea5082f3862df4a908c20b2659'; // W2 (manifest schema_version 1)
    const hsDeps2 = makeDeps({
      rows: [dirRow('shard-a', 'wh_probe_agg', { schemaVersion: 1 }), dirRow('shard-b', 'wh_probe_agg', { schemaVersion: 1 })],
      fetcher: okFetch((s) => ({
        v: 1,
        shard: s,
        table: 'wh_probe_agg',
        schema_version: 1,
        partial: {
          kind: 'scalar',
          aggs: { m: { op: 'min', col: 'amount' }, c: { op: 'count', col: 'amount' } },
          rows: [{ k: [], a: { m: '5', c: 2 } }],
          rowCount: 1,
          more: false,
        },
      } as unknown as WhPartialEnvelope), hsSeen2),
      handshake: {
        readTemplateInventory: async (shard: string): Promise<TemplateInventoryRow[]> => {
          hsSeen2.push(`inv:${shard}`);
          return [{ template_hash: w2Hash, qc_class: 'QC2', logical_table: 'wh_probe_agg', schema_version: 1, state: 'active', max_rows: 1000 }];
        },
      },
    });
    const SAMPLED_TEMPLATED_REQ = {
      v: 1,
      qid: 'skip-me', // sha256('skip-me')[0] = 0x97 → &15 = 7 = K_SAMPLING (hand-computed leg-1 vector)
      table: 'wh_probe_agg',
      column_types: { amount: 'numeric' }, // the client-declared type channel (erratum §4.3) — the agg needs it
      column_scales: { amount: 0 },
      query: { select: [{ op: 'min', col: 'amount', alias: 'm' }, { op: 'count', col: 'amount', alias: 'c' }], limit: 10 },
    };
    const hsRes2 = await handleWhEngineRequest(req('/query', { method: 'POST', body: SAMPLED_TEMPLATED_REQ }), hsDeps2.deps);
    eq('sampled templated query still 200 (eligible verdict, normal merge)', hsRes2.status, 200);
    const hj2 = await hsRes2.json();
    eq('SAMPLED: inventory once per candidate shard, BEFORE any wh_query call (folded order pin, in-bucket qid)', hsSeen2, ['inv:shard-a', 'inv:shard-b', 'shard-a', 'shard-b']);
    eq('SAMPLED: exactly one inventory read per shard (no duplicates)', hsSeen2.filter((s: string) => s.startsWith('inv:')).length, 2);
    eq('SAMPLED: coverage 2/2, no warnings', [hj2.coverage, hj2.warnings], ['2/2', []]);
    eq('SAMPLED: hand-computed scalar merge through the wire (min bigint serialized "5", count 2+2=4)', hj2.result, { m: '5', c: 4 });

    // P0-3: the per-table projection — a query for orders must never fan out
    // over events-only shards (and vice versa), even though readDirectory
    // returns the full embed.
    const evSeen: string[] = [];
    const mixedDeps = makeDeps({ rows: [...ORDERS_ROWS, ...EVENTS_ROWS], fetcher: okFetch((s) => scalarEnv(s, 'events'), evSeen) });
    const ev = await handleWhEngineRequest(req('/query', { method: 'POST', body: { ...COUNT_REQ, table: 'events' } }), mixedDeps.deps);
    eq('events query -> 200', ev.status, 200);
    eq('fan-out touched ONLY events shards (P0-3)', evSeen, ['shard-c']);

    const warm = makeDeps({ rows: EVENTS_ROWS, fetcher: okFetch((s) => scalarEnv(s, 'orders')) });
    const tw = await handleWhEngineRequest(req('/query', { method: 'POST', body: COUNT_REQ }), warm.deps);
    eq('table with zero serving placements -> 404 tier_warm (P0-3)', tw.status, 404);
    const twj = await tw.json();
    eq('tier_warm error body carries directory_version (P2-3/§4.6)', [twj.error.code, twj.directory_version], ['tier_warm', 42]);

    const ff = makeDeps({ fetcher: failFetch({ code: 'network' }) });
    const ffr = await handleWhEngineRequest(req('/query', { method: 'POST', body: { ...COUNT_REQ, coverage_mode: 'fail_fast' } }), ff.deps);
    eq('fail_fast with failing shards -> 500', ffr.status, 500);
    const ffj = await ffr.json();
    eq('fail_fast payload (P2-3: perShard + latency + directory_version)', [ffj.error.code, ffj.perShard.length, typeof ffj.latency_ms, ffj.directory_version], ['internal', 2, 'number', 42]);

    const boom = makeDeps({ readThrows: new Error('boom password=hunter2') });
    const b = await handleWhEngineRequest(req('/query', { method: 'POST', body: COUNT_REQ }), boom.deps);
    eq('generic internal error -> 500', b.status, 500);
    const bj2 = await b.json();
    eq('generic 500 scrubs raw text (P3-4)', bj2.error.message, 'internal engine error');
    eq('generic 500 keeps qid (P3-4)', bj2.qid, 'q-1');
    ok('generic 500 leaks no raw detail', !JSON.stringify(bj2).includes('hunter2'), 'leak found');

    const { deps: nrDeps } = makeDeps();
    const nr = await handleWhEngineRequest(req('/bogus'), nrDeps);
    eq('no route -> 400 with route echo', nr.status, 400);
    eq('no-route message', (await nr.json()).error.message, 'no route GET /bogus');
  });
}

// -----------------------------------------------------------------------------
// wh_directory_reader
// -----------------------------------------------------------------------------
async function readerPins(): Promise<void> {
  console.log('wh_directory_reader');
  eq('coerceDirectoryVersion: number passes', coerceDirectoryVersion(42), 42);
  eq('coerceDirectoryVersion: 0 is valid', coerceDirectoryVersion(0), 0);
  for (const bad of [null, false, '', '5', [5], -1, 1.5]) {
    let threw = false;
    try {
      coerceDirectoryVersion(bad);
    } catch {
      threw = true;
    }
    eq(`coerceDirectoryVersion rejects ${JSON.stringify(bad) ?? 'undefined'} (P2-5)`, threw, true);
  }

  // P0-2 mapping: project_ref -> shard; loud on drift.
  const mapped = toWhDirectoryRow({
    project_ref: 'abc123', key_min: '1', key_max: '9', hash_slot: 7, state: 'serving',
    platform_status: 'ACTIVE_HEALTHY', schema_version: 3, last_health_at: '2026-09-28T00:00:00Z',
    logical_name: 'orders', shard_key_type: 'hash', shard_key_column: 'id', table_schema_version: 3,
  });
  eq('toWhDirectoryRow maps project_ref -> shard (P0-2)', mapped.shard, 'abc123');
  eq('toWhDirectoryRow passthrough', [mapped.logical_name, mapped.shard_key_type, mapped.shard_key_column, mapped.table_schema_version], ['orders', 'hash', 'id', 3]);
  for (const [name, row] of [
    ['missing project_ref', { schema_version: 3 }],
    ['string schema_version', { project_ref: 'x', schema_version: '3' }],
    ['fractional hash_slot', { project_ref: 'x', schema_version: 3, hash_slot: 1.5 }],
  ] as [string, unknown][]) {
    let threw = false;
    try {
      toWhDirectoryRow(row);
    } catch {
      threw = true;
    }
    eq(`toWhDirectoryRow loud on ${name}`, threw, true);
  }

  // r120 OPT-1 harness: the fresh-path reader = the module factory over ONE
  // atomic fetch edge (index.ts wires it to client.rpc('wh_directory_atomic_read')).
  // The atomic results are consumed in sequence (one entry per expected rpc
  // POST); fetchVersion keeps the old fake style — readDirectory must NEVER
  // call it (the standalone probe is replay/health-only since P2-4 retired).
  function atomicReader(atomicResults: { data: unknown; error?: { message: string } }[]) {
    const calls: unknown[] = [];
    const probeCalls: number[] = [];
    const reader = makeDirectoryReader({
      fetchVersion: async () => {
        probeCalls.push(probeCalls.length);
        return { value: 42, error: null };
      },
      fetchAtomic: async () => {
        calls.push(calls.length);
        const r = atomicResults[Math.min(calls.length - 1, atomicResults.length - 1)];
        return { data: r.data, error: r.error ?? null };
      },
    });
    return { reader, calls, probeCalls };
  }

  const rawRow = { project_ref: 's1', state: 'serving', platform_status: 'ACTIVE_HEALTHY', schema_version: 3, last_health_at: 't' };

  // (a) success: ONE rpc POST -> version coerced + rows mapped through
  // toWhDirectoryRow (P0-2: project_ref -> shard).
  const happy = atomicReader([{ data: { version: 42, rows: [rawRow] }, error: null }]);
  const okRes = await happy.reader.readDirectory();
  eq('atomic read: version stamped from the ONE payload', okRes.version, 42);
  eq('atomic read: rows map project_ref -> shard (P0-2)', okRes.rows[0].shard, 's1');
  eq('atomic read: row passthrough fields', [okRes.rows[0].state, okRes.rows[0].platform_status, okRes.rows[0].schema_version, okRes.rows[0].last_health_at], ['serving', 'ACTIVE_HEALTHY', 3, 't']);
  eq('atomic read: exactly ONE rpc POST per readDirectory (OPT-1)', happy.calls.length, 1);
  eq('atomic read: the version probe is NOT part of the read (P2-4 retired)', happy.probeCalls.length, 0);

  // (b) rpc error arm: the PostgREST failure text is wrapped in the loud
  // internal message, never relayed raw.
  const rpcErr = atomicReader([{ data: null, error: { message: 'permission denied' } }]);
  let rpcErrCaught: unknown;
  try {
    await rpcErr.reader.readDirectory();
  } catch (e) {
    rpcErrCaught = e;
  }
  ok('rpc error -> loud internal', (rpcErrCaught as Error).message.includes('atomic directory read failed: permission denied'), (rpcErrCaught as Error)?.message);

  // (h) F3 law: the reader throws PLAIN Errors ONLY — never WhEngineError.
  // (The entrypoint relays WhEngineError messages raw to the wire; plain
  // Errors hit the P3-4 scrub. This battery never imports WhEngineError, so
  // the pin is the constructor identity itself.)
  eq('F3: the atomic reader throws a PLAIN Error (never WhEngineError)', [rpcErrCaught instanceof Error, (rpcErrCaught as Error).constructor.name], [true, 'Error']);

  // (c) non-object payload arm: the rpc body must be the {version, rows}
  // object — a drifted/garbage payload fails loudly.
  for (const bad of ['garbage', null]) {
    let threw = '';
    try {
      await atomicReader([{ data: bad, error: null }]).reader.readDirectory();
    } catch (e) {
      threw = (e as Error).message;
    }
    ok(`non-object payload (${JSON.stringify(bad)}) -> loud internal`, threw.includes('non-object payload'), threw);
  }

  // (d) F7: a missing config row surfaces as jsonb null version => the EXACT
  // loud message (never coerced to 0 — the P2-5 law's missing-row face).
  const missing = atomicReader([{ data: { version: null, rows: [] }, error: null }]);
  let missingThrew = '';
  try {
    await missing.reader.readDirectory();
  } catch (e) {
    missingThrew = (e as Error).message;
  }
  eq('config row missing -> the exact F7 message', missingThrew, 'directory version probe failed: config row missing');

  // (e) F1: the version arrives as the RAW jsonb scalar and is judged by the
  // SAME P2-5 law as the standalone probe (a string '7' is not a number —
  // never Number()-coerced into a plausible version).
  const strVersion = atomicReader([{ data: { version: '7', rows: [] }, error: null }]);
  let strVersionThrew = false;
  try {
    await strVersion.reader.readDirectory();
  } catch {
    strVersionThrew = true;
  }
  eq("non-integer version ('7') -> the P2-5 coerce law rejects", strVersionThrew, true);

  // (f) rows must be an array — a drifted RPC shape fails loudly.
  const badRows = atomicReader([{ data: { version: 1, rows: 'x' }, error: null }]);
  let badRowsThrew = '';
  try {
    await badRows.reader.readDirectory();
  } catch (e) {
    badRowsThrew = (e as Error).message;
  }
  ok("non-array rows ('x') -> loud internal", badRowsThrew.includes('non-array rows'), badRowsThrew);

  // (g) F2: the loud fleet-shape bound — the retired truncation guard's
  // successor. 1001 rows refuse; exactly 1000 rows (the bound is >, not >=)
  // resolve. Never silently truncated, never a silent LIMIT inside the RPC.
  const overBound = atomicReader([{ data: { version: 42, rows: Array.from({ length: 1001 }, () => rawRow) }, error: null }]);
  let overThrew = '';
  try {
    await overBound.reader.readDirectory();
  } catch (e) {
    overThrew = (e as Error).message;
  }
  ok('1001 rows -> fleet-shape law breakage (F2 bound)', overThrew.includes('fleet-shape law breakage'), overThrew);

  const atBound = atomicReader([{ data: { version: 42, rows: Array.from({ length: 1000 }, () => rawRow) }, error: null }]);
  const atRes = await atBound.reader.readDirectory();
  eq('exactly 1000 rows resolve (the bound is >, not >=)', [atRes.version, atRes.rows.length], [42, 1000]);
}

// -----------------------------------------------------------------------------
// Static pins — deploy-gate wiring of the shipped shell (§4.1)
// -----------------------------------------------------------------------------
function staticPins(): void {
  console.log('static pins (shell wiring only — fleet-api discovery stays in fleet-manager)');
  ok('index.ts keeps the deploy gate (hasRealFetcher rides the FLIP constant)',
    indexSrc.includes('hasRealFetcher: FLIP_hasRealFetcher') && FLIP_hasRealFetcher === true,
    'gate line absent or the constant was reverted to the pre-r118 stub state');
  // r69 (design_r69_shard_channel.md §5 statics): the probe-gated stub is
  // DELETED and replaced by the RPC real fetcher over the shard key channel —
  // the old source-text stub-message pin flips to the DELETION pin. The gate
  // constant is FLIPPED TRUE r118 (design_r118_realfetcher_flip.md §3b — all
  // flip conditions met; pin above discriminates a revert to false). The
  // entrypoint's own gate message is pinned by geo_write_fence_test.ts:702
  // and is UNTOUCHED (still reachable via the deps.hasRealFetcher=false
  // override path).
  // r123 AMENDMENT: the fetcher dep's rawFetch is now the CONDITIONALLY
  // spread WH_PROXY lever dep (design_r122_acct2_proxy.md §1.4) — the
  // single-line wiring grew the spread, so the r69 exact-string pin splits
  // into the factory call + the resolveKey dep. The lever's own wiring pin
  // follows below.
  ok('index.ts stub is deleted — the r69 RPC real fetcher over the shard channel is wired',
    !indexSrc.includes('makeRealFetcher') &&
    !indexSrc.includes('real shard fetcher lands after live probes') &&
    indexSrc.includes('makeRpcShardFetcher({') &&
    indexSrc.includes('resolveKey: resolveShardKey,') &&
    indexSrc.includes('parseShardKeyEnv(Deno.env.get(\'WH_SHARD_KEYS\'))') &&
    indexSrc.includes('rpcMode: Deno.env.get(\'WH_REAL_FETCHER\') === \'on\''),
    'stub still present or the r69 channel wiring is missing');
  // r123 WH_PROXY lever (design_r122_acct2_proxy.md §1.3/§1.4): the rawFetch
  // dep is CONDITIONALLY spread — with the lever inert (WH_PROXY_FETCHER not
  // exactly 'on' / wh_shard_proxy_map KV absent-malformed / WH_PROXY_TOKEN
  // unset) the spread adds NOTHING and the fetcher dep is the default
  // platform fetch: the byte-identical unset path (mirror of the rpcMode
  // expression's unset law). The KV read is the DIRECT db() chain — NEVER
  // getConfig() (drops unknown keys) — boot-once in the shell IIFE (⟫B4),
  // TIMEOUT-RACED (the r123 fresh-eyes P3), shape re-validated engine-side
  // via parseWhProxyMapValue (defense in depth — the FM 0019/0027 validator
  // mirror), token from the DEDICATED
  // WH_PROXY_TOKEN (never WHE_BEARER_TOKEN — the snapshotKey doctrine).
  ok('index.ts r123 WH_PROXY lever: conditional rawFetch wiring + boot-once DIRECT KV read (byte-identical default fetch when inert)',
    indexSrc.includes('...(proxyRawFetch !== undefined ? { rawFetch: proxyRawFetch } : {})') &&
    indexSrc.includes("Deno.env.get('WH_PROXY_FETCHER') === 'on'") &&
    indexSrc.includes("Deno.env.get('WH_PROXY_TOKEN')") &&
    indexSrc.includes(".eq('key', 'wh_shard_proxy_map')") &&
    indexSrc.includes('.maybeSingle()') &&
    indexSrc.includes('parseWhProxyMapValue(') &&
    indexSrc.includes('makeProxiedRawFetch(') &&
    !indexSrc.includes('getConfig()'),
    'lever wiring missing, unconditional, or the KV read is not the direct db() chain');
  // r123 P0 fix (the live-e2e RED closure): the lever arms ONLY as the
  // OWN-REF DIRECT carve-out wrapper (makeOwnRefBypassRawFetch) — the
  // engine's own shard keeps the default platform fetch (the co-hosted law:
  // intra-project traffic never rides the cross-account proxy; the v14 e2e
  // drew a relayed 401 "Invalid API key" for the own-ref credential through
  // the acct2 proxy), remote shards ride the proxy — and ONLY with a KNOWN
  // own ref: the SUPABASE_URL subdomain via ownProjectRefFromSupabaseUrl,
  // the SAME authoritative source the shard-key resolver already uses (no
  // new env — the co-hosted law IS the self-ref source). An unparseable
  // SUPABASE_URL keeps the lever INERT (fail-closed — "own never proxies"
  // is unprovable without the own-ref identity). The boot KV read is
  // TIMEOUT-RACED (raceWhProxyKvBoot @ WH_PROXY_KV_BOOT_TIMEOUT_MS — a
  // hanging PostgREST degrades to the KV-absent state instead of wedging
  // module evaluation; the fresh-eyes P3). The ordering pin proves the
  // wrapper is constructed BETWEEN the lever env check and the conditional
  // spread — the bypass exists ONLY when the lever is ON (lever off ⇒
  // proxyRawFetch stays undefined ⇒ the default platform fetch).
  // r131 D2 AMENDMENT: the 8 boot defect branches + the armed console.log
  // collapsed into the ONE merged wh_boot line (wh_bootlog.ts) — the prose
  // literals are gone; the arm re-pins the COLLECTOR/CODE wirings at the
  // same guard positions (1:1 codes, branch order) + the lever:'armed'
  // fold-in at the former :225 site. The ordering conjuncts below are
  // UNTOUCHED (they anchor on the env check / the bypass factory / the
  // conditional spread — none of which moved).
  ok('index.ts r123 P0 own-ref bypass: carve-out wrapper + known-own-ref gate + boot KV timeout-race, all INSIDE the lever block',
    indexSrc.includes('makeOwnRefBypassRawFetch({') &&
    indexSrc.includes('proxiedRawFetch: makeProxiedRawFetch({ proxyUrl: proxyMap.url, proxyToken }),') &&
    indexSrc.includes("if (proxyMap !== null && ownRef === '')") &&
    indexSrc.includes("bootDefects.push('proxy_ownref_unparsed');") &&
    indexSrc.includes("bootLever = 'armed';") &&
    indexSrc.includes('raceWhProxyKvBoot(') &&
    indexSrc.includes('WH_PROXY_KV_BOOT_TIMEOUT_MS') &&
    indexSrc.includes("bootDefects.push('proxy_kv_timeout');") &&
    indexSrc.indexOf("Deno.env.get('WH_PROXY_FETCHER') === 'on'") < indexSrc.indexOf('makeOwnRefBypassRawFetch({') &&
    indexSrc.indexOf('makeOwnRefBypassRawFetch({') < indexSrc.indexOf('...(proxyRawFetch !== undefined ? { rawFetch: proxyRawFetch } : {})'),
    'own-ref bypass wiring missing, ungated, or outside the lever block');
  ok('index.ts wires the extracted handler', indexSrc.includes('handleWhEngineRequest(req, deps)'), 'wiring');
  ok('index.ts uses the shared directory reader', indexSrc.includes('makeDirectoryReader'), 'reader');
  // r120 OPT-1: the reader's fetchAtomic edge is the ONE atomic rpc POST —
  // the fresh-path pre-fanout chain collapses 4 serial RTs -> 2 (atomic + fence).
  ok("index.ts wires the atomic directory rpc (client.rpc('wh_directory_atomic_read'))",
    indexSrc.includes("client.rpc('wh_directory_atomic_read')"),
    'atomic rpc wiring absent (the r40 paginated-page wiring must stay retired)');

  // NOTE (standalone split): the former §4.1 discovery-route pins (pinned
  // route path, path'd URL shape, NO_ENGINE_REF 503 guard, directory_version
  // field, P2-5 typeof guard on the version probe) byte-pinned fleet-manager's
  // fleet-api function, which does NOT ship in this standalone repo. The
  // discovery route stays in fleet-manager — consumers use the FM kit for
  // discovery; the engine pins only its own shell wiring above.
}

// -----------------------------------------------------------------------------
// r57 wh_ryw v1 — the flip lever (WH_RYW_V1, default OFF) + the m5
// dependency-graph cell (design_r53_wh_ryw_lsn_poll.md §8; PLAN r58 secondary
// "G6 stamped compare; flip stays gated")
// -----------------------------------------------------------------------------
const RYW_REQ = { ...COUNT_REQ, read_plane: 'replica', min_lsn: '0/FFFFFFFE' };

function geoRowServing(appliedLsn: string | null, overrides: Partial<WhGeoDirectoryRow> = {}): WhGeoDirectoryRow {
  return {
    project_ref: 'fmhostref',
    region: 'us-east-1',
    subscription: 'geo_sub',
    state: 'serving',
    applied_lsn: appliedLsn,
    replay_ts: '2026-09-29T00:00:00Z',
    lag_bytes: null,
    coverage: ['orders'],
    last_health_at: '2026-09-29T00:00:00.000000Z',
    updated_at: '2026-09-29T00:00:00.000000Z',
    ...overrides,
  };
}

/** m5 (design §8 mutant class "dbrelay-in-engine"; dbrelay §11 D4): the
 *  engine's ENTIRE import graph — warehouse-engine/index.ts plus every
 *  transitive relative-import module — references NOTHING from the relay
 *  path. The relay stays the SANDBOX-side transport (C2/C5); relay
 *  credentials or transport code inside the engine would be the secret-
 *  surface violation the design rejects permanently (§3-b(3)). Offline walk
 *  (fs reads, --allow-read), bounded by a visited set + the functions dir.
 *
 *  STATIC-ONLY WALK (future-change tripwire): the specifier regexes below
 *  match STATIC imports exclusively (`from '…'` / `import '…'`) — a dynamic
 *  `import(…)` would EVADE the walk entirely (its target never enters the
 *  visited set, so a relay module reachable only dynamically would pass the
 *  first pin while being imported). The second pin therefore scans every
 *  walked source for /import\s*\(/ and FAILS on any dynamic-import site:
 *  zero exist today (r57, all 12 walked modules); if one ever appears —
 *  dbrelay-named or not — this cell goes RED and the walk must be re-
 *  reviewed before the graph claim is trusted again. */
function m5DependencyGraphPin(): void {
  console.log('m5 dependency-graph cell');
  const root = new URL('../warehouse-engine/index.ts', import.meta.url);
  const seen = new Set<string>([root.href]);
  const stack: URL[] = [root];
  let files = 0;
  const offenders: string[] = [];
  const REs = [/\bfrom\s+['"]([^'"]+)['"]/g, /\bimport\s+['"]([^'"]+)['"]/g];
  // dynamic-import tripwire: see the STATIC-ONLY WALK note above
  const dynImportSites: string[] = [];
  while (stack.length > 0) {
    const url = stack.pop()!;
    let src: string;
    try {
      src = Deno.readTextFileSync(url);
    } catch (e) {
      failed++;
      failures.push(`m5: could not read ${url.href}: ${(e as Error).message}`);
      continue;
    }
    files++;
    if (/dbrelay/i.test(src)) offenders.push(url.href);
    if (/import\s*\(/.test(src)) dynImportSites.push(url.href);
    for (const re of REs) {
      re.lastIndex = 0;
      let m: RegExpExecArray | null;
      while ((m = re.exec(src)) !== null) {
        const spec = m[1];
        if (!spec.startsWith('.')) continue; // bare/npm/node specifiers: out of graph
        const resolved = new URL(spec, url);
        if (!resolved.href.includes('/supabase/functions/')) continue; // graph bound
        if (!resolved.href.endsWith('.ts')) continue;
        if (seen.has(resolved.href)) continue;
        seen.add(resolved.href);
        stack.push(resolved);
      }
    }
  }
  ok(
    `m5: engine import graph (${files} modules walked) imports NOTHING from the relay path`,
    offenders.length === 0 && files > 5,
    `offenders: ${offenders.join(', ') || 'none'}`,
  );
  ok(
    `m5: walk covers STATIC import specifiers only — 0 dynamic import(…) sites in the walked graph`,
    dynImportSites.length === 0,
    `dynamic-import sites (their edges EVADE the static walk above): ${dynImportSites.join(', ') || 'none'}`,
  );
}

async function rywFlipPins(): Promise<void> {
  console.log('r57 wh_ryw v1 flip lever (WH_RYW_V1, default OFF)');
  await withEnv('WHE_BEARER_TOKEN', TOKEN, async () => {
    // ---- the env parser: ONE activation value, strict (frozen-contract law:
    // never best-effort parse an activation flag) ----
    eq('env absent => OFF', rywGateEnabledFromEnv(undefined), false);
    for (const off of ['', 'off', 'OFF', 'ON', 'On', ' on', 'on ', '1', 'true', 'on\n']) {
      eq(`env ${JSON.stringify(off)} => OFF (strict 'on' only)`, rywGateEnabledFromEnv(off), false);
    }
    eq("env 'on' => ON", rywGateEnabledFromEnv('on'), true);

    // ---- gate OFF (the deploy default: deps.rywGateEnabled ABSENT) ----
    // A pinned min_lsn is a 400 naming the lever — planner-honest fail-closed
    // (the r47 parse-gate precedent: an enforced-looking no-op floor is a
    // reject, never a silent ignore and never a fallback masking it).
    const { deps, counters } = makeDeps({ geoMode: 'spread', geoRows: [geoRowServing('0/FFFFFFFE')] });
    const r1 = await handleWhEngineRequest(req('/query', { method: 'POST', body: RYW_REQ }), deps);
    eq('gate OFF + min_lsn => 400', r1.status, 400);
    eq('gate OFF 400 shape (message names the lever)', await r1.json(), {
      v: 1,
      qid: 'q-1',
      error: {
        code: 'malformed',
        message:
          'min_lsn requires the read-your-writes gate (v1) enabled on this deployment (WH_RYW_V1=on) — the flip lever is deploy-time and currently OFF',
      },
    });
    eq('gate OFF 400 fires BEFORE any directory/geo work (fail fast)', { probe: counters.probe, read: counters.read }, { probe: 0, read: 0 });

    // explicit false == absent (both spellings of the default-off law)
    const { deps: dFalse } = makeDeps({ geoMode: 'spread', geoRows: [geoRowServing('0/FFFFFFFE')], rywGateEnabled: false });
    const rF = await handleWhEngineRequest(req('/query', { method: 'POST', body: RYW_REQ }), dFalse);
    eq('gate OFF (explicit false) + min_lsn => 400 too', rF.status, 400);

    // ---- gate ON: the r47 errata behavior EXACTLY (the machinery the lever
    // activates is the as-built G6 ladder — the lever adds nothing to it) ----
    const seen: string[] = [];
    const { deps: dOn } = makeDeps({
      geoMode: 'spread',
      geoRows: [geoRowServing('0/FFFFFFFE')],
      rywGateEnabled: true,
      fetcher: okFetch((s) => scalarEnv(s, 'orders'), seen),
    });
    const rOn = await handleWhEngineRequest(req('/query', { method: 'POST', body: RYW_REQ }), dOn);
    eq('gate ON + floor met => 200', rOn.status, 200);
    const jOn = await rOn.json() as Record<string, unknown>;
    eq('gate ON serves the replica (coverage 1/1, single dispatch, zero plane warnings)', {
      coverage: jOn.coverage,
      warnings: jOn.warnings,
      seen,
    }, { coverage: '1/1', warnings: [], seen: ['fmhostref'] });

    // floor BEHIND on a flipped deployment => the r47 fail-closed
    // fall-forward (BigInt refuse — never a stale serve, never a 5xx)
    const { deps: dBehind } = makeDeps({ geoMode: 'spread', geoRows: [geoRowServing('0/A')], rywGateEnabled: true });
    const rB = await handleWhEngineRequest(req('/query', { method: 'POST', body: RYW_REQ }), dBehind);
    const jB = await rB.json() as { warnings: Array<{ detail?: string }> };
    eq('gate ON + floor behind => primary fallback lsn_behind', jB.warnings[0].detail, 'lsn_behind');

    // the flip does NOT weaken the parse gates: garbage min_lsn still 400s
    // from the FORMAT gate (names min_lsn, not the lever)
    const { deps: dGarbage } = makeDeps({ geoMode: 'spread', geoRows: [geoRowServing('0/FFFFFFFE')], rywGateEnabled: true });
    const rG = await handleWhEngineRequest(req('/query', { method: 'POST', body: { ...RYW_REQ, min_lsn: '0/1/0' } }), dGarbage);
    eq('gate ON + garbage min_lsn => 400 from the format gate', rG.status, 400);
    ok('format-gate 400 names min_lsn, not the lever', ((await rG.json() as { error: { message: string } }).error.message).includes('min_lsn'), 'message');

    // ---- the lever gates ONLY the RYW floor ----
    // read_plane:"replica" WITHOUT min_lsn (G6a documented eventual
    // consistency) still serves while OFF — eventual-consistency geo reads
    // are the r47 live surface, not the RYW gate.
    const seenG6a: string[] = [];
    const { deps: dG6a } = makeDeps({ geoMode: 'spread', geoRows: [geoRowServing(null)], fetcher: okFetch((s) => scalarEnv(s, 'orders'), seenG6a) });
    const rG6a = await handleWhEngineRequest(req('/query', { method: 'POST', body: { ...COUNT_REQ, read_plane: 'replica' } }), dG6a);
    const jG6a = await rG6a.json() as Record<string, unknown>;
    eq('gate OFF + replica plane WITHOUT min_lsn (G6a) => still serves', {
      status: rG6a.status,
      coverage: jG6a.coverage,
      seen: seenG6a,
    }, { status: 200, coverage: '1/1', seen: ['fmhostref'] });

    // ---- additive-only law: the DEFAULT path (no read_plane/min_lsn) is
    // key-for-key identical in BOTH lever states — the lever adds NO wire
    // field and removes none ----
    const { deps: dOffDefault } = makeDeps();
    const rOff = await handleWhEngineRequest(req('/query', { method: 'POST', body: COUNT_REQ }), dOffDefault);
    const keysOff = Object.keys((await rOff.json()) as Record<string, unknown>).sort();
    const { deps: dOnDefault } = makeDeps({ rywGateEnabled: true });
    const rOnDefault = await handleWhEngineRequest(req('/query', { method: 'POST', body: COUNT_REQ }), dOnDefault);
    const keysOn = Object.keys((await rOnDefault.json()) as Record<string, unknown>).sort();
    eq('default path key set identical in both lever states (additive-only)', keysOn, keysOff);
    eq('default path key set is the r121 envelope (the fold adds EXACTLY `phases` to the r57 set)', keysOff, [
      'coverage', 'coverage_ratio', 'directory_version', 'latency_ms', 'partial', 'perShard', 'phases', 'qid', 'result', 'v', 'warnings',
    ]);

    // ---- static wiring pins: the lever's single read site ----
    ok('index.ts reads the WH_RYW_V1 lever through the shared parser', indexSrc.includes("rywGateEnabled: rywGateEnabledFromEnv(Deno.env.get('WH_RYW_V1'))"), 'wiring line absent');
    ok('index.ts imports the lever parser (no ad-hoc env parse)', indexSrc.includes('rywGateEnabledFromEnv'), 'import absent');

    // ---- r124 A8 statics: the generated build stamp rides the DI law ----
    // The SHELL imports the constant (a static import cannot throw) and
    // threads it; _shared itself never reads the file — it only sees the
    // value as deps.engineBuild (the hasRealFetcher precedent). The boot
    // defect for an empty constant rides the merged wh_boot line as the
    // 'stamp_absent' collector push (r131 D2) — fixed code, boot-time,
    // echo law (never per-request).
    ok(
      "index.ts A8: the ENGINE_BUILD stamp is imported + threaded as deps.engineBuild (the hasRealFetcher DI precedent — the constant import lives in the shell, never in _shared)",
      indexSrc.includes("import { ENGINE_BUILD } from '../_shared/engine_build.ts';") &&
        indexSrc.includes('...(engineBuild !== \'\' ? { engineBuild } : {}),'),
      'the stamp import or the DI thread is missing',
    );
    ok(
      'index.ts A8: the stamp-absent boot defect exists as the pinned collector push (echo law; boot-time, never per-request)',
      indexSrc.includes("bootDefects.push('stamp_absent');"),
      'the stamp_absent collector push is missing',
    );

    // m5 rides this section (same design §8 batch)
    m5DependencyGraphPin();
  });
}

// -----------------------------------------------------------------------------
// §4.6 snapshot integration (replay / attach / silent fallback)
// -----------------------------------------------------------------------------
async function snapshotIntegrationPins(): Promise<void> {
  console.log('§4.6 snapshot integration');
  const KEY = 'entrypoint-snapshot-key';
  await withEnv('WHE_BEARER_TOKEN', TOKEN, async () => {
    // 1. Full embed (no snapshot in request) + key => 200 with a VERIFIABLE
    //    directory_snapshot attached.
    const { deps, counters } = makeDeps({ snapshotKey: KEY });
    const r1 = await handleWhEngineRequest(req('/query', { method: 'POST', body: COUNT_REQ }), deps);
    eq('full embed 200', r1.status, 200);
    const j1 = await r1.json();
    ok('full embed attaches directory_snapshot', typeof j1.directory_snapshot === 'string', 'missing');
    const v1 = await verifyDirectorySnapshot(j1.directory_snapshot, { key: KEY, currentVersion: 42 });
    eq('attached snapshot verifies green (sig/TTL/version)', v1.ok, true);
    if (v1.ok) {
      eq('attached snapshot payload = directory rows', (v1.payload as WhDirectoryRow[]).length, ORDERS_ROWS.length);
    }

    // 2. Valid replay => directory read SKIPPED, no re-attach (byte-budget win).
    const replayDeps = makeDeps({ snapshotKey: KEY });
    const r2 = await handleWhEngineRequest(req('/query', { method: 'POST', body: { ...COUNT_REQ, directory_snapshot: j1.directory_snapshot } }), replayDeps.deps);
    eq('valid replay 200', r2.status, 200);
    const j2 = await r2.json();
    eq('valid replay does NOT re-attach', j2.directory_snapshot, undefined);
    eq('valid replay skips the full read', replayDeps.counters.read, 0);
    eq('valid replay probed the version once', replayDeps.counters.probe, 1);
    eq('replay response still carries directory_version', j2.directory_version, 42);

    // 3. Tampered snapshot => silent full embed + FRESH attach (never an error).
    const [seg, sig] = (j1.directory_snapshot as string).split('.');
    const mid = Math.floor(seg.length / 2);
    const tampered = `${seg.slice(0, mid)}${seg[mid] === 'A' ? 'B' : 'A'}${seg.slice(mid + 1)}.${sig}`;
    ok('tampered differs from original', tampered !== j1.directory_snapshot, 'no-op tamper');
    const tamperDeps = makeDeps({ snapshotKey: KEY });
    const r3 = await handleWhEngineRequest(req('/query', { method: 'POST', body: { ...COUNT_REQ, directory_snapshot: tampered } }), tamperDeps.deps);
    eq('tampered replay still 200 (silent full embed, §4.6)', r3.status, 200);
    const j3 = await r3.json();
    ok('tampered replay gets a FRESH snapshot', typeof j3.directory_snapshot === 'string' && j3.directory_snapshot !== tampered, 'missing or echoes tampered');
    eq('tampered replay did the full read', tamperDeps.counters.read, 1);

    // 4. Stale version (snapshot 42 vs current 43) => full embed + fresh attach.
    const staleDeps = makeDeps({ snapshotKey: KEY, version: 43 });
    const staleSnap = await signDirectorySnapshot(42, ORDERS_ROWS, KEY);
    const r4 = await handleWhEngineRequest(req('/query', { method: 'POST', body: { ...COUNT_REQ, directory_snapshot: staleSnap } }), staleDeps.deps);
    const j4 = await r4.json();
    eq('stale version -> 200 full embed', [r4.status, j4.directory_version], [200, 43]);
    ok('stale version -> fresh attach stamped 43', typeof j4.directory_snapshot === 'string', 'missing');

    // 5. Expired TTL => full embed (maxAgeMs default 60s; sign in the past).
    const expiredDeps = makeDeps({ snapshotKey: KEY });
    const old = await signDirectorySnapshot(42, ORDERS_ROWS, KEY, Date.now() - 61_000);
    const r5 = await handleWhEngineRequest(req('/query', { method: 'POST', body: { ...COUNT_REQ, directory_snapshot: old } }), expiredDeps.deps);
    eq('expired snapshot -> 200 full embed (never an error)', r5.status, 200);
    eq('expired snapshot did the full read', expiredDeps.counters.read, 1);

    // 6. Spoofed key => ignored (fail-closed to trust).
    const spoofDeps = makeDeps({ snapshotKey: KEY });
    const spoofed = await signDirectorySnapshot(42, ORDERS_ROWS, 'attacker-key');
    const r6 = await handleWhEngineRequest(req('/query', { method: 'POST', body: { ...COUNT_REQ, directory_snapshot: spoofed } }), spoofDeps.deps);
    eq('spoofed-key snapshot -> 200 full embed', r6.status, 200);
    eq('spoofed snapshot did the full read', spoofDeps.counters.read, 1);

    // 7. No key configured => replay IGNORED, nothing attached (mode disabled).
    const noKeyDeps = makeDeps();
    const r7 = await handleWhEngineRequest(req('/query', { method: 'POST', body: { ...COUNT_REQ, directory_snapshot: j1.directory_snapshot } }), noKeyDeps.deps);
    const j7 = await r7.json();
    eq('mode disabled: 200 full embed', [r7.status, j7.directory_snapshot], [200, undefined]);
    eq('mode disabled: read ran', noKeyDeps.counters.read, 1);

    // 8. Signed garbage payload (engine-bug class): valid sig, non-array
    //    payload => array backstop falls back to full embed.
    const garbageDeps = makeDeps({ snapshotKey: KEY });
    const garbage = await signSnapshotBlob({ version: 42, issued_at: new Date().toISOString(), payload: { not: 'an array' } }, KEY);
    const r8 = await handleWhEngineRequest(req('/query', { method: 'POST', body: { ...COUNT_REQ, directory_snapshot: garbage } }), garbageDeps.deps);
    eq('signed-garbage payload -> 200 full embed (array backstop)', [r8.status, garbageDeps.counters.read], [200, 1]);

    // 9. The deploy gate still precedes snapshot work.
    const gateDeps = makeDeps({ hasRealFetcher: false, snapshotKey: KEY });
    const r9 = await handleWhEngineRequest(req('/query', { method: 'POST', body: { ...COUNT_REQ, directory_snapshot: j1.directory_snapshot } }), gateDeps.deps);
    eq('gate precedes snapshot verify', [r9.status, gateDeps.counters.probe, gateDeps.counters.read], [500, 0, 0]);
  });
}

// -----------------------------------------------------------------------------
// r69 §5 battery — the entrypoint-level D2/AM-3 cells + the channel statics
// the core self-cell batch did not land (battery author; split-cells law).
// -----------------------------------------------------------------------------
async function rpcPlaneBatteryPins(): Promise<void> {
  console.log('r69 battery: entrypoint D2/AM-3 + channel statics');
  await withEnv('WHE_BEARER_TOKEN', TOKEN, async () => {
    // ---- AM-3: the status ladder maps plan_untemplated to 400 EXPLICITLY —
    // never the 500 fallthrough. A hashless plan on a flipped deployment
    // (rpcMode on) dies at D2 pre-fan-out; a 500 here would misread plan
    // honesty as an engine fault (the exact AM-3 rationale).
    const seen: string[] = [];
    const { deps } = makeDeps({
      rpcMode: true,
      fetcher: (async (shard: string) => {
        seen.push(shard);
        return { ok: true as const, envelope: scalarEnv(shard, 'orders'), estRows: 10 };
      }) as WhShardFetcher,
    });
    const r = await handleWhEngineRequest(req('/query', { method: 'POST', body: COUNT_REQ }), deps);
    eq('hashless plan + rpcMode on => 400 (AM-3: the explicit ladder arm, NOT the 500 fallthrough)', r.status, 400);
    const j = await r.json() as { error: { code: string; message: string }; directory_version: number };
    eq('error.code plan_untemplated', j.error.code, 'plan_untemplated');
    ok(
      "message lists the table + the derived count + the template-backed requirement (never client-facing internals)",
      j.error.message.includes("table 'orders'") && j.error.message.includes('derives 0 template hash(es)') &&
        j.error.message.includes('template-backed plans only'),
      j.error.message,
    );
    eq('D2 fired PRE-fan-out (zero fetcher calls)', seen.length, 0);
    eq('the error body still carries directory_version (P2-3)', j.directory_version, 42);

    // ---- AM-2 at the ladder: a where-carrying TEMPLATE plan (derived hashes
    // NON-empty) is the same 400 — the RPC ignores where, so a where-filtered
    // question answered by a full-table aggregate is the silent-wrongness
    // class. The derived set is genuinely non-empty here (wh_probe_agg
    // groupby+sum over schema_version 1 => [W1H]) — only the where rejects.
    const seenW: string[] = [];
    const { deps: dW } = makeDeps({
      rows: [dirRow('shard-a', 'wh_probe_agg', { schemaVersion: 1 })],
      rpcMode: true,
      fetcher: (async (shard: string) => {
        seenW.push(shard);
        return { ok: true as const, envelope: scalarEnv(shard, 'wh_probe_agg'), estRows: 10 };
      }) as WhShardFetcher,
    });
    const whereReq = {
      v: 1,
      qid: 'q-1',
      table: 'wh_probe_agg',
      query: {
        select: [{ op: 'sum', col: 'amount' }],
        groupBy: ['region'],
        where: [{ col: 'region', op: 'eq', value: 'eu' }],
      },
      coverage_mode: 'best_effort',
      column_types: { region: 'text', amount: 'numeric' },
      column_scales: { amount: 2 },
    };
    const rW = await handleWhEngineRequest(req('/query', { method: 'POST', body: whereReq }), dW);
    eq('where-carrying template plan => 400 plan_untemplated (AM-2; derived hashes NON-empty)', rW.status, 400);
    eq('error.code', ((await rW.json()) as { error: { code: string } }).error.code, 'plan_untemplated');
    eq('D2 fired PRE-fan-out again (zero fetcher calls)', seenW.length, 0);

    // ---- §5 statics the core batch did not land ----
    // D6 single source of truth: the SAME resolver identifier feeds BOTH
    // consumers (the handshake plane auth AND the real fetcher).
    // r123 AMENDMENT: the fetcher dep grew the conditional WH_PROXY rawFetch
    // spread (design_r122_acct2_proxy.md §1.4) — the exact single-line wiring
    // string splits into factory-call + resolveKey-dep fragments; the D6
    // intent (the SAME resolver identifier on BOTH consumers) is unchanged.
    ok(
      'index.ts D6: ONE resolver instance feeds BOTH the handshake plane auth and the real fetcher',
      indexSrc.includes('makeRpcShardFetcher({') &&
        indexSrc.includes('resolveKey: resolveShardKey,') &&
        indexSrc.includes('shardServiceKey: resolveShardKey'),
      'the resolver must be wired to both consumers (single source of truth)',
    );
    // AM-8/OQ-7: the boot defect-class log fires ONCE per isolate boot — the
    // log lines live in the boot-time deps IIFE (before Deno.serve), fixed
    // strings only, both defect classes covered.
    // r131 D2 AMENDMENT: the prose literals are gone — the boot-log REGION
    // re-anchors on the collector declaration (index.ts uses the typed
    // WhBootDefectCode[] collector) and now covers the collector pushes +
    // the post-IIFE emit block (strictly stronger than the old prose
    // anchor). Invariant unchanged: strictly BEFORE Deno.serve.
    const defectIdx = indexSrc.indexOf('const bootDefects: WhBootDefectCode[] = [];');
    const serveIdx = indexSrc.indexOf('Deno.serve('); // the CALL, not the header comment's "Deno.serve shell" mention
    ok(
      'index.ts AM-8: the boot defect collector exists and is BOOT-TIME (before Deno.serve — once per isolate, never per-request)',
      defectIdx >= 0 && serveIdx > defectIdx,
      'collector missing or re-placed into a per-request path',
    );
    ok(
      'index.ts AM-8: the boot-log region is interpolation-free (fixed strings — never value fragments)',
      !indexSrc.slice(defectIdx, serveIdx).includes('${'),
      'template interpolation found in the boot-log region',
    );
    ok(
      'index.ts AM-8: BOTH defect classes are collected (ownref_unparsed + ownkey_empty — the merged wh_boot line keeps 1:1 codes)',
      indexSrc.includes("'ownref_unparsed'") && indexSrc.includes("'ownkey_empty'"),
      'a defect class is uncollected',
    );
  });
}

// -----------------------------------------------------------------------------
// r124 OPT-3 — the pre-chain fence-parallelize battery (design_r124_opt3_a8.md
// §1, audit A ⟫A-5). Six offline cells: (a) the parallel-firing proof,
// (b) the sub-span wall invariant, (c) replay dir_ms 0 + consult still fires,
// (d) both-fail ⇒ the DIRECTORY error wins, (e) fence-throw ⇒ 500,
// (f) 400-class ⇒ zero consults + zero rpc POSTs. The ladder-side sub-span
// record (⟫A-5g) lives in my-project/scripts (outside this repo) — out of
// scope for the whe battery by design.
// -----------------------------------------------------------------------------

/** A healthy OPEN fence read — the geo_write_fence_test fixture shape (the
 *  0017/0019-seeded post-bootstrap state; primary_override undefined ⇒ the
 *  placements identity ⇒ no instruction on the wire). */
function fenceRead(over: Partial<WhFenceValues> = {}): WhFenceRead {
  return { ok: true, values: { read_only: false, write_epoch: 1, primary_override: undefined, ...over } };
}

/** Counting step timers (every nowMs call advances 5ms) — the sub-span
 *  invariant arms ride this; NO exact-value pins on the measured numbers
 *  (the noise law: only presence + the structural invariant). */
function opt3StepTimers(): WhEngineTimers {
  let n = 0;
  return {
    nowMs: () => (n += 5),
    startTimeout: (_ms: number) => ({ promise: new Promise<'timeout'>(() => {}), dispose: () => {} }),
  };
}

async function opt3PreChainPins(): Promise<void> {
  console.log('r124 OPT-3: pre-chain fence parallelize');
  await withEnv('WHE_BEARER_TOKEN', TOKEN, async () => {
    // (a) ⟫A-5a PARALLEL-FIRING PROOF: the fence consult is INITIATED while
    // the directory read is still in-flight. Controllable deferred: the dir
    // read resolves ONLY when released below; under the pre-r124 serial
    // shape the consult would fire strictly AFTER the dir read settled, so
    // counters.fence would still be 0 at the tick. The early-start shape
    // (fence unawaited at the try boundary) has it at 1.
    let releaseDir!: () => void;
    const dirGate = new Promise<{ rows: WhDirectoryRow[]; version: number }>((resolve) => {
      releaseDir = () => resolve({ rows: ORDERS_ROWS, version: 42 });
    });
    const { deps, counters } = makeDeps({
      readImpl: () => dirGate,
      fetchFenceConfig: async () => fenceRead(),
      ownProjectRef: 'engineref',
    });
    const pending = handleWhEngineRequest(req('/query', { method: 'POST', body: COUNT_REQ }), deps);
    // One macrotask tick: the handler has then reached the early-start
    // window (consult initiated) and parked on the unresolved dir read.
    await new Promise((r) => setTimeout(r, 0));
    eq('parallel-firing (⟫A-5a): the fence consult is INITIATED while the dir read is in-flight', [counters.fence, counters.read], [1, 1]);
    releaseDir();
    const pa = await pending;
    eq('parallel-firing: the released request still serves 200', pa.status, 200);

    // (b) ⟫A-5b fresh-path phases carry the sub-spans + the WALL invariant:
    // pre_chain_ms >= max(dir_ms, fence_ms) — fence_ms runs from the EARLY
    // start (⟫A-4: the dir overlap is subtracted honestly, never added).
    const { deps: bDeps } = makeDeps({
      fetchFenceConfig: async () => fenceRead(),
      ownProjectRef: 'engineref',
      timers: opt3StepTimers(),
    });
    const pb = await handleWhEngineRequest(req('/query', { method: 'POST', body: COUNT_REQ }), bDeps);
    const bphases = ((await pb.json()) as { phases: Record<string, number> }).phases;
    eq('sub-spans (⟫A-5b): the fresh path serves 200', pb.status, 200);
    eq('sub-spans: the fresh path carries dir_ms', typeof bphases.dir_ms, 'number');
    eq('sub-spans: the fresh path carries fence_ms', typeof bphases.fence_ms, 'number');
    ok(
      'sub-spans: the wall invariant pre_chain_ms >= max(dir_ms, fence_ms)',
      bphases.pre_chain_ms >= Math.max(bphases.dir_ms, bphases.fence_ms),
      JSON.stringify(bphases),
    );
    ok('sub-spans: handshake_ms + fanout_ms still present (additive-only spread)', typeof bphases.handshake_ms === 'number' && typeof bphases.fanout_ms === 'number', 'phases keys');

    // (c) ⟫A-5c REPLAY path: the signed snapshot verify chain replaces the
    // directory read — ZERO rpc POSTs, dir_ms 0 (never-ran=0), and the fence
    // consult STILL fires (it started before the verify chain ran).
    const KEY = 'opt3-snapshot-key';
    const snap = await signDirectorySnapshot(42, ORDERS_ROWS, KEY);
    const { deps: cDeps, counters: cCounters } = makeDeps({
      snapshotKey: KEY,
      fetchFenceConfig: async () => fenceRead(),
      ownProjectRef: 'engineref',
    });
    const pc = await handleWhEngineRequest(req('/query', { method: 'POST', body: { ...COUNT_REQ, directory_snapshot: snap } }), cDeps);
    const cphases = ((await pc.json()) as { phases: Record<string, number> }).phases;
    eq('replay (⟫A-5c): the valid snapshot serves 200', pc.status, 200);
    eq('replay: ZERO rpc POSTs (the directory read never runs)', cCounters.read, 0);
    eq('replay: the fence is STILL consulted (the early start precedes the verify chain)', cCounters.fence, 1);
    eq('replay: dir_ms 0 (the never-ran=0 convention)', cphases.dir_ms, 0);
    eq('replay: fence_ms present', typeof cphases.fence_ms, 'number');

    // (d) ⟫A-2/⟫A-5d BOTH-FAIL precedence: the DIRECTORY error WINS. The
    // discriminator: the dir fake throws a WhEngineError (relayed RAW on the
    // wire as a 400) while the fence fake throws a plain Error (which would
    // scrub to the generic 500 'internal engine error' if it ever won the
    // precedence). Seeing the directory's 400 + message proves the order.
    const { deps: dDeps } = makeDeps({
      readThrows: new WhEngineError('malformed', 'both-fail: the directory error wins'),
      fetchFenceConfig: async () => {
        throw new Error('both-fail: the fence error must never surface');
      },
      ownProjectRef: 'engineref',
    });
    const pd = await handleWhEngineRequest(req('/query', { method: 'POST', body: COUNT_REQ }), dDeps);
    const dj = (await pd.json()) as { error: { code: string; message: string } };
    eq('both-fail (⟫A-5d): the DIRECTORY error surfaces (400; a fence win would have scrubbed to the 500)', pd.status, 400);
    eq('both-fail: the exact directory message (the fence rejection never surfaced)', dj.error.message, 'both-fail: the directory error wins');

    // (e) ⟫A-5e FENCE-THROW on the read path ⇒ 500 (previously UNPINNED):
    // the wired prod dep is never-throw; an INJECTED rejection is rethrown
    // (dir ok) and hits the P3-4 scrub. The ok:false degrade — the never-
    // throw contract's other face — stays pinned in geo_write_fence_test.
    const { deps: eDeps } = makeDeps({
      fetchFenceConfig: async () => {
        throw new Error('injected fence read rejection');
      },
      ownProjectRef: 'engineref',
    });
    const pe = await handleWhEngineRequest(req('/query', { method: 'POST', body: COUNT_REQ }), eDeps);
    eq('fence-throw on the read path (⟫A-5e): 500 (the rejection is rethrown when the dir read is ok)', pe.status, 500);
    eq('fence-throw: the P3-4 scrub (no raw rejection text)', (await pe.json()).error, { code: 'internal', message: 'internal engine error' });

    // (f) ⟫A-5f 400-CLASS ⇒ zero consults + zero rpc POSTs (the cost law):
    // BOTH pre-try 400 gates — the §4.3 parse reject and the RYW lever
    // reject — return before the pre-chain try, so nothing consults.
    const { deps: fDeps, counters: fCounters } = makeDeps({
      fetchFenceConfig: async () => fenceRead(),
      ownProjectRef: 'engineref',
    });
    const pf = await handleWhEngineRequest(req('/query', { method: 'POST', body: { ...COUNT_REQ, query: { select: [{ op: 'count' }], having: {} } } }), fDeps);
    eq('400-class (⟫A-5f): the having reject is a 400', pf.status, 400);
    eq('400-class: ZERO fence consults (the parse gates precede the pre-chain try)', fCounters.fence, 0);
    eq('400-class: ZERO rpc POSTs', fCounters.read, 0);

    const { deps: gDeps, counters: gCounters } = makeDeps({
      fetchFenceConfig: async () => fenceRead(),
      ownProjectRef: 'engineref',
    });
    const pg = await handleWhEngineRequest(req('/query', { method: 'POST', body: { ...COUNT_REQ, min_lsn: '0/FFFFFFFE' } }), gDeps);
    eq('400-class: the RYW-gate reject is a 400 (the lever default OFF)', pg.status, 400);
    eq('400-class: the RYW arm also stays consult-free + rpc-free', [gCounters.fence, gCounters.read], [0, 0]);
  });
}

// -----------------------------------------------------------------------------
Deno.test('warehouse-engine smoke pins', async () => {
  await authPins();
  await optionsHealthPins();
  await queryMalformedPins();
  await queryPathPins();
  await readerPins();
  await snapshotIntegrationPins();
  staticPins();
  await rywFlipPins();
  await rpcPlaneBatteryPins();
  await opt3PreChainPins();

  console.log(`\n${passed} passed, ${failed} failed`);
  if (failed > 0) {
    console.error('FAILURES:\n' + failures.map((f) => `  * ${f}`).join('\n'));
    throw new Error(`${failed} smoke pin(s) failed`);
  }
});
