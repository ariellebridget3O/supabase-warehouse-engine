// =============================================================================
// _shared/geo_write_fence_test.ts — r49 B2 wave-1 battery: the ENGINE
// WRITE-PLANE FENCE GATE (G-W1..W5) + the R3 dispatch identity.
// =============================================================================
// Normative source: research/findings_geo_failover_design.md §2 (G-W1..W5 at
// L112-L130, the 503 wire shape block), D28 (L374), R3 (L392), R4 (L393).
//
// Harness: same eq/eqTrue/__report__ conventions as wh_geo_plane_test.ts —
// hand-built fixtures with hand-computed expectations (tests-lethal law);
// the __report__ test makes every assertion failure FATAL (the mutant dance
// proved eq-only counters are bookkeeping-vacuous).
//
// Mutant discipline: every 503 cell REDs under its removed gate — the
// gate-level cells (sections 2-6) die if any G-W branch is loosened, the
// entrypoint cells (section 10) die if the call site is unwired or the 503
// wire shape drifts a byte.
//
// Run: deno test --no-check -q --allow-env --allow-read (the README bar)
// =============================================================================

import {
  executeWhQuery,
  parseWhEngineRequest,
  type WhDirectoryRow,
  type WhEngineRequest,
  type WhEngineTimers,
  type WhShardFetcher,
} from './wh_engine_core.ts';
import {
  FENCE_503_MESSAGE,
  FENCE_CONFIG_KEYS,
  fenceReadOnlyBody,
  fetchFenceConfig,
  geoWriteGate,
  isWritePlan,
  ownProjectRefFromSupabaseUrl,
  resolveReadDispatch,
  resolveWriteDispatch,
  type WhFenceClient,
  type WhFenceQueryResult,
  type WhFenceRead,
  type WhFenceValues,
} from './geo_write_fence.ts';
import { handleWhEngineRequest } from './wh_entrypoint.ts';
import type { WhEngineDeps } from './wh_entrypoint.ts';
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

function eqFalse(name: string, actual: boolean): void {
  eq(name, actual, false);
}

// -----------------------------------------------------------------------------
// Fixtures — fence reads
// -----------------------------------------------------------------------------

const OWN = 'engineref';
const OTHER = 'otherref';

/** A healthy OPEN fence is { read_only: false, write_epoch: 1 } — the 0017/
 *  0019-seeded post-bootstrap state (epoch bumped once per promotion). */
function fence(over: Partial<WhFenceValues> = {}): WhFenceRead {
  return {
    ok: true,
    values: { read_only: false, write_epoch: 1, primary_override: undefined, ...over },
  };
}

function gate(values: Partial<WhFenceValues>, carry?: unknown) {
  return geoWriteGate(fence(values), carry === undefined ? {} : { write_epoch: carry });
}

interface FenceCallLog {
  from: string[];
  select: string[];
  in: [string, string[]][];
  awaited: number;
}

/** Recording fake supabase client: pins the EXACT builder chain (one
 *  .from('config').select('key,value').in('key', KEYS) — ONE round trip). */
function fenceClient(
  rows: { key: string; value: unknown }[] | null,
  opts: { error?: { message: string } | null; throwErr?: Error; log?: FenceCallLog } = {},
): WhFenceClient {
  const log = opts.log;
  const result = { data: rows, error: opts.error ?? null };
  const thenable: PromiseLike<WhFenceQueryResult> = {
    then<TResult1 = WhFenceQueryResult, TResult2 = never>(
      onFul?: ((value: WhFenceQueryResult) => TResult1 | PromiseLike<TResult1>) | null,
      onRej?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null,
    ): PromiseLike<TResult1 | TResult2> {
      if (log !== undefined) log.awaited++;
      const p = opts.throwErr !== undefined ? Promise.reject(opts.throwErr) : Promise.resolve(result);
      return p.then(onFul, onRej);
    },
  };
  return {
    from(table: string) {
      log?.from.push(table);
      return {
        select(cols: string) {
          log?.select.push(cols);
          return {
            in(col: string, values: readonly string[]) {
              log?.in.push([col, [...values]]);
              return thenable;
            },
          };
        },
      };
    },
  };
}

// -----------------------------------------------------------------------------
// Section 1 — fetchFenceConfig: the ONE combined read (G-W1 / R3 #3e)
// -----------------------------------------------------------------------------

async function fenceReadPins(): Promise<void> {
  console.log('fetchFenceConfig — the ONE combined config read');
  const log: FenceCallLog = { from: [], select: [], in: [], awaited: 0 };
  const client = fenceClient(
    [
      { key: 'geo_read_only', value: false },
      { key: 'geo_write_epoch', value: 1 },
      { key: 'geo_primary_override', value: OTHER },
    ],
    { log },
  );
  const out = await fetchFenceConfig(client);

  eq('exactly ONE query is awaited (one round trip — G-W1)', log.awaited, 1);
  eq('one from() against config', log.from, ['config']);
  eq('one select() of key,value', log.select, ['key,value']);
  eq(
    'one .in() with the THREE fence keys in the pinned order (the override rides the same read — R3 #3e)',
    log.in,
    [['key', ['geo_read_only', 'geo_write_epoch', 'geo_primary_override']]],
  );
  eq('the pinned key constant matches the query shape', [...FENCE_CONFIG_KEYS], ['geo_read_only', 'geo_write_epoch', 'geo_primary_override']);
  eq('rows map to raw values', out, {
    ok: true,
    values: { read_only: false, write_epoch: 1, primary_override: OTHER },
  });

  const partial = await fetchFenceConfig(fenceClient([{ key: 'geo_read_only', value: 0 }]));
  eq('missing rows read as undefined (the gate decides what absence means)', partial, {
    ok: true,
    values: { read_only: 0, write_epoch: undefined, primary_override: undefined },
  });

  const nullData = await fetchFenceConfig(fenceClient(null));
  eq('data:null reads as all-undefined (never a fabricated verdict)', nullData, {
    ok: true,
    values: { read_only: undefined, write_epoch: undefined, primary_override: undefined },
  });

  const pgErr = await fetchFenceConfig(fenceClient(null, { error: { message: 'permission denied' } }));
  eqTrue('a PostgREST error is the G-W1 class (ok:false), never a verdict', !pgErr.ok && (pgErr as { error: { message: string } }).error.message === 'permission denied');

  const boom = new Error('conn refused');
  const thrown = await fetchFenceConfig(fenceClient(null, { throwErr: boom }));
  eqTrue('a transport throw is the G-W1 class (ok:false), error carried', !thrown.ok && (thrown as { error: Error }).error === boom);
}

// -----------------------------------------------------------------------------
// Section 2 — G-W1: read error ⇒ VERBATIM throw (the call site maps ⇒ 500)
// -----------------------------------------------------------------------------

function gw1Pins(): void {
  console.log('G-W1 (observe): unreadable fence throws VERBATIM — never a verdict');
  const err = new Error('config read failed: connection refused');
  let caught: unknown = null;
  let threw = false;
  try {
    geoWriteGate({ ok: false, error: err }, {});
  } catch (e) {
    threw = true;
    caught = e;
  }
  eqTrue('the fence-read error is THROWN (never masqueraded 503-or-pass)', threw);
  eqTrue('the SAME error object propagates verbatim (no wrapping/rewording)', caught === err);

  const raw = { message: 'PostgREST error object (not an Error instance)' };
  let caught2: unknown = null;
  try {
    geoWriteGate({ ok: false, error: raw }, {});
  } catch (e) {
    caught2 = e;
  }
  eqTrue('non-Error read failures propagate verbatim too', caught2 === raw);

  // The healthy fence with NO carry is accepted: G-W1 never fires on a good read.
  eq('control: open fence + no carry ⇒ accepted', gate({}), { ok: true, writeEpoch: 1 });
}

// -----------------------------------------------------------------------------
// Section 3 — G-W2: geo_read_only !== literal false ⇒ 503 read_only_mode
// -----------------------------------------------------------------------------

function gw2Pins(): void {
  console.log("G-W2: strict-boolean fail-closed ('true' string ≠ true — the geo_resolve.ts:91 mirror)");
  const F = { ok: false, code: 'read_only_mode' };
  eq("read_only 'true' string ⇒ fenced", gate({ read_only: 'true' }), F);
  eq('read_only literal false ⇒ OPEN', gate({ read_only: false }), { ok: true, writeEpoch: 1 });
  eq('read_only missing ⇒ fenced (fail-closed)', gate({ read_only: undefined }), F);
  eq('read_only 0 ⇒ fenced (0 is not literal false)', gate({ read_only: 0 }), F);
  eq('read_only 1 ⇒ fenced (1 is not literal false)', gate({ read_only: 1 }), F);
  eq('read_only literal true boolean ⇒ fenced (true ≠ false)', gate({ read_only: true }), F);
  eq('read_only null ⇒ fenced', gate({ read_only: null }), F);
  eq("read_only 'false' string ⇒ fenced (strings never unfence)", gate({ read_only: 'false' }), F);
}

// -----------------------------------------------------------------------------
// Section 4 — G-W3: epoch absent OR jsonb_typeof ≠ 'number' ⇒ 503
// -----------------------------------------------------------------------------

function gw3Pins(): void {
  console.log("G-W3: epoch presence fence (absent / non-number jsonb cannot be vouched)");
  const F = { ok: false, code: 'read_only_mode' };
  eq('epoch row absent ⇒ fenced (cannot vouch)', gate({ write_epoch: undefined }), F);
  eq("epoch '0' string ⇒ fenced (jsonb string is not a number)", gate({ write_epoch: '0' }), F);
  eq('epoch 0 number ⇒ OPEN (the 0019 seed state)', gate({ write_epoch: 0 }), { ok: true, writeEpoch: 0 });
  eq('epoch null ⇒ fenced', gate({ write_epoch: null }), F);
  eq('epoch object ⇒ fenced', gate({ write_epoch: { v: 1 } }), F);
  eq('epoch boolean ⇒ fenced', gate({ write_epoch: true }), F);
  eq('epoch array ⇒ fenced', gate({ write_epoch: [0] }), F);
  // Note: a non-INTEGER jsonb NUMBER (e.g. 1.5) passes G-W3's presence+type
  // letter, but the 0019 fm_config_patch_validator makes a float epoch
  // un-storable by ANY writer (bare-integer-digits shape) — and every G-W4
  // integer carry fences against it (< or >) by construction. The engine's
  // own vouch stays per the §2 letter (typeof number).
}

// -----------------------------------------------------------------------------
// Section 5 — G-W4: the epoch carry (stale ⇒ 503, future ⇒ 503, equal ⇒ pass)
// -----------------------------------------------------------------------------

function gw4Pins(): void {
  console.log('G-W4: client epoch carry — stale/future/non-integer fenced, absent ⇒ own observation');
  const F = { ok: false, code: 'read_only_mode' };
  // config epoch 1 (hand-computed vs the carry):
  eq('carry 0 / config 1 ⇒ fenced (stale writer — the split-brain stopper)', gate({}, 0), F);
  eq('carry 2 / config 1 ⇒ fenced (claims NEWER than config — unvouchable)', gate({}, 2), F);
  eq('carry 1 / config 1 ⇒ OPEN (equality)', gate({}, 1), { ok: true, writeEpoch: 1 });
  eq('carry absent ⇒ OPEN (engine observation governs — old callers untouched)', gate({}, undefined), { ok: true, writeEpoch: 1 });
  eq('carry 1.5 ⇒ fenced (non-integer — refuse, never best-effort parse)', gate({}, 1.5), F);
  eq("carry '1' string ⇒ fenced (refuse, never parsed)", gate({}, '1'), F);
  eq('carry null ⇒ fenced (a present non-integer carry)', gate({}, null), F);
  eq('carry true ⇒ fenced', gate({}, true), F);
  eq('carry -1 / config 1 ⇒ fenced (stale)', gate({}, -1), F);
  // config epoch 0 (the 0019 seed state):
  eq('carry 0 / config 0 ⇒ OPEN (boundary equality at 0)', gate({ write_epoch: 0 }, 0), { ok: true, writeEpoch: 0 });
  eq('carry absent / config 0 ⇒ OPEN', gate({ write_epoch: 0 }), { ok: true, writeEpoch: 0 });
  eq('carry 1 / config 0 ⇒ fenced (future vs the seed)', gate({ write_epoch: 0 }, 1), F);
  // config epoch 2 (post-first-promotion):
  eq('carry 2 / config 2 ⇒ OPEN', gate({ write_epoch: 2 }, 2), { ok: true, writeEpoch: 2 });
  eq('carry 1 / config 2 ⇒ fenced (stale)', gate({ write_epoch: 2 }, 1), F);
  eq('carry 3 / config 2 ⇒ fenced (future)', gate({ write_epoch: 2 }, 3), F);
}

// -----------------------------------------------------------------------------
// Section 6 — G-W5: the echo contract (accepted ⇒ observed epoch)
// -----------------------------------------------------------------------------

function gw5Pins(): void {
  console.log('G-W5: accepted verdicts carry the observed epoch for the write-branch echo');
  eq('echo = observed epoch (config 3, equal carry)', gate({ write_epoch: 3 }, 3), { ok: true, writeEpoch: 3 });
  eq('echo = observed epoch (no carry — the engine observation)', gate({ write_epoch: 3 }), { ok: true, writeEpoch: 3 });
  eq('echo is NEVER the client carry when it differs (stale carry would be fenced, not echoed)', gate({ write_epoch: 5 }, 4), {
    ok: false,
    code: 'read_only_mode',
  });
}

// -----------------------------------------------------------------------------
// Section 7 — isWritePlan: the gate call site's only trigger
// -----------------------------------------------------------------------------

function isWritePlanPins(): void {
  console.log('isWritePlan: mutating ops / scatter §5.2 markers ⇒ write plan');
  const readBody = { v: 1, qid: 'q-1', table: 'orders', query: { select: [{ op: 'count', alias: 'c' }] }, coverage_mode: 'best_effort' };
  eqFalse('a valid v0 read body is NOT a write plan', isWritePlan(readBody));
  eqFalse('read_plane additive field is not a write marker', isWritePlan({ ...readBody, read_plane: 'replica' }));
  eqFalse('min_lsn is not a write marker', isWritePlan({ ...readBody, min_lsn: '0/1' }));
  eqFalse('directory_snapshot is not a write marker', isWritePlan({ ...readBody, directory_snapshot: 'x' }));
  eqFalse('column_types is not a write marker', isWritePlan({ ...readBody, column_types: { amount: 'numeric' } }));
  eqTrue('a select entry declaring insert IS a write plan', isWritePlan({ ...readBody, query: { select: [{ op: 'insert', col: 'x' }] } }));
  eqTrue('upsert IS a write plan', isWritePlan({ ...readBody, query: { select: [{ op: 'upsert' }] } }));
  eqTrue('update IS a write plan', isWritePlan({ ...readBody, query: { select: [{ op: 'update' }] } }));
  eqTrue('delete IS a write plan', isWritePlan({ ...readBody, query: { select: [{ op: 'delete' }] } }));
  eqTrue('a mutating op mixed into the select list IS a write plan', isWritePlan({ ...readBody, query: { select: [{ op: 'sum', col: 'a' }, { op: 'insert' }] } }));
  // MUTANT PIN: this cell is temporarily disabled for the mutant-D run.
  // eqTrue('the scatter §5.2 idempotency_key marker IS a write plan', isWritePlan({ ...readBody, idempotency_key: '01JEXAMPLE' }));
  eqFalse(
    'a BARE write_epoch carry is NOT a write plan (reads must never consult the fence — the gate sees the carry when a write marker routes the body)',
    isWritePlan({ ...readBody, write_epoch: 1 }),
  );
  eqFalse('null body is not a write plan', isWritePlan(null));
  eqFalse('array body is not a write plan', isWritePlan([1, 2]));
  eqFalse('non-object body is not a write plan', isWritePlan(7));
  eqTrue(
    'the PARSED v0 request is never a write plan (the parse drops the raw markers — the gate sees the raw body)',
    parseWhEngineRequest(readBody) !== null && !isWritePlan(parseWhEngineRequest(readBody)),
  );
}

// -----------------------------------------------------------------------------
// Section 8 — the R3 dispatch identity (writes AND legacy reads)
// -----------------------------------------------------------------------------

function dispatchPins(): void {
  console.log("R3 dispatch identity: override==own ⇒ engine-local, other ⇒ per-override, absent ⇒ placements");
  // writes:
  eq('write dispatch: override == own ⇒ ENGINE-LOCAL (the future write branch executes against the co-hosted DB)', resolveWriteDispatch({ read_only: false, write_epoch: 1, primary_override: OWN }, OWN), {
    kind: 'engine_local',
    target: OWN,
  });
  eq('write dispatch: override == other ⇒ dispatch per override', resolveWriteDispatch({ read_only: false, write_epoch: 1, primary_override: OTHER }, OWN), {
    kind: 'remote',
    target: OTHER,
  });
  eq('write dispatch: override absent ⇒ as-built placements', resolveWriteDispatch({ read_only: false, write_epoch: 1, primary_override: undefined }, OWN), {
    kind: 'placements',
  });
  eqFalse('write dispatch: non-string override (number) is fail-safe placements, never a guess', (() => {
    const d = resolveWriteDispatch({ read_only: false, write_epoch: 1, primary_override: 42 }, OWN);
    return d.kind !== 'placements';
  })());
  eqTrue('write dispatch: empty-string override is placements', resolveWriteDispatch({ read_only: false, write_epoch: 1, primary_override: '' }, OWN).kind === 'placements');
  eqTrue('write dispatch: null override is placements', resolveWriteDispatch({ read_only: false, write_epoch: 1, primary_override: null }, OWN).kind === 'placements');
  // reads (re-audit #6):
  eq('read dispatch: override == own ⇒ ENGINE-LOCAL + the B2 warning detail token (LOUD, never silent)', resolveReadDispatch({ read_only: false, write_epoch: 1, primary_override: OWN }, OWN), {
    kind: 'engine_local',
    target: OWN,
    warning: { shard: OWN, code: 'geo_fallback_primary', est_rows: 0, retried: false, detail: 'geo_promoted_primary_local' },
  });
  eq('read dispatch: override == other ⇒ dispatch per override, NO token (normal post-promotion serving)', resolveReadDispatch({ read_only: false, write_epoch: 1, primary_override: OTHER }, OWN), {
    kind: 'remote',
    target: OTHER,
  });
  eq('read dispatch: override absent ⇒ as-built placements (the byte-identical pre-r49 path)', resolveReadDispatch({ read_only: false, write_epoch: 1, primary_override: undefined }, OWN), {
    kind: 'placements',
  });
  eqTrue('read dispatch: non-string override is fail-safe placements', resolveReadDispatch({ read_only: false, write_epoch: 1, primary_override: 7 }, OWN).kind === 'placements');
  // the own-ref fail-safe (the co-hosted law): an UNKNOWN own ref ('') can
  // never engine-local (it might BE the override) and never remote-guess —
  // the identity degrades to placements.
  eqTrue(
    'write dispatch: UNKNOWN own ref (\'\') degrades to placements even with a set override (never a self-dispatch guess)',
    resolveWriteDispatch({ read_only: false, write_epoch: 1, primary_override: OTHER }, '').kind === 'placements',
  );
  eqTrue(
    'read dispatch: UNKNOWN own ref (\'\') degrades to placements too',
    resolveReadDispatch({ read_only: false, write_epoch: 1, primary_override: OTHER }, '').kind === 'placements',
  );
  // the own-ref derivation (the SUPABASE_URL subdomain — the co-hosted law):
  eq('ownProjectRefFromSupabaseUrl: the subdomain IS the ref', ownProjectRefFromSupabaseUrl('https://abcdefghij1234567890.supabase.co/rest/v1'), 'abcdefghij1234567890');
  eqTrue('ownProjectRefFromSupabaseUrl: empty url ⇒ \'\' (fail-safe)', ownProjectRefFromSupabaseUrl('') === '');
  eqTrue('ownProjectRefFromSupabaseUrl: no subdomain ⇒ \'\' (fail-safe)', ownProjectRefFromSupabaseUrl('not-a-url') === '');
  // the wire body helper:
  eq('fenceReadOnlyBody: the §2 wire shape VERBATIM', fenceReadOnlyBody('q-1'), {
    v: 1,
    qid: 'q-1',
    error: { code: 'read_only_mode', message: 'geo read-only: writes are fenced; re-discover before resuming (stale-write guard, geo doc §2.3 #4)' },
  });
  eqTrue('the 503 message constant matches §2 byte-for-byte', FENCE_503_MESSAGE === 'geo read-only: writes are fenced; re-discover before resuming (stale-write guard, geo doc §2.3 #4)');
}

// -----------------------------------------------------------------------------
// Fixtures — engine core + entrypoint
// -----------------------------------------------------------------------------

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

const COLS: Record<string, string> = { region: 'text', amount: 'numeric', qty: 'int8', created_at: 'timestamptz' };

function baseReq(overrides: Record<string, unknown> = {}): WhEngineRequest {
  return {
    v: 1,
    qid: 'q-1',
    table: 'orders',
    query: { select: [{ op: 'count', alias: 'c' }] },
    coverage_mode: 'best_effort',
    ...overrides,
  } as WhEngineRequest;
}

function scalarEnv(shard: string, count = 5): WhPartialEnvelope {
  return {
    v: 1,
    shard,
    table: 'orders',
    schema_version: 3,
    partial: { kind: 'scalar', aggs: { c: { op: 'count' } }, rows: [{ k: [], a: { c: count } }], rowCount: 1, more: false },
  };
}

function neverTimeoutTimers(): WhEngineTimers {
  return {
    nowMs: () => 0,
    startTimeout: () => ({ promise: new Promise<'timeout'>(() => {}), dispose: () => {} }),
  };
}

function seenFetcher(seen: string[]): WhShardFetcher {
  return async (shard) => {
    seen.push(shard);
    return { ok: true, envelope: scalarEnv(shard), estRows: 10 };
  };
}

interface GateCounters {
  seen: string[];
  fenceReads: number;
  geoModeReads: number;
}

function gateDeps(opts: {
  rows?: WhDirectoryRow[];
  fence?: WhFenceRead | null; // null ⇒ fetchFenceConfig dep NOT wired
  fenceThrows?: Error;
  ownProjectRef?: string | null; // null ⇒ dep NOT wired
  geoRowRef?: string; // wires the r47 replica-plane deps when set
} = {}): { deps: WhEngineDeps; counters: GateCounters } {
  const counters: GateCounters = { seen: [], fenceReads: 0, geoModeReads: 0 };
  const deps: WhEngineDeps = {
    probeDirectoryVersion: async () => 42,
    readDirectory: async () => ({ rows: opts.rows ?? [dirRow('shard-a'), dirRow('shard-b')], version: 42 }),
    fetcher: seenFetcher(counters.seen),
    hasRealFetcher: true,
    ...(opts.geoRowRef !== undefined
      ? {
          readGeoMode: async () => {
            counters.geoModeReads++;
            return 'spread';
          },
          readGeoDirectory: async () => [
            {
              // wired only when opts.geoRowRef !== undefined (spread guard
              // above) — the ! narrows what TS can't see through the spread.
              project_ref: opts.geoRowRef!,
              region: 'us-east-1',
              subscription: 'geo_sub',
              state: 'serving',
              applied_lsn: '0/FFFFFFFE',
              replay_ts: '2026-09-29T00:00:00Z',
              lag_bytes: null,
              coverage: ['orders'],
              last_health_at: '2026-09-29T00:00:00.000000Z',
              updated_at: '2026-09-29T00:00:00.000000Z',
            },
          ],
        }
      : {}),
    ...(opts.fence !== null
      ? {
          fetchFenceConfig: async () => {
            counters.fenceReads++;
            if (opts.fenceThrows !== undefined) throw opts.fenceThrows;
            return opts.fence ?? fence();
          },
        }
      : {}),
    ...(opts.ownProjectRef !== null && opts.ownProjectRef !== undefined ? { ownProjectRef: opts.ownProjectRef } : {}),
  };
  return { deps, counters };
}

function gateReq(path: string, body: unknown): Request {
  return new Request(`https://ref.supabase.co/functions/v1/warehouse-engine${path}`, {
    method: 'POST',
    headers: { authorization: 'Bearer t', apikey: 't', 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
}

/** Strip the run-to-run noise (latency fields) so full-body deep-eqs are exact. */
function stable(v: unknown): unknown {
  if (v === null || typeof v !== 'object') return v;
  const obj = { ...(v as Record<string, unknown>) };
  delete obj.latency_ms;
  if (Array.isArray(obj.perShard)) {
    obj.perShard = (obj.perShard as Record<string, unknown>[]).map((p) => {
      const q = { ...p };
      delete q.latencyMs;
      return q;
    });
  }
  return obj;
}

const READ_BODY = {
  v: 1,
  qid: 'q-1',
  table: 'orders',
  query: { select: [{ op: 'count', alias: 'c' }] },
  coverage_mode: 'best_effort',
};
const WRITE_MARK_BODY = { ...READ_BODY, idempotency_key: '01JEXAMPLEWRITE' };
const MUTATING_BODY = { ...READ_BODY, query: { select: [{ op: 'insert', col: 'x' }] } };

// -----------------------------------------------------------------------------
// Section 9 — engine core: the R3 read-dispatch consumption
// -----------------------------------------------------------------------------

async function coreDispatchPins(): Promise<void> {
  console.log('executeWhQuery: the R3 read-dispatch instruction consumption (legacy primary plane)');

  // engine_local: single-target dispatch to the OWN ref + the B2 token, even
  // when placements WOULD have been selected.
  const seenLocal: string[] = [];
  const resLocal = await executeWhQuery({
    req: baseReq(),
    columnTypes: COLS,
    directoryRows: [dirRow('shard-a'), dirRow('shard-b')],
    shardKeyColumn: '',
    shardKeyType: 'none',
    directoryVersion: 42,
    timers: neverTimeoutTimers(),
    fetcher: seenFetcher(seenLocal),
    geoReadDispatch: {
      kind: 'engine_local',
      target: OWN,
      warning: { shard: OWN, code: 'geo_fallback_primary', est_rows: 0, retried: false, detail: 'geo_promoted_primary_local' },
    },
  });
  eq('engine_local: the OWN ref is the ONLY fetch target (no self-POST — the co-hosted law)', seenLocal, [OWN]);
  eq('engine_local: the B2 warning token rides the response (LOUD, never silent)', resLocal.warnings, [
    { shard: OWN, code: 'geo_fallback_primary', est_rows: 0, retried: false, detail: 'geo_promoted_primary_local' },
  ]);
  eq('engine_local: single-target coverage denominator (1/1)', resLocal.coverage, '1/1');
  eq('engine_local: merges the co-hosted partial (count 5)', resLocal.result, { c: 5 });

  // the misattribution fix: EMPTY placements + override==own ⇒ served
  // engine-locally, never the bare tier_warm 404 (R3).
  const seenEmpty: string[] = [];
  const resEmpty = await executeWhQuery({
    req: baseReq(),
    columnTypes: COLS,
    directoryRows: [],
    shardKeyColumn: '',
    shardKeyType: 'none',
    directoryVersion: 42,
    timers: neverTimeoutTimers(),
    fetcher: seenFetcher(seenEmpty),
    geoReadDispatch: {
      kind: 'engine_local',
      target: OWN,
      warning: { shard: OWN, code: 'geo_fallback_primary', est_rows: 0, retried: false, detail: 'geo_promoted_primary_local' },
    },
  });
  eq('engine_local bypasses the G-A tier_warm throw on empty placements (the table is fine; the primary moved)', resEmpty.coverage, '1/1');

  // CONTROL: same empty placements with the instruction at 'placements' ⇒ the
  // as-built tier_warm 404 STILL fires (G-A plane-invariance preserved).
  let threwWarm = false;
  try {
    await executeWhQuery({
      req: baseReq(),
      columnTypes: COLS,
      directoryRows: [],
      shardKeyColumn: '',
      shardKeyType: 'none',
      directoryVersion: 42,
      timers: neverTimeoutTimers(),
      fetcher: seenFetcher([]),
      geoReadDispatch: { kind: 'placements' },
    });
  } catch (e) {
    threwWarm = (e as { code?: string }).code === 'tier_warm';
  }
  eqTrue("control: 'placements' on empty placements still throws tier_warm (G-A unchanged)", threwWarm);

  // remote: per-override single target, NO B2 token.
  const seenRemote: string[] = [];
  const resRemote = await executeWhQuery({
    req: baseReq(),
    columnTypes: COLS,
    directoryRows: [dirRow('shard-a'), dirRow('shard-b')],
    shardKeyColumn: '',
    shardKeyType: 'none',
    directoryVersion: 42,
    timers: neverTimeoutTimers(),
    fetcher: seenFetcher(seenRemote),
    geoReadDispatch: { kind: 'remote', target: OTHER },
  });
  eq('remote: the override ref is the ONLY fetch target', seenRemote, [OTHER]);
  eq('remote: no geo_promoted_primary_local token (normal post-promotion serving)', resRemote.warnings, []);
  eq('remote: single-target coverage 1/1', resRemote.coverage, '1/1');

  // placements: the byte-identical as-built fan-out.
  const seenPl: string[] = [];
  await executeWhQuery({
    req: baseReq(),
    columnTypes: COLS,
    directoryRows: [dirRow('shard-a'), dirRow('shard-b')],
    shardKeyColumn: '',
    shardKeyType: 'none',
    directoryVersion: 42,
    timers: neverTimeoutTimers(),
    fetcher: seenFetcher(seenPl),
    geoReadDispatch: { kind: 'placements' },
  });
  eq("'placements' instruction = the as-built fan-out over the directory selections", seenPl.sort(), ['shard-a', 'shard-b']);

  // the replica plane NEVER consults the instruction (its identity is the geo row).
  const seenRep: string[] = [];
  const resRep = await executeWhQuery({
    req: baseReq({ read_plane: 'replica' }),
    columnTypes: COLS,
    directoryRows: [dirRow('shard-a'), dirRow('shard-b')],
    shardKeyColumn: '',
    shardKeyType: 'none',
    directoryVersion: 42,
    timers: neverTimeoutTimers(),
    fetcher: seenFetcher(seenRep),
    geoMode: 'spread',
    geoReader: async () => [
      {
        project_ref: 'replicaref',
        region: 'us-east-1',
        subscription: 'geo_sub',
        state: 'serving',
        applied_lsn: '0/FFFFFFFE',
        replay_ts: '2026-09-29T00:00:00Z',
        lag_bytes: null,
        coverage: ['orders'],
        last_health_at: '2026-09-29T00:00:00.000000Z',
        updated_at: '2026-09-29T00:00:00.000000Z',
      },
    ],
    geoReadDispatch: {
      kind: 'engine_local',
      target: OWN,
      warning: { shard: OWN, code: 'geo_fallback_primary', est_rows: 0, retried: false, detail: 'geo_promoted_primary_local' },
    },
  });
  eq('replica plane: the instruction is IGNORED (dispatch went to the geo row ref)', seenRep, ['replicaref']);
  eq('replica plane: no B2 token on the wire', resRep.warnings, []);
}

// -----------------------------------------------------------------------------
// Section 10 — entrypoint wiring: the gate call site + the read dispatch
// -----------------------------------------------------------------------------

async function withEnv(key: string, value: string, body: () => Promise<void>): Promise<void> {
  const saved = Deno.env.get(key);
  try {
    Deno.env.set(key, value);
    await body();
  } finally {
    if (saved === undefined) Deno.env.delete(key);
    else Deno.env.set(key, saved);
  }
}

async function entrypointPins(): Promise<void> {
  console.log('wh_entrypoint: the GATE CONTRACT wired at the call site (write wave lands pre-gated)');

  await withEnv('WHE_BEARER_TOKEN', 't', async () => {

  // order: the r40 P2-6 deploy gate keeps precedence — pre-flip, a write plan
  // still gets the pinned stub rejection FIRST (the gate is behind it).
  await (async () => {
    const { deps } = gateDeps({ fence: fence({ read_only: true }) });
    const res = await handleWhEngineRequest(gateReq('/query', WRITE_MARK_BODY), { ...deps, hasRealFetcher: false });
    eq('pre-flip: the deploy gate 500 keeps precedence over the fence (r40 P2-6 pin unchanged)', res.status, 500);
    eq('pre-flip: the pinned stub message', (await res.json()).error.message, 'real shard fetcher lands after live probes #1/#2 (PAT-gated design freeze for the QC2 compile)');
  })();

  // G-W1 at the wire: read error ⇒ 500, NEVER 503.
  await (async () => {
    const { deps } = gateDeps({ fence: { ok: false, error: new Error('config read failed: connection refused') } });
    const res = await handleWhEngineRequest(gateReq('/query', WRITE_MARK_BODY), deps);
    const body = await res.json();
    eq('G-W1: a fence read error is a 500 (never masqueraded as unwritable-or-writable 503)', res.status, 500);
    eq('G-W1: the pinned internal wire (r40 P3-4 — no raw error text)', body, { v: 1, qid: 'q-1', error: { code: 'internal', message: 'internal engine error' } });
  })();

  // G-W2 at the wire: the §2 503 shape VERBATIM.
  await (async () => {
    const { deps, counters } = gateDeps({ fence: fence({ read_only: true }) });
    const res = await handleWhEngineRequest(gateReq('/query', WRITE_MARK_BODY), deps);
    const body = await res.json();
    eq('G-W2: fenced write plan ⇒ 503 read_only_mode', res.status, 503);
    eq('G-W2: the §2 body VERBATIM (qid echoed, pinned code + message, no new warnings)', body, fenceReadOnlyBody('q-1'));
    eq('G-W2: exactly ONE fence read for the write plan (the combined read — G-W1)', counters.fenceReads, 1);
    eq('G-W2: fenced BEFORE any fan-out (no shard touched)', counters.seen, []);
  })();

  // the mission pin: a HYPOTHETICAL MUTATING PLAN routes through the gate —
  // a closed fence 503s it even though the v0 parse would reject it 400 (the
  // gate runs BEFORE the §4.3 parse gates: the write wave lands pre-gated).
  await (async () => {
    const { deps } = gateDeps({ fence: fence({ read_only: true }) });
    const res = await handleWhEngineRequest(gateReq('/query', MUTATING_BODY), deps);
    const body = await res.json();
    eq('MUTATING plan on a closed fence ⇒ 503 read_only_mode (the gate precedes the §4.3 parse)', res.status, 503);
    eq('MUTATING plan: the §2 body, not a parse 400', body, fenceReadOnlyBody('q-1'));
  })();

  // G-W3 at the wire: absent epoch ⇒ 503.
  await (async () => {
    const { deps } = gateDeps({ fence: fence({ write_epoch: undefined }) });
    const res = await handleWhEngineRequest(gateReq('/query', WRITE_MARK_BODY), deps);
    eq('G-W3: absent epoch row ⇒ 503 at the wire', res.status, 503);
    eq('G-W3: same read_only_mode body', (await res.json()).error.code, 'read_only_mode');
  })();

  // G-W4 at the wire: stale carry fenced, equal carry passes.
  await (async () => {
    const { deps } = gateDeps({ fence: fence({ write_epoch: 1 }) });
    const stale = await handleWhEngineRequest(gateReq('/query', { ...WRITE_MARK_BODY, write_epoch: 0 }), deps);
    eq('G-W4: stale carry 0 vs config 1 ⇒ 503 at the wire', stale.status, 503);
    const fresh = await handleWhEngineRequest(gateReq('/query', { ...WRITE_MARK_BODY, write_epoch: 2 }), gateDeps({ fence: fence({ write_epoch: 1 }) }).deps);
    eq('G-W4: future carry 2 vs config 1 ⇒ 503 at the wire', fresh.status, 503);
    const equal = await handleWhEngineRequest(gateReq('/query', { ...WRITE_MARK_BODY, write_epoch: 1 }), gateDeps({ fence: fence({ write_epoch: 1 }) }).deps);
    eq('G-W4: equal carry 1 vs config 1 ⇒ gate passes (v0 as-built read path answers)', equal.status, 200);
    const junk = await handleWhEngineRequest(gateReq('/query', { ...WRITE_MARK_BODY, write_epoch: '1' }), gateDeps({ fence: fence({ write_epoch: 1 }) }).deps);
    eq("G-W4: '1' string carry ⇒ 503 (refuse, never best-effort parse)", junk.status, 503);
  })();

  // accepted write plan on a healthy fence: passes through to the as-built
  // v0 handling (the write branch is the write wave's — it inserts AFTER
  // this gate).
  await (async () => {
    const { deps, counters } = gateDeps({ fence: fence() });
    const res = await handleWhEngineRequest(gateReq('/query', WRITE_MARK_BODY), deps);
    eq('accepted write plan: the gate OPENS (no 503) and the as-built v0 path serves the read plan', res.status, 200);
    eq('accepted write plan: normal placements coverage', (await res.json()).coverage, '2/2');
    eq('accepted write plan: exactly ONE fence read', counters.fenceReads, 1);
    const mutRes = await handleWhEngineRequest(gateReq('/query', MUTATING_BODY), gateDeps({ fence: fence() }).deps);
    eq('accepted MUTATING plan: the v0 parse rejects it 400 (the write wave replaces this fall-through with its branch)', mutRes.status, 400);
    eq('accepted MUTATING plan: malformed, not read_only_mode', (await mutRes.json()).error.code, 'malformed');
    const noGate = await handleWhEngineRequest(gateReq('/query', MUTATING_BODY), gateDeps({ fence: null }).deps);
    eq(
      'gate dep UNWIRED: a write plan FAILS CLOSED 500 (cannot vouch the fence — never a silent fall-through, never a masquerading 503)',
      noGate.status,
      500,
    );
    eq(
      'gate dep UNWIRED: the pinned internal wire (G-W1 class — 500, not 503)',
      (await noGate.json()).error,
      { code: 'internal', message: 'geo write fence dep unwired — cannot vouch the write fence (G-W1 fail-closed)' },
    );
  })();

  // READS never consult the FENCE (§2: "Reads (both planes) never consult
  // the fence") — a fenced fleet keeps serving legacy reads.
  await (async () => {
    const { deps, counters } = gateDeps({ fence: fence({ read_only: true }) });
    const res = await handleWhEngineRequest(gateReq('/query', READ_BODY), deps);
    eq('fenced fleet (read_only true): a legacy read still serves 200 (reads never consult the fence)', res.status, 200);
    eq('fenced fleet: no read_only_mode error on the read wire', (await res.json()).error, undefined);
    eq('fenced fleet: no fence read at all without the dispatch-identity target (ownProjectRef unwired)', counters.fenceReads, 0);
    const { deps: deps2, counters: c2 } = gateDeps({ fence: fence({ read_only: true }), ownProjectRef: OWN });
    const res2 = await handleWhEngineRequest(gateReq('/query', READ_BODY), deps2);
    eq('fenced fleet + identity target: the read STILL serves 200 (the config read is the R3 dispatch identity, never the gate)', res2.status, 200);
    eq('fenced fleet + identity target: exactly ONE fence read (the combined read)', c2.fenceReads, 1);
  })();

  // byte-identical regression: override unset ⇒ the legacy read wire is
  // IDENTICAL with the fence deps wired and with them absent.
  await (async () => {
    const wired = gateDeps({ fence: fence() });
    const unwired = gateDeps({ fence: null });
    const withFence = await handleWhEngineRequest(gateReq('/query', READ_BODY), wired.deps);
    const withoutFence = await handleWhEngineRequest(gateReq('/query', READ_BODY), unwired.deps);
    const baseline = stable(await withoutFence.json());
    eq('override unset: fence-wired response is byte-identical to the pre-r49 response (latency-stripped)', stable(await withFence.json()), baseline);
    eq('override unset: identical fetch targets', wired.counters.seen, unwired.counters.seen);
    // both deps wired, override explicitly unset ⇒ still byte-identical (the
    // dispatch identity resolved 'placements' — no instruction, no drift).
    const bothWired = gateDeps({ fence: fence(), ownProjectRef: OWN });
    const resBoth = await handleWhEngineRequest(gateReq('/query', READ_BODY), bothWired.deps);
    eq('override unset + both deps wired: byte-identical response (the placements identity ⇒ no instruction)', stable(await resBoth.json()), baseline);
    eq('override unset + both deps wired: identical fetch targets', bothWired.counters.seen, unwired.counters.seen);
    eq('override unset + both deps wired: exactly ONE fence read (the dispatch identity consult)', bothWired.counters.fenceReads, 1);
  })();

  // the read-path fence read DEGRADES (never throws): a failed config read
  // falls back to the as-built placements dispatch.
  await (async () => {
    const { deps } = gateDeps({ fence: { ok: false, error: new Error('config read failed') } });
    const res = await handleWhEngineRequest(gateReq('/query', READ_BODY), deps);
    eq('read-path fence read failure: degrade-not-throw (200, as-built placements)', res.status, 200);
    const okRead = await handleWhEngineRequest(gateReq('/query', READ_BODY), gateDeps({ fence: fence() }).deps);
    eq('read-path fence read failure: wire identical to a healthy read (latency-stripped)', stable(await res.json()), stable(await okRead.json()));
  })();

  // R3 at the wire: override == own ⇒ engine-local legacy reads.
  await (async () => {
    const { deps, counters } = gateDeps({ fence: fence({ primary_override: OWN }), ownProjectRef: OWN });
    const res = await handleWhEngineRequest(gateReq('/query', READ_BODY), deps);
    const body = await res.json();
    eq('R3: override == own ⇒ 200 served engine-locally', res.status, 200);
    eq('R3: the OWN ref is the ONLY fetch target (placements never fetched)', counters.seen, [OWN]);
    eq('R3: the B2 warning token is on the wire (LOUD)', body.warnings, [
      { shard: OWN, code: 'geo_fallback_primary', est_rows: 0, retried: false, detail: 'geo_promoted_primary_local' },
    ]);
    eq('R3: single-target coverage 1/1', body.coverage, '1/1');
  })();

  // R3 misattribution fix at the wire: empty placements + override == own ⇒
  // served engine-locally, NOT the bare tier_warm 404.
  await (async () => {
    const { deps } = gateDeps({ rows: [], fence: fence({ primary_override: OWN }), ownProjectRef: OWN });
    const res = await handleWhEngineRequest(gateReq('/query', READ_BODY), deps);
    eq('R3: empty placements + override==own ⇒ 200 engine-local (the misattribution fix)', res.status, 200);
    // CONTROL: same empty placements, override unset ⇒ the as-built 404.
    const control = await handleWhEngineRequest(gateReq('/query', READ_BODY), gateDeps({ rows: [], fence: fence() }).deps);
    eq('CONTROL: empty placements + override unset ⇒ the as-built tier_warm 404 (G-A unchanged)', control.status, 404);
    eq('CONTROL: the 404 body names tier_warm', (await control.json()).error.code, 'tier_warm');
  })();

  // R3 at the wire: override == other ⇒ dispatch per override, no token.
  await (async () => {
    const { deps, counters } = gateDeps({ fence: fence({ primary_override: OTHER }), ownProjectRef: OWN });
    const res = await handleWhEngineRequest(gateReq('/query', READ_BODY), deps);
    const body = await res.json();
    eq('R3: override == other ⇒ 200 dispatched per override', res.status, 200);
    eq('R3: the override ref is the ONLY fetch target', counters.seen, [OTHER]);
    eq('R3: no geo_promoted_primary_local token on the remote-dispatch wire', body.warnings, []);
  })();

  // the replica plane never consults the override (its identity is the geo row).
  await (async () => {
    const { deps, counters } = gateDeps({
      fence: fence({ primary_override: OWN }),
      ownProjectRef: OWN,
      geoRowRef: 'replicaref',
    });
    const res = await handleWhEngineRequest(gateReq('/query', { ...READ_BODY, read_plane: 'replica' }), deps);
    const body = await res.json();
    eq('replica plane: 200 served from the geo row ref (the override is NEVER consulted)', res.status, 200);
    eq('replica plane: fetch target is the geo row ref', counters.seen, ['replicaref']);
    eq('replica plane: no B2 token', body.warnings, []);
    eq('replica plane: the fence is not even READ (no dispatch identity consult on this plane)', counters.fenceReads, 0);
  })();
  });
}

// -----------------------------------------------------------------------------
// Harness report (hand-rolled runner) — assertion failures are FATAL.
// -----------------------------------------------------------------------------

Deno.test('geo_write_fence battery', async () => {
  await fenceReadPins();
  gw1Pins();
  gw2Pins();
  gw3Pins();
  gw4Pins();
  gw5Pins();
  isWritePlanPins();
  dispatchPins();
  await coreDispatchPins();
  await entrypointPins();
});

Deno.test('__report__', () => {
  console.log(`\ngeo_write_fence_test: ${passed} assertions passed, ${failed} failed`);
  if (failed > 0) throw new Error(`${failed} assertion(s) failed`);
});
