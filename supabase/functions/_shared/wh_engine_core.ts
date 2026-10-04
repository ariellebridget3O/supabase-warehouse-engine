// =============================================================================
// _shared/wh_engine_core.ts — warehouse ENGINE CORE (r39, QC1-QC2 v0).
// =============================================================================
// Pure, dependency-injected pipeline per findings_wh_catalog_contract.md §4
// + findings_wh_scatter_gather.md §4.1/§5.1: request parse (§4.3) -> merge
// plan -> conservative E11 shard pruning -> per-shard PostgREST URL compile
// -> WINDOW=16 fan-out (per-shard timeout, allSettled — never-throw) ->
// merge algebra (wh_merge) -> response assembly (§4.4).
//
// PURITY: imports ONLY wh_types/wh_canonical/wh_merge. All I/O (HTTP fetch,
// timers) is injected — offline-testable with plain fakes. The thin
// warehouse-engine/index.ts wires the real fetch/timers and does auth.
//
// Planner honesty (scatter §1.2 #4): anything v0 cannot merge correctly is
// rejected at plan time with a message naming the clause — never a silent
// mis-merge. Coverage counts MERGED contributions only (a lying/oversize/
// stale-schema partial is excluded with a warning, never merged).
//
// r69 (design_r69_shard_channel.md): the RPC plane — D2 plan_untemplated
// plan gate, the §6.1 rpc target construction, and the §6.2 flat-wire
// adapter (adaptWireEnvelope, AM-1/F-N1) — ALL gated on the additive
// rpcMode flag (F-N4: with rpcMode absent every path here is byte-identical
// to the pre-r69 pipeline; the flag is wiring-owned, never env-read here).
// =============================================================================

import { WhMergeError } from './wh_types.ts';
import type { WhColumnPlan, WhPartialEnvelope } from './wh_types.ts';
import { canonicalizeValue, compareCanonical, WhCanonicalizeError } from './wh_canonical.ts';
import type { WhCanonicalValue } from './wh_canonical.ts';
import {
  finalizeGroups,
  finalizeScalarAggs,
  mergeGroupedPartials,
  mergeScalarAggs,
} from './wh_merge.ts';
import type { WhFinalAggValue, WhMergePlan } from './wh_merge.ts';
import {
  checkHandshake,
  derivePlanMergeOps,
  manifestRowByHash,
  planOpsSubset,
  planTemplatesMissingInManifest,
} from './wh_handshake.ts';
import type {
  HandshakePlanRef,
  TemplateInventoryRow,
  WhShardHandshake,
} from './wh_handshake.ts';

// ---------- constants (§4.5 limits-as-contract) ----------
export const DEFAULT_WINDOW = 16;
export const MAX_WINDOW = 32;
export const DEFAULT_SHARD_TIMEOUT_MS = 8000;
export const MAX_GROUPS = 2000;
export const SHARD_REQUEST_HEADERS: Record<string, string> = { 'Accept-Profile': 'public' };

// ---------- types ----------
/** One pre-filtered directory row (v_warehouse_directory output shape). */
export interface WhDirectoryRow {
  shard: string;
  key_min: string | null;
  key_max: string | null;
  hash_slot: number | null;
  state: string;
  platform_status: string;
  schema_version: number;
  last_health_at: string;
  // r40 (contract erratum §4.3): passthrough directory metadata the entrypoint
  // projects per queried table. Optional so core fixtures stay unchanged; the
  // entrypoint's row mapper supplies them from v_warehouse_directory.
  logical_name?: string;
  shard_key_type?: 'none' | 'hash' | 'range' | 'time';
  shard_key_column?: string | null;
  table_schema_version?: number;
  // r44 (§4.4 est_rows lane): the directory's row_estimate (0013:119/155).
  // Optional — the §5.2 handshake warnings carry it when available so the
  // gather can weigh degrade-vs-abort; absent => 0 (no estimate known).
  row_estimate?: number;
}

// 'quorum_unmet' is in the union forward-compat (§4.4 pins 409) — v0 never
// throws it (quorum mode deferred, §6), but the entrypoint's status map must
// not 500-fallthrough if a future round adds the throw.
// 'plan_untemplated' (r69 D2/AM-3): the RPC plane's plan-honesty 4xx — a plan
// the wh_query RPC tier cannot serve (hashless / where-carrying / no
// rpc-eligible derived template). Thrown ONLY when rpcMode is on (F-N4);
// the entrypoint's status ladder maps it to 400 explicitly (AM-3).
export type WhEngineErrorCode =
  | 'malformed'
  | 'capacity_exceeded'
  | 'page_unavailable'
  | 'tier_warm'
  | 'quorum_unmet'
  | 'plan_untemplated'
  | 'internal';

export interface WhPerShardEntry {
  shard: string;
  ok: boolean;
  latencyMs: number;
  error: string | null;
  stamped?: boolean;
}

/** Engine error with the §4.4 status-table code space. `internal` carries
 *  perShard/latencyMs/directoryVersion on the payload (fail-fast 5xx). */
export class WhEngineError extends Error {
  readonly code: WhEngineErrorCode;
  readonly perShard?: WhPerShardEntry[];
  readonly latencyMs?: number;
  readonly directoryVersion?: number;

  constructor(
    code: WhEngineErrorCode,
    message: string,
    extra?: { perShard?: WhPerShardEntry[]; latencyMs?: number; directoryVersion?: number },
  ) {
    super(message);
    this.name = 'WhEngineError';
    this.code = code;
    if (extra?.perShard !== undefined) this.perShard = extra.perShard;
    if (extra?.latencyMs !== undefined) this.latencyMs = extra.latencyMs;
    if (extra?.directoryVersion !== undefined) this.directoryVersion = extra.directoryVersion;
  }
}

export interface WhSelectEntry {
  op: 'sum' | 'count' | 'min' | 'max' | 'avg';
  col?: string;
  alias?: string;
}

export interface WhWhereEntry {
  col: string;
  op: 'eq' | 'neq' | 'gt' | 'gte' | 'lt' | 'lte' | 'between' | 'is';
  value: unknown;
}

/** Normalized v1 request (§4.3). Neutral clauses (having=null/orderBy=null|[]/
 *  offset=0/distinct=false) are DROPPED at parse; non-neutral forms reject
 *  (planner honesty). */
export interface WhEngineRequest {
  v: 1;
  qid: string;
  table: string;
  query: {
    select: WhSelectEntry[];
    where?: WhWhereEntry[];
    groupBy?: string[];
    limit?: number | null;
  };
  coverage_mode: 'best_effort' | 'fail_fast';
  directory_snapshot?: string;
  // r47 read-plane extension (wh-contract r47 errata, additive): absent/null
  // => 'primary' = the pre-r47 behavior EXACTLY (geo deps never consulted);
  // 'replica' => the geo gates G1..G6 decide the plane. Unknown values are a
  // parse-time 400 (frozen-contract law). min_lsn REQUIRES read_plane
  // 'replica' (400 otherwise — the primary IS the write head; an
  // enforced-looking no-op floor would be planner-dishonest).
  read_plane?: 'primary' | 'replica';
  min_lsn?: string;
}

/** One row of `v_geo_directory` (0017 — the REPLICA selection view; column
 *  semantics per 0017:179-184, read from the migration not the spec sketch).
 *  NEVER mapped into WhDirectoryRow (no span columns — any mapping fabricates
 *  data, the single-dispatch law, geo-readplane impl plan §2). */
export interface WhGeoDirectoryRow {
  project_ref: string;
  region: string | null;
  subscription: string | null;
  state: string;
  applied_lsn: string | null;
  replay_ts: string | null;
  lag_bytes: number | string | null; // bigint rendered; NULL in v1 (refuse-to-guess) — never consumed
  coverage: unknown; // jsonb: null | string[] | other(fail-closed ∅) — the r47 pinned shape
  last_health_at: string;
  updated_at: string;
}

export interface WhEngineTimers {
  nowMs(): number;
  startTimeout(ms: number): { promise: Promise<'timeout'>; dispose(): void };
}

/** r69 (§3.1 D1): the §6.1 RPC spec a fan-out target carries when the rpcMode
 *  plane dispatches the wh_query RPC — `rpc` present on a target ⇒ the
 *  fetcher POSTs the byte-pinned call shape; absent ⇒ the residual select-
 *  shape GET (replica/geo-dispatch planes only, §3.4 arm 2). */
export interface WhRpcSpec {
  p_template_hash: string;
  p_params: Record<string, unknown>;
}

/** r69 (AM-1/F-N7): the fan-out ok-arm envelope — the select path returns
 *  the nested engine shape (WhPartialEnvelope); the RPC path returns the RAW
 *  flat §6.2 wire envelope (the seeded fn's shape verbatim,
 *  db/shard-migrations/0015_wh_query_rpc.sql:389-404/:418-432) which
 *  adaptWireEnvelope converts to WhPartialEnvelope at the consumption site,
 *  BEFORE the untouched gates. WhPartialEnvelope stays the post-adapter
 *  engine shape (wh_types.ts:80-86). */
export type WhFanoutEnvelope = WhPartialEnvelope | { [key: string]: unknown };

/** r69 (§3.1): widened to the THREE-ARG call — existing 2-arg fetcher impls
 *  (all pre-r69 tests) remain assignable (additive law); runFanout passes
 *  t.rpc through, undefined on the unset path. The ok-arm `envelope` is the
 *  RAW wire as received, declared `unknown` (AM-1/F-N7): the select path
 *  carries the engine shape, the rpc path the crash-gated flat wire — the
 *  transport layer adds no shape claim; deep validation is the consumption
 *  site's job (adaptWireEnvelope + the §6.3 gates). */
export type WhShardFetcher = (
  shard: string,
  url: string,
  rpc?: WhRpcSpec,
) => Promise<
  | { ok: true; envelope: unknown; estRows?: number }
  | { ok: false; warning: { code?: string; httpStatus?: number; stamped?: boolean }; estRows?: number }
>;

export interface WhEngineWarning {
  shard: string;
  code: string;
  est_rows: number;
  retried: boolean;
  detail?: string;
}

/** r49 B2 R3 (re-audit #6): the legacy primary-plane READ dispatch
 *  identity, computed by the entrypoint from the combined fence read (the
 *  `geo_primary_override` key rides the SAME one-query read, re-audit #3e).
 *  Consumed ONLY on the legacy primary plane (read_plane absent/'primary') —
 *  the replica plane's identity is the geo row and never consults it.
 *    * 'placements'    — override absent/unusable ⇒ the as-built placements
 *                        dispatch, byte-identical (the pre-r49 behavior).
 *    * 'engine_local'  — override == own ref ⇒ ONE target at `target` (the
 *                        co-hosted law: the own ref resolves with the
 *                        engine's own service key, the replica-plane
 *                        precedent) + the LOUD B2 warning token (R3: after
 *                        R16's demote the placements drop out and the bare
 *                        tier_warm 404 would MISATTRIBUTE — the table is
 *                        fine; the primary moved).
 *    * 'remote'        — override == another ref ⇒ ONE target at the
 *                        override ref (dispatch per override), no token
 *                        (normal serving).
 *  The instruction type lives HERE (the consumer); geo_write_fence.ts
 *  imports it type-only (the wh_* purity island stays untouched). */
export type WhGeoReadDispatch =
  | { kind: 'placements' }
  | { kind: 'engine_local'; target: string; warning: WhEngineWarning }
  | { kind: 'remote'; target: string };

export interface WhEngineResponse {
  v: 1;
  qid: string;
  directory_version: number;
  coverage: string;
  coverage_ratio: number;
  partial: boolean;
  rows?: { k: WhCanonicalValue[]; aggs: Record<string, WhFinalAggValue> }[];
  result?: Record<string, WhFinalAggValue>;
  warnings: WhEngineWarning[];
  perShard: WhPerShardEntry[];
  latency_ms: number;
  /** r121 OPT-1b (design §1.7): phase wall-clock decomposition — SUCCESS
   *  envelopes ONLY (OMITTED on error envelopes; injected post-assembly).
   *    * pre_chain_ms — ENTRYPOINT-THREADED: the fence consult + atomic
   *      directory read precede the in-core clock, so the entrypoint measures
   *      them and passes ExecuteArgs.timings.preChainMs (0 when absent).
   *    * handshake_ms — the sampled inventory-backstop block duration
   *      (0 when the sampler did not fire — the fold's steady state).
   *    * fanout_ms — the measured fanout BLOCK duration (the runFanout await
   *      wall over the worker pool, NOT max(perShard)). */
  phases?: { pre_chain_ms: number; handshake_ms: number; fanout_ms: number };
}

// ---------- r121 OPT-1b: sampled inventory-backstop sampler (design §1.5) ----------

/**
 * K_SAMPLING — the pinned sample bucket of the OPT-1b handshake fold. The
 * per-query inventory GET fires iff `(sha256(qid utf8)[0] & 15) ===
 * K_SAMPLING` — a deterministic 1-in-16 function of the request qid (no
 * isolate state, recycle-safe; qid is client-supplied, so clients CAN steer
 * out of the bucket — P3, accepted by design). Named constant (never a
 * literal inside the predicate) so the battery can pin the value and mutate
 * it RED; 7 is the picked bucket — any fixed 0..15 value keeps the 1-in-16
 * rate, 7 pins the audit vectors.
 */
export const K_SAMPLING = 7;

/**
 * The OPT-1b sampling predicate: FIRST byte of sha256(qid utf8) & 15.
 * Web Crypto (`crypto.subtle.digest`) — a pure computation of the qid, not
 * I/O and not env: the purity island law is untouched (async — callers
 * await it; the enclosing executeWhQuery scope is async).
 */
export async function qidSampleBucket(qid: string): Promise<number> {
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(qid));
  return new Uint8Array(digest)[0] & 15;
}

const IDENT_RE = /^[a-zA-Z_][a-zA-Z0-9_]*$/;
const AGG_OPS = new Set(['sum', 'count', 'min', 'max', 'avg']);
const WHERE_OPS = new Set(['eq', 'neq', 'gt', 'gte', 'lt', 'lte', 'between', 'is']);

// ---------- r47 LSN algebra (wh-contract r47 errata; BigInt, never text/Number) ----------

/** pg_lsn TEXT: two <=8-hex-digit halves, case-insensitive, NO trim. */
export const PG_LSN_RE = /^[0-9A-Fa-f]{1,8}\/[0-9A-Fa-f]{1,8}$/;

/** Parse 'X/Y' into (hi, lo) BigInts. Returns null on ANY violation (the
 *  caller fails closed — never guesses). */
export function parsePgLsn(s: unknown): [bigint, bigint] | null {
  if (typeof s !== 'string' || !PG_LSN_RE.test(s)) return null;
  const idx = s.indexOf('/');
  try {
    return [BigInt('0x' + s.slice(0, idx)), BigInt('0x' + s.slice(idx + 1))];
  } catch {
    return null;
  }
}

/** applied >= min ? true : false; null = unadjudicable (either side
 *  unparseable) — the caller treats null as fail-closed (lsn_unknown).
 *  NEVER compare LSN strings lexicographically: '0/9' > '0/A' as text while
 *  0x9 < 0xA numerically — the text order would wave a BEHIND replica
 *  through as read-your-writes (the exact violation the gate exists to
 *  prevent). BigInt mandatory: Number() loses precision above 2^53. */
export function lsnAtLeast(applied: string | null | undefined, min: string): boolean | null {
  const a = parsePgLsn(applied);
  const m = parsePgLsn(min);
  if (a === null || m === null) return null;
  if (a[0] !== m[0]) return a[0] > m[0];
  return a[1] >= m[1];
}

/** The r47-pinned coverage shape (wh-contract r47 errata; full-only law).
 *  null => covered set = the reference tables (the caller resolves
 *  is_reference); jsonb ARRAY of logical table names => covered = exactly
 *  that array (FULL table coverage each — a row-filtered/windowed partial
 *  set is NEVER eligible: a windowed aggregate served from a partially-
 *  covered replica merges a partial SUM as complete, the read-plane back
 *  door into the §5.2 wrong-SUM class); ANY other shape => ∅ fail-closed. */
export function geoCoverageCovers(
  coverage: unknown,
  table: string,
  tableIsReference: boolean | null,
): boolean {
  if (coverage === null || coverage === undefined) return tableIsReference === true;
  if (Array.isArray(coverage)) {
    for (const t of coverage) {
      if (typeof t !== 'string') return false; // malformed member => ∅ fail-closed
    }
    return (coverage as string[]).includes(table);
  }
  return false; // object/number/string/bool => ∅ fail-closed
}

function malformed(msg: string): never {
  throw new WhEngineError('malformed', msg);
}

/** r44 §4.4 est_rows lane: the directory's row_estimate when the row carries
 *  a valid non-negative integer, else 0 (no estimate available). */
function directoryRowEstimate(row: WhDirectoryRow): number {
  return typeof row.row_estimate === 'number' && Number.isInteger(row.row_estimate) && row.row_estimate >= 0
    ? row.row_estimate
    : 0;
}

/** r44: partial-derived est_rows for the truncation gate — the partial's own
 *  rowCount (the rows it carries) when it is a valid non-negative integer,
 *  else null (the caller falls back to the outcome's estimate). */
function partialRowCount(rawEnvelope: unknown): number | null {
  if (rawEnvelope === null || typeof rawEnvelope !== 'object') return null;
  const p = (rawEnvelope as Record<string, unknown>).partial;
  if (p === null || typeof p !== 'object') return null;
  const rc = (p as Record<string, unknown>).rowCount;
  return typeof rc === 'number' && Number.isInteger(rc) && rc >= 0 ? rc : null;
}

// ---------- §4.3 request parse ----------
export function parseWhEngineRequest(raw: unknown): WhEngineRequest {
  if (raw === null || typeof raw !== 'object') malformed('request body must be an object');
  const r = raw as Record<string, unknown>;
  if (r.v !== 1) malformed(`v must be the literal 1 (got ${JSON.stringify(r.v) ?? 'undefined'}) — never best-effort parse an unknown version`);
  if (typeof r.qid !== 'string' || r.qid.length === 0) malformed('qid must be a non-empty string');
  if (typeof r.table !== 'string' || !IDENT_RE.test(r.table)) malformed(`table must be a plain identifier (got ${JSON.stringify(r.table) ?? 'undefined'})`);
  if (r.coverage_mode !== undefined && r.coverage_mode !== null) {
    if (r.coverage_mode !== 'best_effort' && r.coverage_mode !== 'fail_fast') {
      malformed(`coverage_mode must be best_effort|fail_fast (got ${JSON.stringify(r.coverage_mode)})`);
    }
  }
  if (r.directory_snapshot !== undefined && typeof r.directory_snapshot !== 'string') {
    malformed('directory_snapshot must be an opaque string');
  }

  // ---- r47 read-plane extension (wh-contract r47 errata; additive) ----
  // null = absent (the coverage_mode null-lenient precedent); unknown enum
  // values are a hard 400 (frozen-contract law: never best-effort parse).
  let readPlane: 'primary' | 'replica' | undefined;
  if (r.read_plane !== undefined && r.read_plane !== null) {
    if (r.read_plane !== 'primary' && r.read_plane !== 'replica') {
      malformed(`read_plane must be primary|replica (got ${JSON.stringify(r.read_plane)})`);
    }
    readPlane = r.read_plane;
  }
  // min_lsn REQUIRES read_plane 'replica': on the primary plane a committed-
  // LSN floor is vacuous (the primary IS the write head) — an enforced-
  // looking no-op would be planner-dishonest; the reject surfaces caller
  // routing bugs. Format: pg_lsn TEXT (two <=8-hex-digit halves), never a
  // number (>2^53 mangles) — the §2.0 text-path law.
  let minLsn: string | undefined;
  if (r.min_lsn !== undefined && r.min_lsn !== null) {
    if (readPlane !== 'replica') {
      malformed('min_lsn requires read_plane:"replica" (a primary-plane floor is vacuous — the primary IS the write head)');
    }
    if (typeof r.min_lsn !== 'string' || !PG_LSN_RE.test(r.min_lsn)) {
      malformed(`min_lsn must be pg_lsn text 'X/Y' (two <=8-hex-digit halves; got ${JSON.stringify(r.min_lsn) ?? 'undefined'})`);
    }
    minLsn = r.min_lsn;
  }

  const q = r.query;
  if (q === null || q === undefined) malformed('query is required');
  if (typeof q !== 'object' || Array.isArray(q)) malformed('query must be an object');
  const query = q as Record<string, unknown>;

  // select
  const sel = query.select;
  if (!Array.isArray(sel) || sel.length === 0) malformed('query.select must be a non-empty array');
  const select: WhSelectEntry[] = [];
  for (const s of sel) {
    if (s === null || typeof s !== 'object' || Array.isArray(s)) malformed('select entries must be objects');
    const e = s as Record<string, unknown>;
    if (!AGG_OPS.has(String(e.op))) malformed(`select op must be one of sum|count|min|max|avg (got ${JSON.stringify(e.op) ?? 'undefined'})`);
    if (e.alias !== undefined && typeof e.alias !== 'string') malformed('select alias must be a string');
    if (e.op === 'count') {
      if (e.col !== undefined && (typeof e.col !== 'string' || !IDENT_RE.test(e.col))) malformed('count col must be a plain identifier');
      select.push({ op: 'count', ...(e.col !== undefined ? { col: e.col as string } : {}), ...(e.alias !== undefined ? { alias: e.alias as string } : {}) });
    } else {
      if (typeof e.col !== 'string' || !IDENT_RE.test(e.col)) malformed(`${String(e.op)} requires a col (bare projection is not v0)`);
      select.push({ op: e.op as 'sum' | 'min' | 'max' | 'avg', col: e.col, ...(e.alias !== undefined ? { alias: e.alias as string } : {}) });
    }
  }

  // where
  let where: WhWhereEntry[] | undefined;
  if (query.where !== undefined && query.where !== null) {
    if (!Array.isArray(query.where)) malformed('query.where must be an array');
    where = [];
    for (const w of query.where) {
      if (w === null || typeof w !== 'object' || Array.isArray(w)) malformed('where entries must be objects');
      const e = w as Record<string, unknown>;
      if (typeof e.col !== 'string' || !IDENT_RE.test(e.col)) malformed('where col must be a plain identifier');
      if (!WHERE_OPS.has(String(e.op))) malformed(`where op must be one of eq|neq|gt|gte|lt|lte|between|is (got ${JSON.stringify(e.op) ?? 'undefined'}; in-lists are not v0)`);
      if (!('value' in e)) malformed(`where ${String(e.op)} requires a value`);
      if (e.op === 'between') {
        if (!Array.isArray(e.value) || (e.value as unknown[]).length !== 2) malformed('where between requires a 2-element [lo, hi] array');
      }
      if (e.op === 'is' && e.value !== null && e.value !== true && e.value !== false) {
        malformed('where is accepts only null|true|false');
      }
      where.push({ col: e.col, op: e.op as WhWhereEntry['op'], value: e.value });
    }
  }

  // groupBy (duplicates checked at plan build where the plan context lives)
  let groupBy: string[] | undefined;
  if (query.groupBy !== undefined && query.groupBy !== null) {
    if (!Array.isArray(query.groupBy)) malformed('query.groupBy must be an array of column names');
    for (const g of query.groupBy) {
      if (typeof g !== 'string' || !IDENT_RE.test(g)) malformed('query.groupBy entries must be plain identifier strings');
    }
    groupBy = query.groupBy as string[];
  }

  // planner honesty: non-neutral clauses reject, naming the clause
  if (query.having !== undefined && query.having !== null) malformed('HAVING is not supported in v0 — rejected at plan time (scatter §1.2 #4: never silently mis-merge)');
  if (query.orderBy !== undefined && query.orderBy !== null) {
    if (!Array.isArray(query.orderBy) || (query.orderBy as unknown[]).length > 0) {
      malformed('ORDER BY is not supported in v0 (empty/null accepted) — rejected at plan time');
    }
  }
  if (query.offset !== undefined && query.offset !== null && query.offset !== 0) {
    malformed('OFFSET > 0 is not supported in v0 (409 page_unavailable class) — rejected at plan time');
  }
  if (query.distinct !== undefined && query.distinct !== null && query.distinct !== false) {
    malformed('DISTINCT is not supported in v0 — rejected at plan time');
  }

  // limit: absent => not kept; null => kept (no truncation); integer >= 0 => kept
  let limit: number | null | undefined;
  if (query.limit !== undefined) {
    if (query.limit !== null && (typeof query.limit !== 'number' || !Number.isInteger(query.limit) || (query.limit as number) < 0)) {
      malformed('query.limit must be a non-negative integer or null');
    }
    limit = query.limit as number | null;
  }

  return {
    v: 1,
    qid: r.qid,
    table: r.table,
    query: {
      select,
      ...(where ? { where } : {}),
      ...(groupBy ? { groupBy } : {}),
      ...(limit !== undefined ? { limit } : {}),
    },
    coverage_mode: (r.coverage_mode as 'best_effort' | 'fail_fast') ?? 'best_effort',
    ...(r.directory_snapshot !== undefined ? { directory_snapshot: r.directory_snapshot as string } : {}),
    ...(readPlane !== undefined ? { read_plane: readPlane } : {}),
    ...(minLsn !== undefined ? { min_lsn: minLsn } : {}),
  };
}

// ---------- merge-plan build ----------
export function buildMergePlan(
  req: WhEngineRequest,
  types: { columnTypes: Record<string, string>; columnScales?: Record<string, number> },
): WhMergePlan {
  const { columnTypes, columnScales } = types;
  const plan: WhMergePlan = { table: req.table, aggs: {} };

  // groupKeys first (schema-mismatch / unknown col => malformed)
  if (req.query.groupBy !== undefined) {
    const seen = new Set<string>();
    const gks: WhColumnPlan[] = [];
    for (const col of req.query.groupBy) {
      if (seen.has(col)) malformed(`duplicate groupBy column '${col}'`);
      seen.add(col);
      const t = columnTypes[col];
      if (t !== 'text' && t !== 'int8' && t !== 'numeric' && t !== 'timestamptz') {
        malformed(`groupBy column '${col}' is not in the table's column type map`);
      }
      gks.push({ col, type: t });
    }
    if (gks.length > 0) plan.groupKeys = gks;
  }

  const used = new Set<string>();
  for (const s of req.query.select) {
    const name = s.alias ?? (s.op === 'count' && s.col === undefined ? 'count(*)' : `${s.op}(${s.col as string})`);
    if (used.has(name)) malformed(`duplicate aggregate name '${name}' (alias collision)`);
    used.add(name);
    if (s.op === 'count' && s.col === undefined) {
      plan.aggs[name] = { op: 'count' };
      continue;
    }
    const col = s.col as string;
    const t = columnTypes[col];
    if (t !== 'int8' && t !== 'numeric' && t !== 'text' && t !== 'timestamptz') {
      malformed(`aggregate column '${col}' is not in the table's column type map`);
    }
    // merge law (r38 review P2-4) surfaced as a 400: PG has no sum(text) —
    // numeric aggregates require an int8|numeric carrier.
    if (s.op !== 'count' && t !== 'int8' && t !== 'numeric') {
      malformed(`aggregate ${s.op} over ${t} column '${col}' is not supported (PG has no ${s.op}(${t}))`);
    }
    const colPlan: WhColumnPlan = t === 'numeric'
      ? { col, type: 'numeric', scale: columnScales?.[col] ?? 0 }
      : { col, type: t as 'int8' | 'text' | 'timestamptz' };
    plan.aggs[name] = { op: s.op, col, colPlan };
  }
  return plan;
}

// ---------- E11 conservative shard pruning ----------
export interface SelectOpts {
  shardKeyColumn: string;
  shardKeyType: 'none' | 'hash' | 'range' | 'time';
  shardKeyPlan?: WhColumnPlan;
  hashSlotFn?: (value: unknown) => number;
}

export function selectShards(
  directoryRows: WhDirectoryRow[],
  where: WhWhereEntry[] | undefined,
  opts: SelectOpts,
): WhDirectoryRow[] {
  const keyPreds = (where ?? []).filter((w) => w.col === opts.shardKeyColumn);

  if (opts.shardKeyType === 'hash') {
    const eq = keyPreds.find((p) => p.op === 'eq');
    if (eq && opts.hashSlotFn) {
      let slot: number;
      try {
        slot = opts.hashSlotFn(eq.value);
      } catch {
        return directoryRows.slice(); // fn failure = cannot route = conservative all
      }
      return directoryRows.filter((r) => r.hash_slot === slot);
    }
    return directoryRows.slice(); // no eq / no fn / non-key predicate => all
  }

  // range/time: conservative closed-interval intersection (E11: boundary-
  // inclusive on ANY overlap; gt/lt map to closed [v,·)/ (·,v] intervals —
  // the boundary-touching shard is always included).
  if (opts.shardKeyType === 'range' || opts.shardKeyType === 'time') {
    if (!opts.shardKeyPlan) throw new TypeError('selectShards: range/time pruning requires shardKeyPlan');
    const plan = opts.shardKeyPlan;
    let lo: WhCanonicalValue | null = null; // null = unbounded
    let loSet = false;
    let hi: WhCanonicalValue | null = null;
    let hiSet = false;
    for (const p of keyPreds) {
      if (p.op === 'between') {
        const [a, b] = p.value as [unknown, unknown];
        let va: WhCanonicalValue;
        let vb: WhCanonicalValue;
        try {
          va = canonicalizeValue(a, plan);
          vb = canonicalizeValue(b, plan);
        } catch (err) {
          if (err instanceof WhCanonicalizeError) malformed(`shard-key between value not canonicalizable for ${plan.type}: ${(err as Error).message}`);
          throw err;
        }
        if (compareCanonical(va, vb, plan, false) > 0) malformed('between bounds reversed');
        if (!loSet || (lo !== null && va !== null && compareCanonical(va, lo, plan, false) < 0)) {
          loSet = true; lo = va;
        }
        if (!hiSet || (hi !== null && vb !== null && compareCanonical(vb, hi, plan, false) > 0)) {
          hiSet = true; hi = vb;
        }
        continue;
      }
      let v: WhCanonicalValue;
      try {
        v = canonicalizeValue(p.value, plan);
      } catch (err) {
        if (err instanceof WhCanonicalizeError) malformed(`shard-key predicate value not canonicalizable for ${plan.type}: ${(err as Error).message}`);
        throw err;
      }
      if (p.op === 'eq') {
        loSet = true; lo = v;
        hiSet = true; hi = v;
      } else if (p.op === 'gte' || p.op === 'gt') {
        if (!loSet || (lo !== null && v !== null && compareCanonical(v, lo, plan, false) < 0)) {
          loSet = true; lo = v;
        }
      } else if (p.op === 'lte' || p.op === 'lt') {
        if (!hiSet || (hi !== null && v !== null && compareCanonical(v, hi, plan, false) > 0)) {
          hiSet = true; hi = v;
        }
      }
      // neq/is: no interval contribution (conservative)
    }

    return directoryRows.filter((row) => {
      let min: WhCanonicalValue | null = null;
      let max: WhCanonicalValue | null = null;
      try {
        min = row.key_min === null ? null : canonicalizeValue(row.key_min, plan);
        max = row.key_max === null ? null : canonicalizeValue(row.key_max, plan);
      } catch {
        return true; // unparseable bound => cannot compare => do not prune
      }
      // span [min, max) vs closed [lo, hi]: touch iff min <= hi AND max >= lo
      if (hiSet && hi !== null && min !== null && compareCanonical(min, hi, plan, false) > 0) return false;
      if (loSet && lo !== null && max !== null && compareCanonical(max, lo, plan, false) < 0) return false;
      return true;
    });
  }

  return directoryRows.slice(); // shard_key_type none => all
}

// ---------- per-shard URL compile (kit-validated bare agg form) ----------
function aggExpr(name: string, pa: WhMergePlan['aggs'][string]): string {
  if (pa.op === 'count' && pa.col === undefined) return 'count()';
  return `${pa.op}(${pa.col as string})`;
}

export function compileShardUrl(shard: string, plan: WhMergePlan, where?: WhWhereEntry[]): string {
  const parts: string[] = [];
  const sel: string[] = [];
  for (const gk of plan.groupKeys ?? []) sel.push(gk.col);
  for (const [name, pa] of Object.entries(plan.aggs)) sel.push(aggExpr(name, pa));
  parts.push(`select=${sel.join(',')}`);
  for (const w of where ?? []) {
    if (w.op === 'between') {
      const [a, b] = w.value as [unknown, unknown];
      parts.push(`${w.col}=gte.${encodeURIComponent(String(a))}`);
      parts.push(`${w.col}=lte.${encodeURIComponent(String(b))}`);
    } else if (w.op === 'is') {
      parts.push(`${w.col}=is.${w.value === null ? 'null' : w.value ? 'true' : 'false'}`);
    } else {
      parts.push(`${w.col}=${w.op}.${encodeURIComponent(String(w.value))}`);
    }
  }
  return `https://${shard}.supabase.co/rest/v1/${plan.table}?${parts.join('&')}`;
}

/** r69 (§3.2 D1): the §6.1-pinned wh_query RPC URL — the byte-pinned call
 *  shape (`POST https://<ref>.supabase.co/rest/v1/rpc/wh_query`). NO change
 *  to compileShardUrl: the select-shape URL stays the fan-out target identity
 *  (byte-pinned surface, tests, snapshot mode). Pure. */
export function compileShardRpcUrl(shard: string): string {
  return `https://${shard}.supabase.co/rest/v1/rpc/wh_query`;
}

// ---------- warning classification (§5.1 + stamped-header law) ----------

// r69 (AM-5/AM-12/OQ-6): the shard-side WH[0-9]{3} body code (WH400/401/402
// on the 400-class; WH403/WH500 shard-integrity alarms on 5xx-with-json) is
// a DETAIL CARRIER only — the class decision stays status/header-based (r2
// law; the real fetcher parses it off the error body under a strict shape
// guard and stamps it on the incoming warning's `code`). HERE it is lifted
// onto the outcome's `detail` by ONE additive post-branch step at the
// classify convergence point, covering BOTH the 5xx arm (which returns
// BEFORE the generic-http arm) and the generic-http arm (which would
// otherwise eat the code) — F-N2. Branch order and every existing detail
// assignment untouched; byte-identical when no WH code is present (no
// pre-r69 fetcher ever emits one).
const WH_CODE_RE = /^WH[0-9]{3}$/;

export function classifyFetchFailure(
  f: { code?: string; httpStatus?: number; stamped?: boolean },
): { code: string; stamped?: boolean; detail?: string } {
  let out: { code: string; stamped?: boolean; detail?: string };
  if (f.httpStatus === 402) {
    out = { code: 'http_402' };
  } else if (f.httpStatus === 429) {
    out = { code: 'http_429' };
  } else if (typeof f.httpStatus === 'number' && f.httpStatus >= 500 && f.httpStatus <= 599) {
    out = f.stamped ? { code: 'http_5xx', stamped: true } : { code: 'http_5xx', stamped: false };
  } else if (f.code === 'network' || f.code === 'timeout' || f.code === 'abort_on_oversize') {
    out = { code: f.code };
  } else if (typeof f.httpStatus === 'number') {
    out = { code: 'excluded', detail: `http ${f.httpStatus}` };
  } else if (f.code !== undefined) {
    out = { code: 'excluded', detail: f.code };
  } else {
    out = { code: 'excluded' };
  }
  // r69 AM-5/F-N2: the WH-code detail lift (both arms; order untouched).
  if (f.code !== undefined && WH_CODE_RE.test(f.code)) out.detail = f.code;
  // r121 OPT-1b (design §1.3a, LOAD-BEARING): the shard-side per-call
  // template refusal — WH400 (template missing OR state='draft',
  // 0015:302-317) and WH401 ('retired') — maps to the §5.2-exempt
  // `template_missing` class so the FOLDED eligibility path (no inventory
  // GET on unsampled calls) still degrades+warns instead of fail_fast-500ing
  // the refusal class. `detail` KEEPS the WH code (diagnostic value; the
  // mapped exclusion record is battery-pinned). SCOPED to the generic-4xx /
  // code-only `excluded` arms (the shard emits WH400/401 on the 400 class):
  // a WH body code riding a 402/429/5xx/network class keeps its TRANSPORT
  // class — never re-masked (more-honest law, design §1.3a). WH402
  // (hash-shape) and WH403 (registry integrity) stay non-exempt.
  if (out.code === 'excluded' && (f.code === 'WH400' || f.code === 'WH401')) {
    return { code: 'template_missing', detail: f.code };
  }
  return out;
}

// ---------- r44 consumption gates (design §6.3 F2 + F14) ----------
export interface PartialGateRejection {
  code: 'merge_mismatch' | 'truncated_groupby';
  detail: string;
}

/**
 * Pre-merge consumption gates over the RAW wire envelope (wh_merge never
 * sees a rejected partial):
 *
 *  [F2] truncation gate — a `groupby`-class partial (kind 'grouped') carrying
 *  the truncation sentinel is NEVER silently merged: group-trimming makes
 *  per-group Σ incomplete (the wrong-SUM class — merged totals silently miss
 *  whole shards' groups). `truncated` is the §6.2 wh_query sentinel; `more`
 *  is the v0 §2.0 twin (wh_merge P2-1 rejects it as envelope_invalid — this
 *  gate preempts it with the F2-pinned code). topk partials are trimmed BY
 *  DESIGN (K-way heap, E5) and EXEMPT; scalar partials carry no sentinel
 *  (§6.2 F7).
 *
 *  [F14] merge-op subset gate — a partial that CLAIMS a template_hash must
 *  resolve in the engine manifest and the plan's derived ops must be a
 *  SUBSET of the template's declared merge_ops (SUBSET, not equality —
 *  exact equality would falsely reject a topk-only plan against W4's
 *  ["topk","raw_rows"]). Violation ⇒ 422-class merge_mismatch, shard
 *  degraded, never silently mis-merged (contract §4.4). A partial with NO
 *  template claim (the legacy §2.0 select-path shape) skips this gate.
 *
 * Returns null when the partial may proceed to wh_merge. Never throws on
 * wire garbage — unparseable envelopes fall through to wh_merge's own
 * envelope_invalid law.
 */
export function gatePartialAgainstPlan(plan: WhMergePlan, rawEnvelope: unknown): PartialGateRejection | null {
  if (rawEnvelope === null || typeof rawEnvelope !== 'object' || Array.isArray(rawEnvelope)) return null;
  const e = rawEnvelope as Record<string, unknown>;
  const p = e.partial;
  if (p === null || typeof p !== 'object') return null;
  const partial = p as Record<string, unknown>;

  // F2 truncation gate (kind 'grouped' only — topk exempt, scalar no sentinel)
  if (partial.kind === 'grouped' && (partial.truncated === true || partial.more === true)) {
    return {
      code: 'truncated_groupby',
      detail: `grouped partial carries the truncation sentinel (${partial.truncated === true ? 'truncated' : 'more'}=true) — never silently merged (§6.3 F2)`,
    };
  }

  // F14 merge-op subset gate (only when the partial claims a template)
  if (e.template_hash !== undefined) {
    const row = typeof e.template_hash === 'string' && e.template_hash.length > 0
      ? manifestRowByHash(e.template_hash)
      : null;
    if (row === null) {
      return {
        code: 'merge_mismatch',
        detail: `partial template_hash ${JSON.stringify(e.template_hash) ?? 'undefined'} does not resolve in the engine manifest (§6.3 F14)`,
      };
    }
    const planOps = derivePlanMergeOps(plan);
    if (!planOpsSubset(planOps, row.merge_ops)) {
      return {
        code: 'merge_mismatch',
        detail: `plan ops [${planOps.join(',')}] is not a subset of template merge_ops [${row.merge_ops.join(',')}] for ${e.template_hash} (§6.3 F14 subset rule)`,
      };
    }
  }
  return null;
}

// ---------- r69 RPC plane (§2 D2 / §3.2 targets / §3.4 arm-5 adapter) ----------

/**
 * WH_RPC_ELIGIBLE_HASHES (r69 §2 OQ-4, F-N5 representation pin): the template
 * hashes whose agg inputs are NULL-GUARDED on the shard side (design
 * design_wh_query_rpc.md:329) and may therefore carry rpc — W1/W2/W3 (their
 * span params ride `($1->>'k' is null or ...)` guards). The W5-class
 * (`facts_blocks`, `dataset` unguarded) is EXCLUDED: `{}` would yield a
 * zero-row partial merging as a confident 200 — until a guard lands, a plan
 * whose only derived hash is W5's is a `plan_untemplated` 4xx pre-fan-out.
 * STATIC engine-core constant: the eligible hashes enumerated at authoring
 * time from the pinned manifest (ENGINE_TEMPLATE_MANIFEST, wh_handshake.ts —
 * the exact literal sha256 strings; NOT a manifest field, which would red
 * the manifest deep-equality statics wh_handshake_test.ts:393+).
 */
export const WH_RPC_ELIGIBLE_HASHES: readonly string[] = [
  'a934e7e062f59cff5a856afdc7aa743ec9be11c068c7e861ea856c36b40bdbfd', // W1_grouped_sum_count ["groupby","sum","count"]
  'a095adaa148253aee8d1cc8e976f01b3579beeea5082f3862df4a908c20b2659', // W2_scalar_minmax ["min","max","count_col"]
  'bca9dd2c591ed48a0fa5367179dd5deb1d752ed9141c23e6ad53083becf8ecac', // W3_scalar_avg_pair ["avg_pair"]
];

/**
 * rpcParams (r69 §3.2): the v1 pin — `{}` PLUS the request's `limit` when it
 * is a NUMBER (AM-7: explicit `limit:null` (no-truncation) is ABSENT —
 * mirroring the limitK precedent above; shard-side `p_params.limit=null` is
 * NEVER sent). NO where ever reaches p_params in v1 (AM-2: where-carrying
 * template plans are rejected 4xx pre-fan-out); span params (id_min/id_max)
 * stay DEFERRED (OQ-4 upheld-conditional — a span-to-params mapper is new
 * translation logic with zero drilled precedent, deferred to the span-pruning
 * wave). `plan` rides the signature for that wave's mapper; v1 never reads it.
 */
export function rpcParams(_plan: WhMergePlan, query: WhEngineRequest['query']): Record<string, unknown> {
  return typeof query.limit === 'number' ? { limit: query.limit } : {};
}

/** The matched TEMPLATE def view adaptWireEnvelope consumes — F-N1: kind +
 *  agg defs, not just its aggs (EngineTemplateRow satisfies it; the §4
 *  worked-example call shape — a bare agg map — degrades to wire-kind mode). */
export interface WireTemplateView {
  kind?: string;
  aggs: Record<string, { op: string; col?: string }>;
  merge_ops?: readonly string[];
}

/** The plan view the adapter maps INTO (WhMergePlan satisfies it): the
 *  output agg names are the PLAN's own keys (buildMergePlan naming or
 *  aliases — verbatim), so a plan agg the template cannot serve fails the
 *  whole adaptation to null (loud, never fabricated). */
export interface WirePlanView {
  aggs: Record<string, { op: string; col?: string }>;
}

/** op-equivalence for the (equivalent-op, col) match (F-N1 step 1/2): the
 *  plan lattice's count(col) carries op 'count' while the template encodings
 *  name the same aggregate 'count_col' (the W2 mapping law) — both match.
 *  col must be identical (count(*) has no col and never matches a col-scoped
 *  encoding). */
function aggEncodingMatches(
  planOp: string,
  planCol: string | undefined,
  encOp: string,
  encCol: string | undefined,
): boolean {
  if ((planCol ?? undefined) !== (encCol ?? undefined)) return false;
  return planOp === encOp || (planOp === 'count' && encOp === 'count_col');
}

/** The avg-fusion pair lookup (F-N1 step 3): the template's sum+count wire
 *  pair ON THE SAME COL, fused into the avg-partial shape wh_merge's
 *  avg-pair contract consumes — the EXACT pair keys `{s, c}` (wh_merge
 *  accMerge 'avg': s:null ⇔ c:0, §2.2 pair algebra). avg is NEVER served by
 *  a lone sum or a lone count. Gated on the template declaring 'avg_pair'
 *  (W3) when merge_ops is known — a same-col sum+count_col pair WITHOUT the
 *  avg_pair declaration (the W5-class shape) is a genuine sum+count_col
 *  encoding and stays unfused; merge_ops-unknown callers get the structural
 *  reading (only rpc-ELIGIBLE templates ever reach the adapter — W5-class
 *  plans are 4xx'd pre-fan-out, F-N5). */
function avgPairEncoding(
  template: WireTemplateView,
  col: string,
): { sumKey: string; countKey: string } | null {
  if (template.merge_ops !== undefined && !template.merge_ops.includes('avg_pair')) return null;
  let sumKey: string | null = null;
  let countKey: string | null = null;
  for (const [name, def] of Object.entries(template.aggs)) {
    if ((def.col ?? undefined) !== col) continue;
    if (def.op === 'sum') {
      if (sumKey === null) sumKey = name;
    } else if (def.op === 'count_col' || def.op === 'count') {
      if (countKey === null) countKey = name;
    }
  }
  return sumKey !== null && countKey !== null ? { sumKey, countKey } : null;
}

/**
 * adaptWireEnvelope — the FLAT §6.2 wire → nested WhPartialEnvelope adapter
 * (r69 §3.4 arm 5, AM-1/F-N1 algorithm). PURE. Returns the engine shape or
 * NULL (a plan aggregate the template cannot serve, or wire garbage — the
 * call site degrades to the loud excluded path; NEVER fabricated).
 *
 * Signature note (F-N1 grounds): F-N1 pins (wire, template) — template = the
 * matched TEMPLATE def (kind + agg defs). The algorithm's plan-side steps —
 * (2) plan-agg naming/matching, (5) the extra-agg DROP, no-match ⇒ null —
 * are plan-RELATIVE and cannot be implemented against the template alone
 * (the F14 SUBSET rule legitimately serves subset plans; emitting the
 * template's FULL agg set would die in wh_merge's P3 exact-set law), so the
 * plan view rides as the third argument. WhMergePlan satisfies WirePlanView.
 *
 * Algorithm (F-N1, all steps pinned):
 *  (1) op-equivalence count_col ≡ count (aggEncodingMatches);
 *  (2) each PLAN agg maps from the encoding whose (equivalent-op, col)
 *      matches — output names are the plan's own (buildMergePlan conventions:
 *      `sum(amount)`, `count(*)`, `count(amount)`, `avg(col)`, or aliases);
 *  (3) avg fusion from the template's sum+count pair on the same col into
 *      the `{s, c}` pair (avgPairEncoding — the wh_merge avg-pair contract);
 *  (4) scalar-kind remap: the verbatim `partial` body object's keys remap by
 *      the same rule (encoding names never survive into the engine shape);
 *  (5) extra-agg drop: only plan-consumed aggs are emitted (wire aggs with
 *      no plan consumer are dropped — wh_merge is plan-driven, P3);
 *  (6) wraps partial:{kind, groupKeys?, aggs, rows, rowCount, more} with
 *      more := wire.truncated (grouped: rowCount := wire.rowCount; scalar:
 *      the one-row wrap, rowCount 1);
 *  (7) wire-only fields (qc_class/encoding/template_timeout_ms/latencyMs)
 *      dropped; v/table/schema_version/template_hash carried top-level
 *      (F14 reads template_hash). `shard` is stamped by the CALL SITE (the
 *      wire carries none — §6.2 declared deviation). wh_merge stays
 *      byte-identical.
 */
export function adaptWireEnvelope(
  wire: unknown,
  template: WireTemplateView,
  plan: WirePlanView,
): Omit<WhPartialEnvelope, 'shard'> | null {
  if (wire === null || typeof wire !== 'object' || Array.isArray(wire)) return null;
  if (template === null || typeof template !== 'object' || template.aggs === null || typeof template.aggs !== 'object') return null;
  if (plan === null || typeof plan !== 'object' || plan.aggs === null || typeof plan.aggs !== 'object') return null;
  const w = wire as Record<string, unknown>;
  if (typeof w.table !== 'string' || typeof w.schema_version !== 'number') return null;
  // Wrap mode: the matched template's kind is the authority ('rows' = grouped
  // output, 'scalar'); absent => the wire's own kind. Anything else (topk
  // wires carry no plan-consumable aggregate set) fails null — loud.
  const kind = typeof template.kind === 'string'
    ? template.kind
    : typeof w.kind === 'string'
    ? w.kind
    : '';
  const mode: 'grouped' | 'scalar' | null =
    kind === 'rows' || kind === 'grouped' ? 'grouped'
    : kind === 'scalar' ? 'scalar'
    : null;
  if (mode === null) return null;
  // r69 a4battery FIX (mutant-dance catch, §6.2 F7 merge-semantic class): the
  // wire's OWN kind, when present, must denote the SAME merge class as the
  // resolved mode. A 'topk'-kind wire served through a grouped template's
  // algebra would merge its TRIMMED rows as complete groups — the exact
  // silent wrong-SUM class F2 exempts topk from (topk partials are trimmed
  // BY DESIGN) — so the mismatch fails CLOSED to null (loud excluded
  // upstream, never fabricated). The seeded fn emits 'grouped'|'topk'|'scalar'
  // only (0015 v_kind); 'rows' is the registry kind, never an envelope kind.
  if (typeof w.kind === 'string') {
    const wireMode: 'grouped' | 'scalar' | null =
      w.kind === 'grouped' ? 'grouped'
      : w.kind === 'scalar' ? 'scalar'
      : null; // 'topk' (and anything else) has NO adapter mode — fail closed
    if (wireMode !== mode) return null;
  }

  // Per-plan-agg serving map: plan agg name -> the wire row's `a` key(s).
  type Serving = { direct: string } | { avg: { sumKey: string; countKey: string } };
  const serving = new Map<string, Serving>();
  const decls: Record<string, { op: string; col?: string }> = {};
  for (const [name, pa] of Object.entries(plan.aggs)) {
    if (pa === null || typeof pa !== 'object' || typeof pa.op !== 'string') return null;
    decls[name] = pa.col === undefined ? { op: pa.op } : { op: pa.op, col: pa.col };
    if (pa.op === 'avg') {
      if (typeof pa.col !== 'string') return null;
      const pair = avgPairEncoding(template, pa.col);
      if (pair === null) return null; // never a lone sum / lone count
      serving.set(name, { avg: pair });
      continue;
    }
    let matched: string | null = null;
    for (const [encName, encDef] of Object.entries(template.aggs)) {
      if (aggEncodingMatches(pa.op, pa.col, encDef.op, encDef.col)) {
        matched = encName;
        break;
      }
    }
    if (matched === null) return null; // plan agg with NO encoding match ⇒ null (fail-closed)
    serving.set(name, { direct: matched });
  }

  // Remap one aggregate-value map: encoding keys -> plan names (steps 2-5).
  // Values pass through VERBATIM (wh_merge canonicalizes; the text path is
  // the exact carrier). A missing encoding key fails the adaptation — never
  // a fabricated empty contribution.
  const remapAggs = (rawA: unknown): Record<string, unknown> | null => {
    if (rawA === null || typeof rawA !== 'object' || Array.isArray(rawA)) return null;
    const src = rawA as Record<string, unknown>;
    const out: Record<string, unknown> = {};
    for (const [name, s] of serving) {
      if ('direct' in s) {
        if (!Object.prototype.hasOwnProperty.call(src, s.direct)) return null;
        out[name] = src[s.direct];
      } else {
        if (
          !Object.prototype.hasOwnProperty.call(src, s.avg.sumKey) ||
          !Object.prototype.hasOwnProperty.call(src, s.avg.countKey)
        ) return null;
        out[name] = { s: src[s.avg.sumKey], c: src[s.avg.countKey] };
      }
    }
    return out;
  };

  if (mode === 'scalar') {
    // scalar-kind wire: the single-row body object verbatim under `partial`
    // (0015:418-432) — remap, then the one-row wrap (step 6).
    const a = remapAggs(w.partial);
    if (a === null) return null;
    return {
      v: 1,
      table: w.table,
      schema_version: w.schema_version,
      ...(typeof w.template_hash === 'string' ? { template_hash: w.template_hash } : {}),
      partial: {
        kind: 'scalar',
        aggs: decls as WhPartialEnvelope['partial']['aggs'],
        rows: [{ k: [], a }],
        rowCount: 1,
        more: w.truncated as boolean,
      },
    };
  }

  if (!Array.isArray(w.rows)) return null;
  const rows: { k: unknown[]; a: Record<string, unknown> }[] = [];
  for (const r of w.rows) {
    if (r === null || typeof r !== 'object' || Array.isArray(r)) return null;
    const rr = r as Record<string, unknown>;
    if (!Array.isArray(rr.k)) return null;
    const a = remapAggs(rr.a);
    if (a === null) return null;
    rows.push({ k: rr.k as unknown[], a });
  }
  return {
    v: 1,
    table: w.table,
    schema_version: w.schema_version,
    ...(typeof w.template_hash === 'string' ? { template_hash: w.template_hash } : {}),
    partial: {
      kind: 'grouped',
      ...(Array.isArray(w.groupKeys) ? { groupKeys: w.groupKeys as string[] } : {}),
      aggs: decls as WhPartialEnvelope['partial']['aggs'],
      rows,
      rowCount: w.rowCount as number,
      more: w.truncated as boolean,
    },
  };
}

// ---------- fan-out (WINDOW pool + per-shard timeout, allSettled) ----------
export interface WhFanoutTarget {
  shard: string;
  url: string;
  /** r69 (§3.1 D1): present ⇒ the fetcher POSTs the §6.1 RPC shape; absent ⇒
   *  the residual select-shape GET (replica/geo-dispatch planes only). */
  rpc?: WhRpcSpec;
}

/** The REAL timer pair (production wiring in warehouse-engine/index.ts):
 *  Date.now clock + setTimeout race. Injected fakes replace it in tests. */
export function defaultWhEngineTimers(): WhEngineTimers {
  return {
    nowMs: () => Date.now(),
    startTimeout: (ms: number) => {
      let dispose = (): void => {};
      const promise = new Promise<'timeout'>((resolve) => {
        const id = setTimeout(() => resolve('timeout'), ms);
        dispose = () => clearTimeout(id);
      });
      return { promise, dispose };
    },
  };
}

export interface WhFanoutOutcome {
  shard: string;
  ok: boolean;
  latencyMs: number;
  estRows: number;
  warning?: { code: string; stamped?: boolean; detail?: string };
  error?: string;
  /** r69 (AM-1/F-N7): widened to the raw wire object on the RPC path — the
   *  consumption site adapts it (adaptWireEnvelope) before the gates. */
  envelope?: WhFanoutEnvelope;
}

export function runFanout(
  targets: WhFanoutTarget[],
  fetcher: WhShardFetcher,
  timers: WhEngineTimers,
  opts: { window?: number; shardTimeoutMs?: number },
): Promise<WhFanoutOutcome[]> {
  // Sync validation (the caller gets a synchronous TypeError, not a rejected
  // promise — programmer errors are not runtime outcomes).
  const window = opts.window ?? DEFAULT_WINDOW;
  if (!Number.isInteger(window) || window < 1 || window > MAX_WINDOW) {
    throw new TypeError(`fanout window must be an integer in [1, ${MAX_WINDOW}] (got ${window})`);
  }
  return runFanoutImpl(targets, fetcher, timers, window, opts.shardTimeoutMs ?? DEFAULT_SHARD_TIMEOUT_MS);
}

async function runFanoutImpl(
  targets: WhFanoutTarget[],
  fetcher: WhShardFetcher,
  timers: WhEngineTimers,
  window: number,
  timeoutMs: number,
): Promise<WhFanoutOutcome[]> {
  const outcomes: WhFanoutOutcome[] = new Array(targets.length);
  let next = 0;
  const effective = Math.min(window, targets.length);

  const worker = async (): Promise<void> => {
    for (;;) {
      const i = next++;
      if (i >= targets.length) return;
      const t = targets[i];
      const started = timers.nowMs();
      const timer = timers.startTimeout(timeoutMs);
      let outcome: WhFanoutOutcome;
      try {
        const raced = await Promise.race([
          fetcher(t.shard, t.url, t.rpc).then(
            (r) => ({ kind: 'result' as const, r }),
            (err: unknown) => ({ kind: 'throw' as const, err }),
          ),
          timer.promise.then(() => ({ kind: 'timeout' as const })),
        ]);
        if (raced.kind === 'timeout') {
          outcome = {
            shard: t.shard, ok: false, latencyMs: timers.nowMs() - started, estRows: 0,
            warning: { code: 'timeout' }, error: 'timeout',
          };
        } else if (raced.kind === 'throw') {
          const cls = classifyFetchFailure({ code: 'network' });
          outcome = {
            shard: t.shard, ok: false, latencyMs: timers.nowMs() - started, estRows: 0,
            warning: cls, error: cls.code,
          };
        } else if (raced.r.ok) {
          outcome = {
            shard: t.shard, ok: true, latencyMs: timers.nowMs() - started,
            // Raw-wire relay, verbatim (select path: engine shape; rpc path:
            // the isWhRpcWireShape-gated flat wire) — no shape claim added
            // here; deep validation is the consumption site's job
            // (adaptWireEnvelope + the §6.3 gates). Narrow relay cast only.
            estRows: raced.r.estRows ?? 0, envelope: raced.r.envelope as WhFanoutEnvelope,
          };
        } else {
          const cls = classifyFetchFailure(raced.r.warning);
          const estRows = raced.r.estRows ?? 0;
          outcome = {
            shard: t.shard, ok: false, latencyMs: timers.nowMs() - started, estRows,
            warning: cls, error: cls.code,
          };
        }
      } catch {
        // Belt-and-braces: anything escaping the race classifies as network.
        outcome = {
          shard: t.shard, ok: false, latencyMs: timers.nowMs() - started, estRows: 0,
          warning: { code: 'network' }, error: 'network',
        };
      } finally {
        timer.dispose();
      }
      outcomes[i] = outcome;
    }
  };

  await Promise.all(Array.from({ length: effective }, () => worker()));
  // A timed-out fetcher eventually settles (no cancellation) — swallow late
  // rejections so they never surface as unhandled.
  return outcomes;
}

// ---------- the pipeline ----------
export interface ExecuteArgs {
  req: WhEngineRequest;
  columnTypes: Record<string, string>;
  columnScales?: Record<string, number>;
  directoryRows: WhDirectoryRow[];
  shardKeyColumn: string;
  shardKeyType: 'none' | 'hash' | 'range' | 'time';
  directoryVersion: number;
  timers: WhEngineTimers;
  fetcher: WhShardFetcher;
  tableSchemaVersion?: number;
  hashSlotFn?: (value: unknown) => number;
  window?: number;
  shardTimeoutMs?: number;
  // r44 §5.2 handshake (engine ⇄ shard), wired only on the hasRealFetcher
  // path — the stub path stays reachable (no dep => no handshake, exactly the
  // pre-r44 pipeline). Tests stub the WhShardHandshake shape directly.
  handshake?: WhShardHandshake;
  // r44: the plan-referenced template hashes (wh_query RPC plane). Any hash
  // outside ENGINE_TEMPLATE_MANIFEST is a plan-time 4xx (plan honesty §6.3).
  // r47: the entrypoint passes the ENGINE-DERIVED set (deriveTemplateHashes,
  // wh_handshake) — never client-supplied (no request field exists; a client-
  // declared set would be a lying-client degrade knob).
  templateHashes?: readonly string[];
  // ---- r69 (AM-4/F-N8): the RPC-plane mode flag. DEFAULT ABSENT (false) —
  // the unset path is byte-identical (F-N4: rpcMode === true gates EXACTLY
  // THREE behaviors — (a) the D2 plan_untemplated check, (b) the §3.2 rpc
  // target construction, (c) the adaptWireEnvelope invocation (§3.4 arm 5);
  // with rpcMode false/absent targets never carry rpc, the adapter never
  // runs, and the D2 check is absent — Δ0 by construction). Purity law: the
  // core NEVER reads env — the wiring computes it from the WH_REAL_FETCHER
  // env-guard expression (the staged r60 flip lever) and threads it through
  // WhEngineDeps. Plane pin: the D2 check + rpc targets fire ONLY on the
  // placements/primary plane post-handshake branch — replica-plane and
  // geo-dispatch targets are out of D2 scope by construction (they skip the
  // handshake / never carry rpc; G5 keeps template plans off the replica).
  rpcMode?: boolean;
  // ---- r47 read-plane deps (wh-contract r47 errata; all optional — absent
  // deps on a read_plane:"replica" request fail closed to the primary with
  // geo_fallback_primary/geo_read_error; the primary plane NEVER consults
  // them, keeping the pre-r47 pipeline byte-identical) ----
  /** The FM config geo_mode value (null = unreadable): G1 requires exactly
   *  'spread' — the colocated flip window (~90s stamp tail + 60s discovery
   *  cache) must not route reads to a replica the config no longer
   *  advertises. */
  geoMode?: string | null;
  /** v_geo_directory reader (≤1-row population; the reader owns the limit(2)
   *  law-breakage tripwire). THROWS on read failure — the engine catches →
   *  degrade-not-throw fallback (design-a P2-4). */
  geoReader?: () => Promise<WhGeoDirectoryRow[]>;
  /** Lazy is_reference resolver for the queried table (warehouse_tables),
   *  called ONLY when the geo row's coverage is null; null/throw = fail
   *  closed (coverage_miss). */
  resolveTableReference?: () => Promise<boolean | null>;
  // ---- r49 B2 R3: the legacy primary-plane read-dispatch instruction ----
  // Absent/'placements' ⇒ the as-built placements population, byte-
  // identical. engine_local/remote ⇒ ONE target at `target` (see the type);
  // the instruction PREEMPTS the placements-only fates (the G-A tier_warm
  // throw and the empty-selection emptyResponse) because the placements are
  // simply the WRONG population once the primary moved (R3). The entrypoint
  // computes it from the combined fence read; never client-supplied.
  geoReadDispatch?: WhGeoReadDispatch;
  // ---- r121 OPT-1b (design §1.7): ENTRYPOINT-THREADED pre-chain timing.
  // The entrypoint measures its pre-engine chain (snapshot-replay probe +
  // atomic directory read + snapshot signing + the r49 fence consult) with
  // the SAME timers instance it passes below, and threads the total here;
  // the engine injects it post-assembly onto the success envelope's
  // phases.pre_chain_ms. Optional (absent ⇒ 0): legacy callers/tests
  // unchanged. The value is computed UPSTREAM and passed in — the core
  // never reads env and owns no clock outside args.timers (purity law).
  // r124 OPT-3 (design §1, audit A ⟫A-3): the OPTIONAL sub-spans — dir_ms
  // (the atomic directory read wall; 0 on the replayed path, never-ran=0)
  // and fence_ms (the r49 consult wall measured from its EARLY start, so
  // the dir overlap is subtracted honestly: pre_chain_ms >= max(dir_ms,
  // fence_ms) is the invariant). Spread CONDITIONALLY into phases (both
  // success sites) — absent keys when not threaded, keeping the exact
  // {pre_chain_ms, handshake_ms, fanout_ms} legacy shape green.
  timings?: { preChainMs: number; dirMs?: number; fenceMs?: number };
}

// ---------- r47 read-plane gate ladder (impl plan §2; first match wins) ----------

type GeoPlaneDecision =
  | { kind: 'replica'; row: WhGeoDirectoryRow }
  | { kind: 'fallback'; detail: string; replicaRef?: string };

/**
 * The G1..G6 gate ladder for read_plane:"replica" (G0 and G-A are handled by
 * the caller: G0 = absent/'primary' never enters; G-A tier_warm is
 * plane-invariant and already thrown). Every non-winning cell falls forward
 * to the PRIMARY plane with a machine detail token — never a 5xx, never a
 * silent primary, never a 503 (503 is a discovery-plane code).
 */
async function resolveGeoPlane(args: ExecuteArgs): Promise<GeoPlaneDecision> {
  // G1: mode gate — the config must advertise spread RIGHT NOW (the colocated
  // flip window: stale stamps + cached discovery must not route reads to a
  // replica the config no longer advertises).
  if (args.geoMode !== 'spread') {
    return { kind: 'fallback', detail: 'mode_colocated' };
  }
  // G2: the geo read degrades, never throws (design-a P2-4 — the principled
  // mirror of the throwing primary directory read). A missing dep is the same
  // outcome (dep absent/throwing = geo_read_error).
  if (args.geoReader === undefined) {
    return { kind: 'fallback', detail: 'geo_read_error' };
  }
  let rows: WhGeoDirectoryRow[];
  try {
    rows = await args.geoReader();
  } catch {
    return { kind: 'fallback', detail: 'geo_read_error' };
  }
  // G3: serving filter (the view already enforces fresh + serving|draining).
  // The index law (one_geo_replica_serving covers serving+draining in ONE
  // predicate) means a draining row ⇒ NO serving row anywhere — the two
  // empty-set causes collapse, but the detail names which the operator saw.
  const serving = rows.filter((r) => r.state === 'serving');
  if (serving.length === 0) {
    const draining = rows.find((r) => r.state === 'draining');
    return draining !== undefined
      ? { kind: 'fallback', detail: 'state_draining', ...(typeof draining.project_ref === 'string' && draining.project_ref !== '' ? { replicaRef: draining.project_ref } : {}) }
      : { kind: 'fallback', detail: 'no_serving_row' };
  }
  const row = serving[0];
  if (typeof row.project_ref !== 'string' || row.project_ref === '') {
    return { kind: 'fallback', detail: 'geo_read_error' }; // view drift — never dispatch to a nameless target
  }
  // G4: coverage gate (full-only law — the P0-1 wrong-SUM closure).
  let tableIsReference: boolean | null = null;
  if ((row.coverage === null || row.coverage === undefined) && args.resolveTableReference !== undefined) {
    try {
      tableIsReference = await args.resolveTableReference();
    } catch {
      tableIsReference = null; // fail closed
    }
  }
  if (!geoCoverageCovers(row.coverage, args.req.table, tableIsReference)) {
    return { kind: 'fallback', detail: 'coverage_miss', replicaRef: row.project_ref };
  }
  // G5: template plans never ride the replica plane — the replica's template
  // inventory is unaudited until the ddl-wave applies RPCs replica-first
  // (DDL is NOT replicated, geo doc §5.1). v0 replica plane = select-path only.
  if (args.templateHashes !== undefined && args.templateHashes.length > 0) {
    return { kind: 'fallback', detail: 'templates_not_replica_served', replicaRef: row.project_ref };
  }
  // G6: the LSN floor (adjudicated ONLY when the caller pinned min_lsn).
  // BigInt compare vs the heartbeat-stamped applied_lsn (same P-space WAL
  // coordinates — sound); null applied_lsn = nothing applied yet (fresh
  // copy) or unparseable = unadjudicable ⇒ fail closed. NO wait loop in v0.
  if (args.req.min_lsn !== undefined) {
    const verdict = lsnAtLeast(row.applied_lsn, args.req.min_lsn);
    if (verdict === null) {
      return { kind: 'fallback', detail: 'lsn_unknown', replicaRef: row.project_ref };
    }
    if (verdict === false) {
      return { kind: 'fallback', detail: 'lsn_behind', replicaRef: row.project_ref };
    }
  }
  // G6a/G6d: the replica wins — documented eventual consistency (min_lsn
  // absent) or RYW satisfied (min_lsn pinned and met).
  return { kind: 'replica', row };
}

export async function executeWhQuery(args: ExecuteArgs): Promise<WhEngineResponse> {
  const started = args.timers.nowMs();
  const plan = buildMergePlan(args.req, { columnTypes: args.columnTypes, columnScales: args.columnScales });

  // r44 plan honesty (design §6.3): a plan referencing a template hash that
  // is not in the engine manifest can never be executed honestly — 4xx at
  // PLAN time, before any network I/O (vacuous when no hashes declared).
  const missingInManifest = planTemplatesMissingInManifest(args.templateHashes);
  if (missingInManifest.length > 0) {
    malformed(`template hashes not in the engine manifest (plan honesty, §6.3): ${missingInManifest.join(', ')}`);
  }

  const shardKeyPlan = args.columnTypes[args.shardKeyColumn]
    ? {
      col: args.shardKeyColumn,
      type: args.columnTypes[args.shardKeyColumn] as WhColumnPlan['type'],
      ...(args.columnTypes[args.shardKeyColumn] === 'numeric'
        ? { scale: args.columnScales?.[args.shardKeyColumn] ?? 0 }
        : {}),
    }
    : undefined;
  const selected = selectShards(args.directoryRows, args.req.query.where, {
    shardKeyColumn: args.shardKeyColumn,
    shardKeyType: args.shardKeyType,
    ...(shardKeyPlan ? { shardKeyPlan } : {}),
    ...(args.hashSlotFn ? { hashSlotFn: args.hashSlotFn } : {}),
  });

  // r49 B2 R3: the legacy primary-plane READ dispatch instruction. Only
  // consumed off the replica plane (the replica's identity is the geo row —
  // the instruction is never consulted there); 'placements' = no instruction
  // at all (every guard below is a no-op ⇒ the pre-r49 pipeline, byte-
  // identical — pinned by the fence battery's unset regression).
  const geoDispatch = args.req.read_plane !== 'replica' &&
      args.geoReadDispatch !== undefined && args.geoReadDispatch.kind !== 'placements'
    ? args.geoReadDispatch
    : null;

  const emptyResponse = (warnings: WhEngineWarning[], perShard: WhPerShardEntry[]): WhEngineResponse => {
    const grouped = plan.groupKeys !== undefined;
    const base: WhEngineResponse = {
      v: 1,
      qid: args.req.qid,
      directory_version: args.directoryVersion,
      coverage: '0/0',
      coverage_ratio: 1,
      partial: false,
      warnings,
      perShard,
      latency_ms: args.timers.nowMs() - started,
    };
    if (grouped) base.rows = [];
    else base.result = finalizeScalarAggs(mergeScalarAggs(plan, []), plan);
    // r121 OPT-1b (design §1.7): an empty-selection response is still a
    // SUCCESS envelope — it carries phases (the handshake/fanout phases
    // never ran: 0/0; pre_chain_ms is still the entrypoint-measured span).
    // r124 OPT-3 (⟫A-3): the sub-spans spread CONDITIONALLY — absent keys
    // when not threaded (the exact legacy shape stays pinned green).
    base.phases = {
      pre_chain_ms: args.timings?.preChainMs ?? 0,
      handshake_ms: 0,
      fanout_ms: 0,
      ...(args.timings?.dirMs !== undefined ? { dir_ms: args.timings.dirMs } : {}),
      ...(args.timings?.fenceMs !== undefined ? { fence_ms: args.timings.fenceMs } : {}),
    };
    return base;
  };

  if (geoDispatch === null && args.directoryRows.length === 0) {
    // r40 P2-3: §4.6 — response always carries directory_version when known.
    // r47 G-A: PLANE-INVARIANT (evaluated BEFORE the geo gates) — the replica
    // replicates only P's hot set (rolled-off rows were DELETEd at P and the
    // deletes replicated), so it is equally incomplete vs the cold tier; the
    // 404 contract must not silently become a hot-only aggregate.
    // r49 R3 EXCEPTION: an engine_local/remote instruction PREEMPTS the 404 —
    // after R16's demote the placements drop out and the bare tier_warm 404
    // would misattribute the cause (the table is fine; the primary moved).
    throw new WhEngineError('tier_warm', "table's only placements are warm/cold and no hot copy serves (restore-on-demand is v2)", {
      directoryVersion: args.directoryVersion,
    });
  }

  // ---- r47 read-plane branch (wh-contract r47 errata; impl plan §2) ----
  // SINGLE-DISPATCH LAW: one request/one plane/one target population via ONE
  // total branch BEFORE target construction. The replica is addressed EXACTLY
  // like a shard (compileShardUrl + the §4.6 engine→shard plane auth — the
  // co-hosted law: replica.project_ref = the engine host's own ref, so the
  // fetcher's own service key resolves it) and is the ONLY target when it
  // wins. The geo row is NEVER mapped into WhDirectoryRow/tableRows (no span
  // columns — any mapping fabricates data). Span/hash pruning does NOT apply
  // on the replica plane (whole-table coverage).
  let replicaTarget: { shard: string; url: string } | null = null;
  let geoWarnings: WhEngineWarning[] = [];
  if (args.req.read_plane === 'replica') {
    const decision = await resolveGeoPlane(args);
    if (decision.kind === 'replica' && decision.row !== undefined) {
      replicaTarget = { shard: decision.row.project_ref, url: compileShardUrl(decision.row.project_ref, plan, args.req.query.where) };
    } else if (decision.kind === 'fallback' && decision.detail !== undefined) {
      // Fail-closed fall-forward to the primary plane (never a 5xx, never a
      // silent primary, never a 503 — 503 is a discovery-plane code).
      geoWarnings = [{
        shard: decision.replicaRef ?? 'geo_replica', // RESERVED pseudo-shard token when the ref is unknown
        code: 'geo_fallback_primary',
        est_rows: 0, // plane events never bias SUM/COUNT partials
        retried: false,
        detail: decision.detail,
      }];
    }
  }
  const onReplicaPlane = replicaTarget !== null;
  // r49 R3: the LOUD B2 token rides the engine-local read wire (never
  // silent — the response NAMES that the primary was served locally).
  if (geoDispatch !== null && geoDispatch.kind === 'engine_local') {
    geoWarnings = [geoDispatch.warning];
  }

  if (!onReplicaPlane && geoDispatch === null && selected.length === 0) {
    return emptyResponse(geoWarnings, []);
  }

  const warnings: WhEngineWarning[] = [...geoWarnings];
  const perShard: WhPerShardEntry[] = [];
  const okEnvelopes: WhPartialEnvelope[] = [];

  // ---- r44 §5.2 discovery/verification handshake → r121 OPT-1b FOLD ----
  // Plane scope UNCHANGED: only when the dep is wired (the hasRealFetcher
  // path — the stub path stays reachable) AND the request is on the PRIMARY
  // PLACEMENTS population (!onReplicaPlane && geoDispatch === null): the
  // replica's template inventory is unaudited until the ddl-wave applies
  // RPCs replica-first (geo doc §5.1 DDL-not-replicated law), so a template
  // plan NEVER rides the replica plane (G5) and r49 R3's engine-local/remote
  // target is the PRIMARY itself — the inventory gate is a placements-fan-
  // out mechanism and is skipped on both.
  //
  // r121 OPT-1b (design_r121_opt1b_handshake_fold.md §1): the PER-QUERY
  // inventory sweep is RETIRED from the critical path. Eligibility is
  // enforced shard-side per wh_query call (0015:302-317: a missing/retired/
  // draft template refuses WH400/WH401 BEFORE EXECUTE — an ineligible shard
  // costs no shard compute; the RT was happening anyway) and maps to the
  // exempt template_missing class at classifyFetchFailure. The inventory GET
  // survives ONLY as the SAMPLED AUDIT BACKSTOP below (1-in-16 by qid
  // bucket) + the manifest-side max_rows pre-refusal (F15 with ZERO
  // network). Guards beyond the plane: the EMPTY-HASH guard
  // (templateHashes.length > 0 — ends the vacuous select-path inventory
  // audit; untemplated plans carry no template exposure) and planRef.limitK
  // present. WHEN FIRED: today's fail-closed semantics UNCHANGED —
  // inventory GET per selected shard (parallel), checkHandshake eligibility,
  // ineligible ⇒ exclusion with warning (est_rows from the directory row),
  // fanoutRows = eligible-shards-only. WHEN NOT FIRED: ZERO inventory GETs.
  const handshake = args.handshake;
  let fanoutRows = selected;
  // r121 OPT-1b (design §1.7): handshake_ms — the sampled inventory-backstop
  // BLOCK duration (0 when the sampler did not fire; measured below).
  let handshakeMs = 0;
  // r69 (AM-6/OQ-8) plan-side §5.2 inputs. matchedHash is NOT collected per
  // shard anymore: the AM-6 law (matchedHash ≡ the FIRST derived hash —
  // deterministic plan order, inventory-INDEPENDENT) lets the rpc target
  // construction source args.templateHashes[0] directly (audit-B F10).
  const planRef: HandshakePlanRef = {
    templateHashes: args.templateHashes ?? [],
    limitK: typeof args.req.query.limit === 'number' ? args.req.query.limit : null,
  };
  if (!onReplicaPlane && geoDispatch === null) {
    // ---- r121 OPT-1b: manifest-side max_rows pre-refusal (design §1.2) ----
    let manifestRefused = false;
    // F15 preserved with ZERO network: a plan whose limit K strictly exceeds
    // the matched template's pinned ENGINE_TEMPLATE_MANIFEST.max_rows is
    // refused BEFORE any shard round trip (the old check read max_rows off
    // the inventory GET — now a 1-in-16 backstop, so the refusal moved to
    // the manifest the plan-honesty gate already trusts). Boundary: refuse
    // iff K > max_rows STRICTLY (== passes — the shard's sentinel still
    // re-caps at execution, defense in depth). Outcome shape = today's
    // max_rows_exceeded path EXACTLY: exempt-code exclusion warnings
    // (est_rows = the directory row estimate) + perShard error entries
    // (latencyMs 0 — no RT happened) + fanoutRows = [] ⇒ ZERO shard POSTs;
    // the empty merge assembles coverage 0/n. Never masks D2 below (the
    // plan_untemplated throw still wins) and never masks the mapped
    // template_missing class on unsampled calls (missing-in-manifest hashes
    // died at plan time). Every derived hash's manifest row must serve K —
    // with the pinned manifest (no overlapping merge_ops) at most one hash
    // derives per plan, so this degenerates to the hashes[0] check.
    if (planRef.limitK !== null && planRef.templateHashes.length > 0) {
      const k = planRef.limitK;
      const refused = planRef.templateHashes.some((h) => {
        const mrow = manifestRowByHash(h);
        return mrow !== null && mrow.max_rows < k;
      });
      if (refused) {
        for (const row of selected) {
          warnings.push({ shard: row.shard, code: 'max_rows_exceeded', est_rows: directoryRowEstimate(row), retried: false });
          perShard.push({ shard: row.shard, ok: false, latencyMs: 0, error: 'max_rows_exceeded' });
        }
        fanoutRows = [];
        manifestRefused = true;
      }
    }
    // ---- r121 OPT-1b: the SAMPLED inventory-audit backstop (design §1.5) ----
    // Fires iff sha256(qid utf8)[0] & 15 === K_SAMPLING (deterministic per
    // qid — no isolate state, recycle-safe; Web Crypto is a pure
    // computation, not I/O). Sampled-only drift signals (inventory-side
    // template_missing/schema_mismatch/max_rows_exceeded) keep today's
    // fail-closed shape; the audit is deliberately REACTIVE between samples
    // (per-call WH codes + the sampled sweep catch drift — accepted,
    // design §1.5). Mutually exclusive with the manifest pre-refusal above
    // (a refused plan fans out to nobody — the GET would be dead weight).
    if (
      !manifestRefused &&
      handshake !== undefined &&
      planRef.templateHashes.length > 0 &&
      planRef.limitK !== null &&
      (await qidSampleBucket(args.req.qid)) === K_SAMPLING
    ) {
      const hsStarted = args.timers.nowMs();
      const verdicts = await Promise.all(selected.map(async (row) => {
        const t0 = args.timers.nowMs();
        let inventory: TemplateInventoryRow[];
        try {
          inventory = await handshake.readTemplateInventory(row.shard);
        } catch {
          inventory = []; // belt-and-braces: a throwing dep fails closed (§5.2)
        }
        const latencyMs = args.timers.nowMs() - t0;
        return { row, latencyMs, result: checkHandshake(planRef, inventory, args.tableSchemaVersion) };
      }));
      const eligible: WhDirectoryRow[] = [];
      for (const v of verdicts) {
        if (v.result.eligible) {
          eligible.push(v.row);
          continue;
        }
        const code = v.result.warning?.code ?? 'template_missing'; // invariant: !eligible ⇒ warning present
        warnings.push({ shard: v.row.shard, code, est_rows: directoryRowEstimate(v.row), retried: false });
        perShard.push({ shard: v.row.shard, ok: false, latencyMs: v.latencyMs, error: code });
      }
      fanoutRows = eligible;
      handshakeMs = args.timers.nowMs() - hsStarted;
    }
  }

  // ---- r69 D2 (§2 AM-2/AM-4/OQ-4/F-N5): RPC-plane plan honesty, pre-fan-out.
  // Fires ONLY here — the placements/primary plane post-gate branch (r121
  // OPT-1b: "post-handshake" renamed — the per-query handshake is folded;
  // the sampled backstop + manifest pre-refusal above are its successors) —
  // and ONLY when rpcMode is on (F-N4: with rpcMode absent this block does
  // not exist and the pipeline below is byte-identical). Rejects: hashless
  // plans; where-carrying plans (the RPC ignores where — a where-filtered
  // question answered by a full-table aggregate is the silent-wrongness
  // class); plans whose only derived hash ∉ WH_RPC_ELIGIBLE_HASHES (the W5-
  // class unguarded-template rule). The 4xx message lists table +
  // derived_hashes.length + the template-backed requirement — never
  // client-facing internals. AM-3: the entrypoint's status ladder maps the
  // code to 400 explicitly.
  let rpcSpecByShard: Map<string, WhRpcSpec> | null = null;
  if (args.rpcMode === true && !onReplicaPlane && geoDispatch === null) {
    const derived = args.templateHashes ?? [];
    const whereCount = args.req.query.where?.length ?? 0;
    if (
      derived.length === 0 ||
      whereCount > 0 ||
      derived.every((h) => !WH_RPC_ELIGIBLE_HASHES.includes(h))
    ) {
      throw new WhEngineError(
        'plan_untemplated',
        `plan_untemplated: table '${plan.table}' derives ${derived.length} template hash(es) — the wh_query RPC plane executes template-backed plans only (rpc-eligible templates, no where-filters); the plan is rejected pre-fan-out`,
      );
    }
    rpcSpecByShard = new Map();
    // r121 OPT-1b (design §1.3c / audit-B F10): the rpc target hash is
    // sourced DIRECTLY from the first derived plan hash. AM-6 law
    // (wh_handshake.ts:54-62): on an eligible verdict matchedHash ≡ the
    // FIRST derived hash — deterministic plan order, INVENTORY-INDEPENDENT —
    // so the per-shard handshake-verdict map was pure bookkeeping, and its
    // orphaned "rpc target construction without a handshake-matched template
    // hash" internal throw would now fire on every UNSAMPLED call (the
    // verdict collection is a 1-in-16 backstop). DELETED. Multi-hash
    // strictness assumption (pinned): the pinned ENGINE_TEMPLATE_MANIFEST
    // has no overlapping merge_ops, so a plan derives at most ONE template
    // hash — hashes[0]-only sourcing cannot under-serve a multi-template
    // requirement (D2 above already guaranteed derived.length > 0 here). If
    // a future manifest ever carries overlapping merge_ops, re-widen here
    // AND in the manifest-side max_rows pre-refusal above.
    const matched = derived[0];
    for (const row of fanoutRows) {
      rpcSpecByShard.set(row.shard, {
        p_template_hash: matched,
        p_params: rpcParams(plan, args.req.query),
      });
    }
  }

  // r47 single-dispatch: exactly ONE target population — the replica alone
  // (coverage denominator 1), the r49 R3 override target alone (denominator
  // 1), or the primary selections. NEVER both. r69 (§3.2): on the rpcMode
  // placements plane the selections carry the §6.1 rpc target (url = the
  // RPC path; D2 above guaranteed a template-backed plan + a matched hash);
  // with rpcSpecByShard null (rpcMode absent — F-N4) the select-shape target
  // is built byte-identically.
  const targets = onReplicaPlane && replicaTarget !== null
    ? [replicaTarget]
    : geoDispatch !== null
    ? [{ shard: geoDispatch.target, url: compileShardUrl(geoDispatch.target, plan, args.req.query.where) }]
    : fanoutRows.map((row) => {
        const rpc = rpcSpecByShard?.get(row.shard);
        return rpc !== undefined
          ? { shard: row.shard, url: compileShardRpcUrl(row.shard), rpc }
          : { shard: row.shard, url: compileShardUrl(row.shard, plan, args.req.query.where) };
      });
  // r121 OPT-1b (design §1.7): fanout_ms = the measured fanout BLOCK
  // duration — the runFanout await wall (worker-pool scheduling + ALL shard
  // round trips), NOT max(perShard): the block wall keeps the phases
  // decomposition additive under worker-pool queueing (a per-shard max would
  // hide the queue wait the wall exposes).
  const fanoutStarted = args.timers.nowMs();
  const outcomes = await runFanout(targets, args.fetcher, args.timers, {
    ...(args.window !== undefined ? { window: args.window } : {}),
    ...(args.shardTimeoutMs !== undefined ? { shardTimeoutMs: args.shardTimeoutMs } : {}),
  });
  const fanoutMs = args.timers.nowMs() - fanoutStarted;

  for (const o of outcomes) {
    if (o.ok && o.envelope) {
      // r69 (§3.4 arm 5, AM-1/F-N4): on the RPC plane the fetcher returned
      // the RAW flat §6.2 wire envelope — adapt it to the nested engine shape
      // HERE, before the untouched gates (schema/F2/F14/wh_merge). rpcMode
      // gates the invocation EXACTLY: with rpcMode absent the envelope is
      // already the engine shape and the adapter never runs (Δ0). A template
      // the wire's hash does not resolve, or a plan aggregate the template
      // cannot serve, adapts to NULL ⇒ the loud excluded path — never
      // fabricated (fail-closed). The call site stamps `shard` (the wire
      // carries none — §6.2 declared deviation).
      let envelope: WhPartialEnvelope;
      if (args.rpcMode === true) {
        const wireHash = (o.envelope as { template_hash?: unknown }).template_hash;
        const templateRow = typeof wireHash === 'string' ? manifestRowByHash(wireHash) : null;
        const adapted = templateRow !== null ? adaptWireEnvelope(o.envelope, templateRow, plan) : null;
        if (adapted === null) {
          warnings.push({
            shard: o.shard,
            code: 'excluded',
            est_rows: o.estRows,
            retried: false,
            detail: 'envelope_invalid: rpc wire envelope does not adapt to the plan (template unresolved or a plan aggregate unserved) — excluded loud, never fabricated (r69 §3.4 arm 5)',
          });
          perShard.push({ shard: o.shard, ok: false, latencyMs: o.latencyMs, error: 'excluded' });
          continue;
        }
        envelope = { ...adapted, shard: o.shard };
      } else {
        envelope = o.envelope as WhPartialEnvelope;
      }
      // schema gate: envelope schema_version must match the table's current
      // version (ddl-wave law) — stale partials are excluded, never merged.
      if (args.tableSchemaVersion !== undefined && envelope.schema_version !== args.tableSchemaVersion) {
        warnings.push({ shard: o.shard, code: 'schema_mismatch', est_rows: o.estRows, retried: false });
        perShard.push({ shard: o.shard, ok: false, latencyMs: o.latencyMs, error: 'schema_mismatch' });
        continue;
      }
      // r44 consumption gates (§6.3 F2/F14): a partial carrying the grouped
      // truncation sentinel, or claiming a template whose merge_ops do not
      // cover the plan's ops (SUBSET rule), is REJECTED — warning object
      // carried (est_rows partial-derived, §4.4), shard degraded, and
      // wh_merge never sees it. The honest shards still merge.
      const gate = gatePartialAgainstPlan(plan, envelope);
      if (gate !== null) {
        warnings.push({
          shard: o.shard,
          code: gate.code,
          est_rows: partialRowCount(envelope) ?? o.estRows,
          retried: false,
          detail: gate.detail,
        });
        perShard.push({ shard: o.shard, ok: false, latencyMs: o.latencyMs, error: gate.code });
        continue;
      }
      perShard.push({ shard: o.shard, ok: true, latencyMs: o.latencyMs, error: null });
      okEnvelopes.push(envelope);
    } else {
      const w = o.warning ?? { code: 'excluded' };
      // r121 OPT-1b (design §1.3b): mapped shard-side template refusals
      // (WH400/WH401 → template_missing at classifyFetchFailure, detail
      // KEPT) RE-ATTACH the directory's row estimate for the shard — the
      // fold's exclusions must carry the same §4.4 degrade-vs-abort weight
      // today's handshake exclusions had (the LETHAL-1b lane; the mapped
      // record shape is battery-pinned). The lookup is over `selected` (the
      // dispatched placements population — the mapped codes only arise on
      // the rpc placements plane, where targets ⊆ selected); an
      // unresolvable shard falls back to the transport estimate. Non-mapped
      // failures keep o.estRows ?? 0.
      const dirRow = w.code === 'template_missing' && (w.detail === 'WH400' || w.detail === 'WH401')
        ? selected.find((r) => r.shard === o.shard)
        : undefined;
      warnings.push({
        shard: o.shard, code: w.code,
        est_rows: dirRow !== undefined ? directoryRowEstimate(dirRow) : (o.estRows ?? 0),
        retried: false,
        ...(w.detail !== undefined ? { detail: w.detail } : {}),
      });
      perShard.push({
        shard: o.shard, ok: false, latencyMs: o.latencyMs, error: w.code,
        ...(w.stamped === true ? { stamped: true } : {}),
      });
    }
  }

  // fail_fast (§5.1): collect-then-fail — ALL shards settle first, any
  // fetch-level failure surfaces as a 5xx with the full perShard[] payload.
  if (args.req.coverage_mode === 'fail_fast') {
    // Handshake exclusions (template_missing/schema_mismatch/max_rows_
    // exceeded) are deliberate §5.2 exclusions — degrade+warn, never a 5xx
    // (schema_mismatch was already exempt; the r44 handshake codes join it).
    const fetchFailures = perShard.filter((p) =>
      !p.ok && p.error !== 'schema_mismatch' && p.error !== 'template_missing' && p.error !== 'max_rows_exceeded'
    );
    if (fetchFailures.length > 0) {
      throw new WhEngineError('internal', `fail_fast: ${fetchFailures.length}/${perShard.length} shards failed (collect-then-fail, no cancellation in v0)`, {
        perShard,
        latencyMs: args.timers.nowMs() - started,
        directoryVersion: args.directoryVersion,
      });
    }
  }

  // Merge incrementally through the wire (monoid closure): a lying envelope
  // is excluded with a warning; the honest ones still merge.
  let merged: WhPartialEnvelope | null = null;
  const grouped = plan.groupKeys !== undefined;
  for (const env of okEnvelopes) {
    try {
      merged = merged === null
        ? (grouped ? mergeGroupedPartials(plan, [env]) : mergeScalarAggs(plan, [env]))
        : (grouped ? mergeGroupedPartials(plan, [merged, env]) : mergeScalarAggs(plan, [merged, env]));
    } catch (err) {
      const detail = err instanceof WhMergeError || err instanceof WhEngineError
        ? `${(err as Error).name}: ${(err as Error).message}`
        : String(err);
      warnings.push({ shard: env.shard, code: 'excluded', est_rows: 0, retried: false, detail });
      const idx = perShard.findIndex((p) => p.shard === env.shard);
      if (idx >= 0) {
        perShard[idx] = { ...perShard[idx], ok: false, error: 'excluded' };
      }
    }
  }

  const responded = perShard.filter((p) => p.ok).length;
  // r47: the coverage denominator is the DISPATCHED population — 1 on a won
  // replica plane (a '1/N' over the primary selections would read as partial
  // although the replica is the whole query) or on the r49 R3 single override
  // target, selected.length otherwise.
  const coverageDenominator = onReplicaPlane || geoDispatch !== null ? 1 : selected.length;
  const coverage = `${responded}/${coverageDenominator}`;
  const coverage_ratio = coverageDenominator === 0 ? 1 : responded / coverageDenominator;

  const response: WhEngineResponse = {
    v: 1,
    qid: args.req.qid,
    directory_version: args.directoryVersion,
    coverage,
    coverage_ratio,
    partial: responded < coverageDenominator,
    warnings: warnings.sort((a, b) => (a.shard < b.shard ? -1 : a.shard > b.shard ? 1 : 0)),
    perShard,
    latency_ms: args.timers.nowMs() - started,
  };

  if (grouped) {
    const fin = merged === null ? [] : finalizeGroups(merged, plan);
    if (fin.length > MAX_GROUPS) {
      // r40 P2-3: §4.6 — response always carries directory_version when known.
      throw new WhEngineError('capacity_exceeded', `${fin.length} groups exceed maxGroups=${MAX_GROUPS} — pre-aggregate per shard via RPC`, {
        directoryVersion: args.directoryVersion,
      });
    }
    const limit = args.req.query.limit;
    response.rows = limit === null || limit === undefined ? fin : fin.slice(0, limit);
  } else {
    response.result = merged === null ? finalizeScalarAggs(mergeScalarAggs(plan, []), plan) : finalizeScalarAggs(merged, plan);
  }
  // r121 OPT-1b (design §1.7): phases — injected POST-ASSEMBLY onto the
  // SUCCESS envelope only. Error envelopes are built by the entrypoint's
  // status ladder from the thrown WhEngineError and can never carry this
  // field (every throw above bypasses this assignment). pre_chain_ms is
  // ENTRYPOINT-THREADED (the fence consult + atomic directory read precede
  // the in-core clock — the entrypoint measures them and passes
  // ExecuteArgs.timings.preChainMs; 0 when absent, e.g. legacy callers);
  // handshake_ms is the sampled inventory-backstop block (0 when the
  // sampler did not fire — the fold's steady state); fanout_ms is the
  // measured fanout BLOCK duration (the runFanout await wall, documented
  // at the measurement site — NOT max(perShard)).
  // r124 OPT-3 (design §1, audit A ⟫A-3): dir_ms + fence_ms spread
  // CONDITIONALLY from the OPTIONAL timings fields — absent keys when not
  // threaded (the exact legacy {pre_chain_ms, handshake_ms, fanout_ms}
  // shape stays green), present when the entrypoint threads them (0 =
  // never-ran: replayed ⇒ dir_ms 0; unwired fence ⇒ fence_ms 0).
  response.phases = {
    pre_chain_ms: args.timings?.preChainMs ?? 0,
    handshake_ms: handshakeMs,
    fanout_ms: fanoutMs,
    ...(args.timings?.dirMs !== undefined ? { dir_ms: args.timings.dirMs } : {}),
    ...(args.timings?.fenceMs !== undefined ? { fence_ms: args.timings.fenceMs } : {}),
  };
  return response;
}
