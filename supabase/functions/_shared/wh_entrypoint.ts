// =============================================================================
// _shared/wh_entrypoint.ts — warehouse-engine HTTP handler (extracted r40)
// =============================================================================
// r40 (wiring review maxxing-r40-wiring-review): the r39 wiring shipped inside
// index.ts's Deno.serve with ZERO direct test coverage — three P0s survived a
// 606-test gate because the suite could not see the entrypoint at all. This
// module extracts the handler with injected deps (deploy.ts r16-F7 pattern:
// the Deno.serve entrypoint stays a thin env-wiring shell), so the smoke pins
// exercise the REAL routes.
//
// Review fixes carried here:
//   * P0-1: parseWhEngineRequest is now CALLED before any directory work —
//     the r39 code cast the raw body `as never` straight into executeWhQuery,
//     bypassing every §4.3 gate ({}, v:2, having/orderBy/offset/distinct
//     rejects, IDENT_RE table check) the 606-test suite proves.
//   * P0-3: the full-embed directory rows are projected to the queried table
//     (logical_name filter) before fan-out — a cross-table read previously
//     fanned out over foreign shards (200 coverage-0/n instead of tier_warm
//     404) and, once the real fetcher lands, would double-merge shards that
//     host placements for 2+ tables.
//   * P1-1 (erratum §4.3): shard_key_type/shard_key_column resolve from the
//     table's directory rows when present (directory = routing truth);
//     column_types/column_scales stay client-declared (pinned v1 envelope
//     extension — the engine enforces op-vs-type merge law; v2 moves them
//     into a catalog registry).
//   * P2-1: /health is a config version probe ONLY — the r39 code ran the
//     full paginated directory embed and discarded the rows (an anonymous
//     prober burned a view scan + an invocation per hit).
//   * P2-6: while the real shard fetcher is probe-gated (P2-6), /query is
//     rejected 500 BEFORE any fan-out — the r39 stub-as-fetcher served
//     plausible-looking empty 200s (coverage "0/n") indistinguishable from a
//     real all-shard outage.
//   * P2-3: error envelopes carry directory_version whenever known (§4.6).
//   * P3-1: quorum_unmet maps to 409 (forward-compat, §4.4 pins it).
//   * P3-4: the generic (non-WhEngineError) 500 no longer leaks raw internal
//     error text and includes qid when the body carried one.
//
// Contract notes (erratum r40): OPTIONS preflight is exempt from the §4.2
// auth-precedes-routing letter (browsers cannot send WHE_BEARER_TOKEN headers;
// pinned consumers are non-browser agents); trailing-slash stripping on the
// fn path is pinned as the one lenient normalization beyond WHATWG; apikey
// is checked for PRESENCE only at fn level (value validation is the platform
// gateway's job per §4.2's parenthetical — with verify_jwt=false the bearer
// check is the fn-level gate).
// =============================================================================

import {
  executeWhQuery,
  defaultWhEngineTimers,
  WhEngineError,
} from './wh_engine_core.ts';
import type { WhDirectoryRow, WhGeoDirectoryRow, WhShardFetcher, WhEngineTimers } from './wh_engine_core.ts';
import { parseWhEngineRequest } from './wh_engine_core.ts';
import type { WhGeoReadDispatch } from './wh_engine_core.ts';
import {
  fenceReadOnlyBody,
  geoWriteGate,
  isWritePlan,
  resolveReadDispatch,
} from './geo_write_fence.ts';
import type { WhFenceRead, WhWriteGateVerdict } from './geo_write_fence.ts';
import { signDirectorySnapshot, verifyDirectorySnapshot } from './wh_snapshot.ts';
import { deriveTemplateHashes } from './wh_handshake.ts';
import type { WhShardHandshake } from './wh_handshake.ts';

const FN = 'warehouse-engine';

/**
 * r57 wh_ryw v1 — the WH_RYW_V1 env parser (the read-your-writes flip
 * lever's SINGLE activation value is the exact string 'on'; frozen-contract
 * law: never best-effort parse — 'ON', '1', 'true', ' on', '' and ABSENT are
 * all OFF). The lever is a DEPLOY-TIME constant (env at boot), not a code
 * change: the flip ships on a deploy, never on a diff. Design:
 * research/design_r53_wh_ryw_lsn_poll.md — v1 IS the as-built r47 G6 stamped
 * compare (degenerate lsn-poll: one sample, zero wait, BigInt floor); the
 * design's §7 leaves the lever unspecified, so the r57 wiring pins it HERE
 * (WH_RYW_V1, read in warehouse-engine/index.ts, threaded as the
 * deps.rywGateEnabled flag below). DEFAULT OFF.
 */
export function rywGateEnabledFromEnv(raw: string | undefined): boolean {
  return raw === 'on';
}

/**
 * r44 FLIP_hasRealFetcher — the SINGLE flip site for the §4.1 deploy gate.
 * Stays FALSE this round (do NOT flip).
 *
 * Flip to true ONLY when ALL of the following hold (scatter §9 probe ladder):
 *   1. the REAL shard fetcher is frozen in — the QC2 compile is design-frozen
 *      (§6.1 call shape byte-pinned) and wired in warehouse-engine/index.ts
 *      in place of the probe-gated stub;
 *   2. the PAT wall is down — a live PAT is minted and the PAT-gated design
 *      lanes (WP-3/WP-5b: timeout surface, 42501 wall) are probeable;
 *   3. live probes #1/#2/#3 are GREEN — #1 platform auth round-trip,
 *      #2 directory read over v_warehouse_directory, #3 wh_query RPC
 *      round-trip against a seeded shard.
 * Until then the handler rejects /query 500 BEFORE any fan-out (r40 P2-6: a
 * deployed stub must never serve plausible empty 200s). Flipping is a CODE
 * change to this constant — never a deploy-time env — so the flip lands on
 * a reviewed diff.
 */
export const FLIP_hasRealFetcher = false;

export interface WhEngineDeps {
  /** Cheap config probe for the current directory version (§4.6 verify chain input). */
  probeDirectoryVersion(): Promise<number>;
  /** Full-embed directory read (truncation-guarded; wh_directory_reader.ts). */
  readDirectory(): Promise<{ rows: WhDirectoryRow[]; version: number }>;
  /** Per-shard fetcher injected into fan-out. */
  fetcher: WhShardFetcher;
  /** r40 P2-6 deploy gate. Absent => FLIP_hasRealFetcher (the single flip
   *  site above). Prod wiring passes the constant explicitly; tests inject
   *  their own value to exercise the handler either way. */
  hasRealFetcher?: boolean;
  /**
   * r44 §5.2 handshake (engine ⇄ shard): per-shard template-inventory reads
   * + eligibility. Optional — when absent, executeWhQuery fans out ungated
   * (the pre-r44 pipeline, still the stub-path behavior). The DEFAULT
   * implementation (makeWhHandshake over the real fetch, contract §4.6
   * engine→shard plane auth) is wired in warehouse-engine/index.ts; tests
   * stub this shape directly.
   */
  handshake?: WhShardHandshake;
  /**
   * r69 (AM-4/F-N8): the RPC-plane mode flag — computed at boot in
   * warehouse-engine/index.ts from the SAME env-guard expression the staged
   * r60 flip patch ORs into the effective real-fetcher gate (WH_REAL_FETCHER,
   * single activation value the exact string 'on' — the r57 WH_RYW_V1 lever
   * pattern: env read at boot, threaded through the wiring as a plain flag;
   * never an ad-hoc parse inside the shared modules — purity law). DEFAULT
   * ABSENT (false): with rpcMode absent the fan-out targets never carry rpc,
   * the §6.2 wire adapter never runs, and the D2 plan_untemplated check does
   * not exist (additive-only law — byte-identical unset path, F-N4). ON: the
   * fan-out speaks the §6.1 wh_query RPC shape on the placements plane.
   */
  rpcMode?: boolean;
  /**
   * §4.6 snapshot HMAC key (WH_SNAPSHOT_KEY secret — never WHE_BEARER_TOKEN).
   * Absent => snapshot mode disabled: full-embed only, nothing attached,
   * a replayed snapshot is ignored (not an error).
   */
  snapshotKey?: string;
  // ---- r47 read-plane deps (wh-contract r47 errata). All optional: absent
  // deps on a read_plane:"replica" request fail CLOSED to the primary plane
  // (geo_fallback_primary/geo_read_error + coverage_miss for the reference
  // resolver); the primary plane NEVER consults them — the pre-r47 pipeline
  // stays byte-identical. Wired in warehouse-engine/index.ts over supabase-js.
  /** v_geo_directory reader (≤1-row population; limit(2) law-breakage
   *  tripwire inside; THROWS on failure — the engine degrades, never throws). */
  readGeoDirectory?: () => Promise<WhGeoDirectoryRow[]>;
  /** The FM config geo_mode value (null = unreadable ⇒ mode gate fails
   *  closed). */
  readGeoMode?: () => Promise<string | null>;
  /** is_reference for the queried table (warehouse_tables); null = unknown
   *  ⇒ coverage-null replicas fail closed. Called lazily — only when the
   *  geo row's coverage is null. */
  readTableReference?: (table: string) => Promise<boolean | null>;
  // ---- r49 B2 fence deps (geo_write_fence.ts). All optional: absent deps
  // keep every existing path byte-identical (the pre-r49 pipeline) — the
  // write gate FAILS CLOSED when its dep is unwired (a write plan must never
  // slip ungated), the read dispatch identity merely degrades to the
  // as-built placements. Wired in warehouse-engine/index.ts over supabase-js.
  /** The ONE combined fence config read (G-W1) — geo_write_fence.ts owns the
   *  query shape (one round trip, three keys). */
  fetchFenceConfig?: () => Promise<WhFenceRead>;
  /** The engine's OWN project_ref (the SUPABASE_URL subdomain — the
   *  co-hosted law). '' / absent ⇒ the R3 dispatch identity degrades to
   *  placements (a wrong own-ref would self-dispatch a remote override). */
  ownProjectRef?: string;
  // ---- r57 wh_ryw v1 flip lever (design_r53_wh_ryw_lsn_poll.md; PLAN r58
  // secondary "G6 stamped compare; flip stays gated") ----
  /** Whether the read-your-writes floor (min_lsn) adjudication is ACTIVE on
   *  this deployment. DEFAULT OFF: absent => false. Wired from the WH_RYW_V1
   *  env (rywGateEnabledFromEnv) in warehouse-engine/index.ts — a deploy-time
   *  constant, never a code change. OFF: a min_lsn-pinned request is a 400
   *  naming the lever (planner honesty — the r47 parse-gate precedent: an
   *  enforced-looking no-op floor is rejected, never silently un-enforced);
   *  EVERY other request, including read_plane:"replica" WITHOUT min_lsn
   *  (G6a documented eventual consistency), is byte-identical in both lever
   *  states (additive-only law). ON: the r47 errata behavior exactly. */
  rywGateEnabled?: boolean;
  /** Optional timers injection (tests); defaults to real timers. */
  timers?: WhEngineTimers;
}

function json(body: unknown, status: number): Response {
  return new Response(JSON.stringify(body, (_k, v) => (typeof v === 'bigint' ? v.toString() : v)), {
    status,
    headers: { 'content-type': 'application/json' },
  });
}

function timingSafeEqual(a: string, b: string): boolean {
  if (a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
  return diff === 0;
}

function authError(kind: 'missing_bearer' | 'invalid_token' | 'bad_apikey'): Response {
  return json({ v: 1, error: { code: 'malformed', message: `auth rejected before route dispatch (${kind})`, auth_kind: kind } }, 401);
}

function checkAuth(req: Request): Response | null {
  const fleetToken = Deno.env.get('WHE_BEARER_TOKEN');
  if (!fleetToken) {
    return json({ v: 1, error: { code: 'internal', message: 'server has no WHE_BEARER_TOKEN secret set' } }, 500);
  }
  const auth = req.headers.get('authorization');
  if (!auth || !auth.toLowerCase().startsWith('bearer ')) return authError('missing_bearer');
  if (!timingSafeEqual(auth.slice(7).trim(), fleetToken)) return authError('invalid_token');
  if (!req.headers.get('apikey')) return authError('bad_apikey');
  return null;
}

// ---------- column-type channel (erratum §4.3: pinned v1 envelope extension) ----------
function parseColumnTypes(raw: unknown, field: string, want: 'string' | 'number'): Record<string, unknown> {
  if (raw === undefined || raw === null) return {};
  if (!isPlainObject(raw)) throw new WhEngineError('malformed', `${field} must be an object of column -> ${want}`);
  for (const [k, v] of Object.entries(raw)) {
    if (!/^[a-zA-Z_][a-zA-Z0-9_]*$/.test(k)) throw new WhEngineError('malformed', `${field} keys must be plain identifiers (got ${JSON.stringify(k)})`);
    if (want === 'string' ? typeof v !== 'string' : typeof v !== 'number') {
      throw new WhEngineError('malformed', `${field}.${k} must be a ${want}`);
    }
  }
  return raw;
}

function isPlainObject(v: unknown): v is Record<string, unknown> {
  return v !== null && typeof v === 'object' && !Array.isArray(v);
}

// ---------- /query core ----------
async function handleQuery(req: Request, deps: WhEngineDeps): Promise<Response> {
  let rawBody: unknown;
  try {
    rawBody = await req.json();
  } catch {
    return json({ v: 1, qid: null, error: { code: 'malformed', message: 'request body is not valid JSON' } }, 400);
  }
  if (!isPlainObject(rawBody)) {
    return json({ v: 1, qid: null, error: { code: 'malformed', message: 'request body must be an object' } }, 400);
  }
  const body = rawBody as Record<string, unknown>;
  const qid = typeof body.qid === 'string' ? body.qid : null;

  // r40 P2-6: reject before ANY directory work while the fetcher is
  // probe-gated. r44: the effective gate is the dep's claim OR the single
  // FLIP_hasRealFetcher constant default (see the constant's comment block
  // for the exact flip conditions).
  const hasRealFetcher = deps.hasRealFetcher ?? FLIP_hasRealFetcher;
  if (!hasRealFetcher) {
    return json({
      v: 1, qid,
      error: { code: 'internal', message: 'real shard fetcher lands after live probes #1/#2 (PAT-gated design freeze for the QC2 compile)' },
    }, 500);
  }

  // ---- r49 B2 wave1: the ENGINE WRITE-PLANE FENCE (§2 G-W1..W5, D28) ----
  // Pre-dispatch on WRITE-PLAN requests ONLY (isWritePlan over the RAW body —
  // the §4.3 parse drops the §5.2 markers). Runs BEFORE the parse gates so a
  // fenced fleet answers 503 read_only_mode even for a not-yet-parsable
  // mutating plan (the write wave cannot land ungated — the call site exists
  // and is wired TODAY). The r40 P2-6 deploy gate above keeps precedence:
  // pre-flip, everything (writes included) gets the pinned stub 500 first.
  if (isWritePlan(rawBody)) {
    let gate: WhWriteGateVerdict;
    try {
      // Fail-closed: an UNWIRED fence dep is the G-W1 class (the engine
      // cannot observe the fence ⇒ it cannot vouch the write ⇒ 500 internal,
      // NEVER a silent fall-through and NEVER a masquerading 503).
      if (deps.fetchFenceConfig === undefined) {
        throw new WhEngineError('internal', 'geo write fence dep unwired — cannot vouch the write fence (G-W1 fail-closed)');
      }
      gate = geoWriteGate(
        await deps.fetchFenceConfig(),
        { write_epoch: (rawBody as { write_epoch?: unknown }).write_epoch },
      );
    } catch (e) {
      // G-W1: the fence-read failure surfaces VERBATIM-class ⇒ 500, never
      // 503 (never masquerade unreadable config as unwritable-or-writable).
      if (e instanceof WhEngineError) {
        return json({ v: 1, qid, error: { code: e.code, message: e.message } }, 500);
      }
      console.error('warehouse-engine fence read error:', (e as Error)?.message ?? e);
      return json({ v: 1, qid, error: { code: 'internal', message: 'internal engine error' } }, 500);
    }
    if (!gate.ok) return json(fenceReadOnlyBody(qid), 503);
    // gate.ok: the fence is OPEN — the G-W5 echo (gate.writeEpoch) is the
    // future write branch's response field. v0 has no write branch: the body
    // falls through to the as-built path below (a mutating op parses 400;
    // a marker-only body is served as the read plan it declares).
  }

  // r40 P0-1: the §4.3 parse gates run HERE (r39 wired the raw body through).
  // The column-type channel (erratum §4.3 v1 extension) validates in the same
  // mapped try — a malformed map is a 400, never an unhandled throw.
  let parsed;
  let columnTypes: Record<string, string>;
  let columnScales: Record<string, number>;
  try {
    parsed = parseWhEngineRequest(body);
    columnTypes = parseColumnTypes(body.column_types, 'column_types', 'string') as Record<string, string>;
    columnScales = parseColumnTypes(body.column_scales, 'column_scales', 'number') as Record<string, number>;
  } catch (e) {
    if (e instanceof WhEngineError) {
      return json({ v: 1, qid, error: { code: e.code, message: e.message } }, 400);
    }
    throw e;
  }

  // ---- r57 wh_ryw v1 flip lever (WH_RYW_V1, default OFF) ----
  // The RYW floor (min_lsn) is adjudicated ONLY on a flipped deployment. The
  // parse gates above already validated the field's SHAPE (strict coupling +
  // pg_lsn format), so this 400 is purely the deployment-state rejection: a
  // floor the engine will not enforce is an enforced-LOOKING no-op — the
  // exact planner-dishonesty class the r47 parse gates reject — so it is a
  // 400 naming the lever, never a silent ignore (that would serve a possibly-
  // behind replica as RYW) and never a fallback (that would hide the
  // misconfiguration behind a primary read). Fail fast: BEFORE any directory
  // or geo work. Requests WITHOUT min_lsn — the entire current traffic — are
  // byte-identical in both lever states (additive-only law).
  if (parsed.min_lsn !== undefined && deps.rywGateEnabled !== true) {
    return json({
      v: 1,
      qid,
      error: {
        code: 'malformed',
        message:
          'min_lsn requires the read-your-writes gate (v1) enabled on this deployment (WH_RYW_V1=on) — the flip lever is deploy-time and currently OFF',
      },
    }, 400);
  }

  let knownVersion: number | undefined;
  try {
    // §4.6 snapshot replay: verify chain (sig -> TTL -> version) BEFORE the
    // expensive full-embed read. ANY failure => silent full embed (never an
    // error). A valid replay skips the directory read AND the re-attach —
    // the client's copy is still current (the §3 Q2 byte-budget win).
    const snapValue = typeof body.directory_snapshot === 'string' ? body.directory_snapshot : undefined;
    let directoryRows: WhDirectoryRow[] | undefined;
    let version: number | undefined;
    let replayed = false;
    if (snapValue !== undefined && deps.snapshotKey !== undefined) {
      const current = await deps.probeDirectoryVersion();
      knownVersion = current;
      const v = await verifyDirectorySnapshot(snapValue, { key: deps.snapshotKey, currentVersion: current });
      if (v.ok && Array.isArray(v.payload)) {
        // Sig proves the engine authored the payload; the array check is a
        // cheap engine-bug backstop (signed garbage falls back, never merges).
        directoryRows = v.payload as WhDirectoryRow[];
        version = v.version;
        replayed = true;
      }
    }
    let freshSnapshot: string | undefined;
    if (!replayed) {
      const read = await deps.readDirectory();
      directoryRows = read.rows;
      version = read.version;
      knownVersion = version;
      if (deps.snapshotKey !== undefined) {
        freshSnapshot = await signDirectorySnapshot(version, directoryRows, deps.snapshotKey);
      }
    }
    if (directoryRows === undefined || version === undefined) {
      // Unreachable (both branches assign) — a narrow backstop for TS flow
      // analysis that doubles as a loud tripwire if the branches ever change.
      throw new WhEngineError('internal', 'directory replay/read produced no rows');
    }

    // r40 P0-3: per-table projection (directory = routing truth; the full
    // embed serves snapshot mode). Loud failure when rows predate the
    // logical_name passthrough — filtering an unmapped read would 404 a
    // table that exists (silent wrongness).
    if (directoryRows.length > 0 && directoryRows.every((r) => typeof r.logical_name !== 'string')) {
      throw new WhEngineError('internal', 'directory rows lack logical_name — entrypoint row mapping drift');
    }
    const tableRows = directoryRows.filter((r) => r.logical_name === parsed.table);
    const head = tableRows[0];

    // r47: ENGINE-DERIVED plan template hashes (wh-contract r47 errata) —
    // never client-supplied (no request field exists; a client-declared set
    // would be a lying-client degrade knob). Manifest-bounded by construction
    // (plan-honesty can never self-reject); the same set feeds the §5.2
    // handshake, F14, and the G5 replica gate (template plans never ride the
    // replica plane — DDL is NOT replicated, the RPC inventory is unaudited).
    const derivedTemplateHashes = deriveTemplateHashes(
      {
        table: parsed.table,
        ...(parsed.query.groupBy !== undefined ? { groupKeys: parsed.query.groupBy } : {}),
        aggs: Object.fromEntries(
          parsed.query.select.map((s, i) => [s.alias ?? `${s.op}_${i}`, { op: s.op, ...(s.col !== undefined ? { col: s.col } : {}) }]),
        ),
      },
      head?.table_schema_version,
    );

    // r47: resolve the geo-mode config ONLY on the replica plane (zero
    // overhead on the primary plane — the pre-r47 behavior is byte-identical;
    // a missing dep fails closed inside the engine's gate ladder).
    const wantsReplica = parsed.read_plane === 'replica';
    const geoMode = wantsReplica && deps.readGeoMode !== undefined ? await deps.readGeoMode() : undefined;

    // r49 B2 R3 (re-audit #6): the legacy primary-plane READ dispatch
    // identity — ONE combined config read (the override key rides the same
    // read, re-audit #3e). Only on the legacy primary plane (the replica
    // plane's identity is the geo row and NEVER consults the override), only
    // when BOTH deps are wired (absent ⇒ the pre-r49 path, byte-identical).
    // A failed read DEGRADES to placements (the read plane degrades, never
    // throws — G-W1's 500 is a WRITE-plane rule). 'placements' resolves to
    // no instruction at all, keeping the unset path byte-identical.
    let geoReadDispatch: WhGeoReadDispatch | undefined;
    if (!wantsReplica && deps.fetchFenceConfig !== undefined && typeof deps.ownProjectRef === 'string' && deps.ownProjectRef !== '') {
      const fence = await deps.fetchFenceConfig();
      if (fence.ok) {
        const dispatch = resolveReadDispatch(fence.values, deps.ownProjectRef);
        if (dispatch.kind !== 'placements') geoReadDispatch = dispatch;
      }
    }

    const response = await executeWhQuery({
      req: parsed,
      columnTypes,
      ...(Object.keys(columnScales).length > 0 ? { columnScales } : {}),
      directoryRows: tableRows,
      // r40 P1-1: shard-key metadata from the directory when present;
      // client-declared values are ignored (erratum §4.3). '' = no shard key
      // (columnTypes[''] is undefined => no shardKeyPlan => no pruning, the
      // tier_warm path also hits this when the table has no serving rows).
      shardKeyColumn: head?.shard_key_column ?? '',
      shardKeyType: head?.shard_key_type ?? 'none',
      directoryVersion: version,
      ...(head?.table_schema_version !== undefined ? { tableSchemaVersion: head.table_schema_version } : {}),
      timers: deps.timers ?? defaultWhEngineTimers(),
      fetcher: deps.fetcher,
      // r44 §5.2: the handshake rides only when wired (optional dep — absent
      // => the ungated pre-r44 pipeline, keeping the stub path reachable).
      ...(deps.handshake !== undefined ? { handshake: deps.handshake } : {}),
      // r47 read-plane deps (engine-side ladder G1..G6; the primary plane
      // never consults them). templateHashes = the ENGINE-DERIVED set.
      templateHashes: derivedTemplateHashes,
      ...(wantsReplica
        ? {
          ...(geoMode !== undefined ? { geoMode } : {}),
          ...(deps.readGeoDirectory !== undefined ? { geoReader: deps.readGeoDirectory } : {}),
          ...(deps.readTableReference !== undefined
            ? { resolveTableReference: () => deps.readTableReference!(parsed.table) }
            : {}),
        }
        : {}),
      // r49 B2 R3: the legacy-read dispatch instruction (absent on the
      // replica plane and whenever the identity is placements — the
      // byte-identical unset path).
      ...(geoReadDispatch !== undefined ? { geoReadDispatch } : {}),
      // r69 (AM-4/F-N8): the RPC-plane mode flag rides the same flag-passing
      // shape as every other wiring-owned flag (spread-conditional — with the
      // lever unset the core never sees the field at all, byte-identical).
      ...(deps.rpcMode === true ? { rpcMode: true } : {}),
    });
    return json({
      ...response,
      // §4.6: attach a fresh snapshot on full embed ONLY — a valid replay
      // leaves the client's copy current (re-attaching would defeat the
      // byte-budget purpose of the protocol).
      ...(!replayed && freshSnapshot !== undefined ? { directory_snapshot: freshSnapshot } : {}),
    }, 200);
  } catch (e) {
    if (e instanceof WhEngineError) {
      const status = e.code === 'malformed' ? 400
        : e.code === 'capacity_exceeded' ? 422
        : e.code === 'tier_warm' ? 404
        : e.code === 'page_unavailable' ? 409
        : e.code === 'quorum_unmet' ? 409
        : e.code === 'plan_untemplated' ? 400 // r69 AM-3: the D2 plan-honesty 4xx (rpcMode plane only)
        : 500;
      const directoryVersion = e.directoryVersion ?? knownVersion;
      return json({
        v: 1,
        qid,
        ...(directoryVersion !== undefined ? { directory_version: directoryVersion } : {}),
        error: { code: e.code, message: e.message },
        ...(e.perShard ? { perShard: e.perShard } : {}),
        ...(e.latencyMs !== undefined ? { latency_ms: e.latencyMs } : {}),
      }, status);
    }
    // r40 P3-4: no raw internal error text on the wire (may carry connection
    // strings); the detail goes to the isolate log for ops.
    console.error('warehouse-engine internal error:', (e as Error)?.message ?? e);
    return json({ v: 1, qid, error: { code: 'internal', message: 'internal engine error' } }, 500);
  }
}

/**
 * The full warehouse-engine HTTP handler. index.ts wires this with real
 * env-derived deps; tests inject fakes.
 */
export async function handleWhEngineRequest(req: Request, deps: WhEngineDeps): Promise<Response> {
  const url = new URL(req.url);
  const rawPath = url.pathname.replace(/\/+$/, '');
  // r40 P0-4 (smoke-pin catch): idx points at the SLASH before the fn name —
  // the r39 code added FN.length (not marker.length), mangling every real
  // deployment-shape path: /functions/v1/warehouse-engine/query -> 'e/query'
  // -> 400 no-route. The engine was unreachable at its pinned URL shape and
  // no static review traced a full first-call URL (r4 C2.3 law, round 2).
  const marker = `/${FN}`;
  const idx = rawPath.indexOf(marker);
  const path = idx >= 0 ? rawPath.substring(idx + marker.length) : rawPath;

  // OPTIONS preflight is exempt from the auth-precedes-routing letter (r40
  // erratum §4.2): browsers cannot attach WHE_BEARER_TOKEN headers on preflights.
  if (req.method === 'OPTIONS') {
    return new Response(null, {
      status: 204,
      headers: {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
        'Access-Control-Allow-Headers': 'Authorization, Content-Type, apikey',
        'Access-Control-Max-Age': '86400',
      },
    });
  }

  // r40 P2-1: /health is a config version probe only — L2 shape per §4.1.
  if (path === '/health' && req.method === 'GET') {
    try {
      const version = await deps.probeDirectoryVersion();
      return json({ v: 1, ok: true, directory_version: version }, 200);
    } catch (e) {
      return json({ v: 1, ok: false, error: { code: 'internal', message: (e as Error).message } }, 500);
    }
  }

  const authFailure = checkAuth(req);
  if (authFailure) return authFailure;

  if (path === '/query' && req.method === 'POST') {
    return handleQuery(req, deps);
  }

  return json({ v: 1, error: { code: 'malformed', message: `no route ${req.method} ${path}` } }, 400);
}
