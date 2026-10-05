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
// r138 F-1a: the freshness keeper stamps last_health_at for shards that
// answered 2xx — via the EXISTING db() singleton (whe_store.ts, the engine's
// only production supabase-js entry point; r116 seam). The call site is the
// entrypoint, NOT the core (the core's purity island bans direct I/O and
// index.ts is frozen) and NOT a new dep (no new env, no new infra, no
// scheduled prober — d2 F-1 decide-once).
import { db } from './whe_store.ts';
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
 * FLIPPED TRUE r118 (design_r118_realfetcher_flip.md §3b): all three gate
 * conditions were met — real fetcher frozen r69, PAT wall down (PAT alive
 * 68 rounds), probes #1/#2/#3 green (r59 PAT-only battery + r118 PostgREST
 * service-key re-proof).
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
export const FLIP_hasRealFetcher = true;

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
  // ---- r124 A8 (design_r124_opt3_a8.md §2): the GENERATED build stamp ----
  /** The engine build identity (`export const ENGINE_BUILD = "<sha7>"` — the
   *  gitignored _shared/engine_build.ts, written by `make stamp` from the
   *  HEAD sha7). Threaded by the SHELL ONLY (the hasRealFetcher precedent:
   *  the constant import lives in warehouse-engine/index.ts; _shared itself
   *  never reads the file — it only ever sees the value as this injected
   *  dep, keeping the purity law intact since a constant import is not an
   *  env read). Absent => /health renders `engine_build: null` (additive
   *  field, never a 500 over the missing stamp). */
  engineBuild?: string;
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

  // r124 OPT-3 (design §1, audit A ⟫A-1): wantsReplica is HOISTED above the
  // pre-chain try — the r49 read-dispatch fence predicate (formerly :431)
  // must be decidable at the :339 boundary (right after the parse + RYW
  // gates) so the consult can start EARLY. Pure local compute on `parsed` —
  // no behavior change on any plane.
  const wantsReplica = parsed.read_plane === 'replica';

  let knownVersion: number | undefined;
  try {
    // r121 OPT-1b (design §1.7): pre_chain_ms is measured HERE — the
    // entrypoint owns the pre-engine chain (the §4.6 snapshot-replay probe +
    // the atomic directory read + snapshot signing + the replica-plane
    // geo-mode read + the r49 read-dispatch fence consult), all of which
    // precede the engine's in-core clock (executeWhQuery's `started`). The
    // total is threaded INTO the engine via ExecuteArgs.timings.preChainMs
    // and injected post-assembly onto the success envelope's
    // phases.pre_chain_ms; error envelopes (the catch ladder below) omit
    // phases entirely. The SAME timers instance is passed to the engine —
    // resolved once from deps.timers (the injection point is unchanged;
    // default real timers when unwired).
    const timers = deps.timers ?? defaultWhEngineTimers();
    const preChainStarted = timers.nowMs();
    // r124 OPT-3 EARLY START (design §1, audit A ⟫A-1): the r49 read-dispatch
    // fence consult starts UNAWAITED at the parse+RYW-gate boundary — 400-class
    // requests were already returned above, so they stay consult-free (the
    // cost law, ⟫A-1). Dependency-freedom is PROVEN (audit A): fetchFenceConfig
    // takes no data args (the "fence trio" = 3 keys in ONE .in() GET), the
    // consult is PURE-READ, and only local compute sits between the two
    // awaits — starting it before the replay probe / directory read is
    // semantics-free. The consume block (below, after the directory read)
    // replaces the old serial await; the predicate SEMANTICS are the former
    // :431 line's exactly (the typeof guard is hoisted into `ownRef`).
    // Consume order: dir first -> fence (⟫A-2), with the deterministic
    // both-fail precedence — the DIRECTORY error wins (a fence rejection is
    // rethrown only when the directory read succeeded).
    const ownRef = typeof deps.ownProjectRef === 'string' ? deps.ownProjectRef : '';
    const fenceEligible = !wantsReplica && deps.fetchFenceConfig !== undefined && ownRef !== '';
    const fenceStartMs = fenceEligible ? timers.nowMs() : 0;
    const fenceP: Promise<WhFenceRead> | undefined = fenceEligible ? deps.fetchFenceConfig!() : undefined;
    // Defensive settlement guard: the consult must NEVER become an unhandled
    // rejection if the chain escapes early (a failed replay probe, a failed
    // directory read, a failed sign — the same paths where the OLD serial
    // code never fired the consult at all). The guard marks the rejection
    // handled; the consume block below still rethrows it when reached with
    // the directory read ok (today's throw semantics preserved, ⟫A-2).
    fenceP?.catch(() => {});
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
    // r124 OPT-3 sub-span (⟫A-3): dir_ms measures the atomic directory read
    // wall; replayed ⇒ 0 (the never-ran=0 convention). The read is STARTED
    // here (only on !replayed — an unconditional start would add an rpc POST
    // per replay request) and consumed immediately (dir first -> fence).
    let dirMs = 0;
    if (!replayed) {
      const dirStartMs = timers.nowMs();
      const read = await deps.readDirectory();
      dirMs = timers.nowMs() - dirStartMs;
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
    // r124 OPT-3 (⟫A-2): the fence CONSUME — dir first, then fence. A fence
    // ok:false degrades silently to placements (unchanged — the read plane
    // degrades, never throws, G-W1 is a WRITE-plane rule); a fence REJECTION
    // is rethrown only when the directory read succeeded (the both-fail
    // precedence: the directory error already escaped above). The consult was
    // STARTED early at the try boundary — its wall (fence_ms, ⟫A-3) runs from
    // the early start, so the overlap with the directory read is subtracted
    // honestly (pre_chain_ms >= max(dir_ms, fence_ms) is the invariant).
    let geoReadDispatch: WhGeoReadDispatch | undefined;
    let fenceMs = 0;
    if (fenceP !== undefined) {
      const fence = await fenceP;
      fenceMs = timers.nowMs() - fenceStartMs;
      if (fence.ok) {
        const dispatch = resolveReadDispatch(fence.values, ownRef);
        if (dispatch.kind !== 'placements') geoReadDispatch = dispatch;
      }
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
    // r129 (design_r128_joinplans §2.2, audit A A4 — the DISPATCH-POPULATION
    // SPLIT, load-bearing): a join request ALSO needs the dim relation's
    // placements from the SAME atomic embed, but they feed the colocation
    // gate ONLY (ExecuteArgs.joinDimRows) — they must NEVER enter
    // directoryRows. Dim rows carry unbounded NULL/NULL key bounds, so they
    // SURVIVE E11 range pruning: folding them into the dispatch population
    // would POST W6 twice per shard (partials merged twice — n/x doubled,
    // phantom coverage 6/6), and head = tableRows[0] must stay a FACT row
    // (the shard-key metadata + tableSchemaVersion binding). Non-join
    // requests: the dim population is not even collected (undefined — the
    // args shape stays byte-identical).
    const joinDescriptor = parsed.query.join;
    const joinDimRows = joinDescriptor !== undefined
      ? directoryRows.filter((r) => r.logical_name === joinDescriptor.table)
      : undefined;

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
        // r129 (design_r128_joinplans §2.3): the join-class partition rides
        // on PRESENCE — a join plan derives ONLY join-class manifest rows,
        // a non-join plan excludes them (deriveTemplateHashes header).
        ...(joinDescriptor !== undefined ? { join: joinDescriptor } : {}),
        aggs: Object.fromEntries(
          parsed.query.select.map((s, i) => [s.alias ?? `${s.op}_${i}`, { op: s.op, ...(s.col !== undefined ? { col: s.col } : {}) }]),
        ),
      },
      head?.table_schema_version,
    );

    // r47: resolve the geo-mode config ONLY on the replica plane (zero
    // overhead on the primary plane — the pre-r47 behavior is byte-identical;
    // a missing dep fails closed inside the engine's gate ladder).
    // (r124 OPT-3: wantsReplica is hoisted above the try — the fence
    // eligibility predicate must be decidable at the try boundary.)
    const geoMode = wantsReplica && deps.readGeoMode !== undefined ? await deps.readGeoMode() : undefined;

    // r124 OPT-3: the r49 B2 R3 serial consult block that stood HERE (after
    // the hashes + geo-mode read) is REPLACED by the early-start + consume
    // pair above (the early start at the try boundary, the consume right
    // after the directory read). The semantics the old block pinned — ONE
    // combined config read, replica plane never consults, placements
    // identity = no instruction (byte-identical unset path) — are unchanged;
    // only the FIRING ORDER moved (parallel-safe per the audit A dependency
    // proof).

    // r121 OPT-1b (design §1.7): the pre-chain span CLOSES here — after the
    // r49 fence consult has SETTLED (r124 OPT-3: the fence/dir pair is
    // consumed before this point, so the span is the wall of the pair plus
    // the local compute), before the engine's own in-core clock starts.
    const preChainMs = timers.nowMs() - preChainStarted;

    const response = await executeWhQuery({
      req: parsed,
      columnTypes,
      ...(Object.keys(columnScales).length > 0 ? { columnScales } : {}),
      directoryRows: tableRows,
      // r129 (design_r128_joinplans §2.2 A4): the dim population rides
      // SEPARATELY (colocation-gate input ONLY — never the dispatch
      // population; the split is load-bearing, see the comment at the :462
      // filter). Spread-conditional: absent on every non-join request (the
      // pre-r129 args shape stays byte-identical).
      ...(joinDimRows !== undefined ? { joinDimRows } : {}),
      // r40 P1-1: shard-key metadata from the directory when present;
      // client-declared values are ignored (erratum §4.3). '' = no shard key
      // (columnTypes[''] is undefined => no shardKeyPlan => no pruning, the
      // tier_warm path also hits this when the table has no serving rows).
      shardKeyColumn: head?.shard_key_column ?? '',
      shardKeyType: head?.shard_key_type ?? 'none',
      directoryVersion: version,
      ...(head?.table_schema_version !== undefined ? { tableSchemaVersion: head.table_schema_version } : {}),
      // r121 OPT-1b (design §1.7): the same resolved timers instance drives
      // the engine's clock (byte-identical to the previous inline
      // `deps.timers ?? defaultWhEngineTimers()` expression), and the
      // pre-chain measurement above rides into the engine as timings.
      // (ExecuteArgs.timings — optional upstream; the engine defaults
      // pre_chain_ms to 0 when absent.)
      timers,
      // r124 OPT-3 (⟫A-3): the sub-span timers ride alongside preChainMs —
      // dir_ms (0 on the replayed path: never-ran=0) + fence_ms (0 when the
      // fence dep is unwired). OPTIONAL fields upstream: the core spreads
      // them CONDITIONALLY into phases so legacy callers/tests that thread
      // only preChainMs keep the exact phases shape (the wh_handshake_test
      // exact pin stays green).
      timings: { preChainMs, dirMs, fenceMs },
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
    // ---- r138 F-1a: the FRESHNESS KEEPER (d2 P1 fold). After a fanout where
    // shards answered HTTP 2xx (the envelope's ok-set = the placements whose
    // partials were consumed), fire-and-forget ONE stats-only UPDATE bumping
    // last_health_at for those shards. Stats-only law, provably never bumps
    // directory_version: the 0013 placements dv trigger is
    // `after update of state, key_min, key_max, hash_slot, schema_version`
    // (0013:329-334) — last_health_at is EXCLUDED from UPDATE OF, so the
    // write cannot fire wh_bump_directory_version. Fire-and-forget: the
    // response below is NOT awaited behind it; every rejection is handled.
    // Failure = log-only — this is NOT a user-facing write (WRITES-FAIL-FAST
    // does not apply): a missed stamp degrades to the pre-r138 90s-staleness
    // behavior, never fails the query. The stamp is a PostgREST value (the
    // engine-side now equivalent — PostgREST cannot execute SQL now()). The
    // placements table carries project_id (uuid), not the ref, so the ok-set
    // resolves ids via projects (the 0013 view's own join columns, pr.id =
    // p.project_id / pr.ref). Echo law: fixed strings only — never ref/value
    // fragments on the log line. Offline (dep-less tests): db() throws the
    // FM missing-env message synchronously — caught here, log-only.
    try {
      const okShards = response.perShard.filter((p) => p.ok).map((p) => p.shard);
      if (okShards.length > 0) {
        const store = db();
        const stampedAt = new Date().toISOString();
        void store
          .from('projects')
          .select('id')
          .in('ref', okShards)
          .then(({ data, error }) => {
            if (error !== null && error !== undefined) throw new Error('keeper project-id resolve failed');
            const ids = ((data ?? []) as { id: unknown }[])
              .map((r) => r.id)
              .filter((id): id is string => typeof id === 'string');
            if (ids.length === 0) return;
            return store
              .from('warehouse_placements')
              .update({ last_health_at: stampedAt })
              .in('project_id', ids);
          })
          .then(
            (res) => {
              // FE-A-1 (r138 fresh-eyes A): postgrest-js RESOLVES PostgREST
              // failures as {error} (never rejects) — the fulfillment path
              // must discriminate too, else a failed stamp is silent and the
              // "failure is log-only" contract is unrealized. Fixed string,
              // echo law intact; same message both paths.
              const upd = res as { error?: unknown } | null | undefined;
              if (upd !== null && upd !== undefined && upd.error !== null && upd.error !== undefined) {
                console.error('warehouse-engine freshness keeper: last_health_at stamp failed (log-only; never fails the query)');
              }
            },
            () => {
              console.error('warehouse-engine freshness keeper: last_health_at stamp failed (log-only; never fails the query)');
            },
          );
      }
    } catch {
      console.error('warehouse-engine freshness keeper: last_health_at stamp failed (log-only; never fails the query)');
    }
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
        // r129 (design_r128_joinplans §2.1/§2.2): the join-plan plan-time
        // 4xx family — ALL THREE are plan/parse-class rejects (400); the
        // wh_engine_core.ts:81-87 hazard comment forbids a 500-fallthrough.
        : e.code === 'join_not_colocated' ? 400
        : e.code === 'join_template_required' ? 400
        : e.code === 'join_key_mismatch' ? 400
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
  // r124 A8 (design §2 ⟫B-1): the additive `engine_build` field — the shell
  // threads the generated stamp (DI); absent ⇒ null. The field can NEVER
  // 500 the probe (a static-import constant cannot throw; null is the
  // absent face) and the 500 arm below is untouched.
  if (path === '/health' && req.method === 'GET') {
    try {
      const version = await deps.probeDirectoryVersion();
      return json({ v: 1, ok: true, directory_version: version, engine_build: deps.engineBuild ?? null }, 200);
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
