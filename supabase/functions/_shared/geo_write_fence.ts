// =============================================================================
// _shared/geo_write_fence.ts — the ENGINE WRITE-PLANE FENCE GATE (r49, B2
// wave 1, G-W1..W5) + the R3 dispatch identity.
// =============================================================================
// Normative source: research/findings_geo_failover_design.md §2 (the fence
// design, G-W1..W5, the 503 wire shape block), D28 (enforcement locus =
// engine entrypoint pre-dispatch on WRITE-PLAN requests, per-request config
// read, fail-closed), R3 (override==own ⇒ ENGINE-LOCAL dispatch; READS
// covered per re-audit #6), R4 (the per-request config read serializes
// FM-ENGINE-mediated writes only — direct-to-P writers are the A1 DB leg).
//
// Surface:
//   * fetchFenceConfig(supabase) — the ONE combined config read (G-W1):
//     `.in('key', ['geo_read_only','geo_write_epoch','geo_primary_override'])`
//     — one round trip; the override key joins the same read per R3/re-audit
//     #3e (the dispatch identity reads it from the same combined read).
//   * geoWriteGate(fence, req) — the PURE gate core, G-W1..W5, fail-closed on
//     every branch. The call site (wh_entrypoint.handleQuery, behind
//     isWritePlan) maps the verdicts: G-W1 throw ⇒ 500 (never 503); a fenced
//     verdict ⇒ fenceReadOnlyBody(qid) — the §2 wire shape VERBATIM; an
//     accepted verdict carries the G-W5 echo (the observed epoch) for the
//     future write branch's response.
//   * resolveWriteDispatch / resolveReadDispatch — the R3 dispatch identity:
//     override == own ref ⇒ ENGINE-LOCAL (the caller executes against its own
//     co-hosted DB — no self-POST), override == other ⇒ dispatch per
//     override, override absent ⇒ as-built placements dispatch. READS TOO
//     (re-audit #6): legacy (no read_plane) primary-plane requests resolve
//     primary=FM via the override and dispatch engine-locally with the B2
//     warning detail token 'geo_promoted_primary_local' (loud, never silent).
//   * isWritePlan(rawBody) — the write-plan predicate (the gate call site's
//     only trigger). A request is a WRITE PLAN iff its plan declares mutating
//     ops (the scatter-INSERT/UPSERT envelope, scatter §5.2).
//   * ownProjectRefFromSupabaseUrl(url) — the engine's own project_ref (the
//     SUPABASE_URL subdomain, the co-hosted law); '' = unparseable ⇒ the
//     dispatch identity degrades to placements (a wrong own-ref could
//     engine-local a REMOTE override into a self-dispatch — never guess).
//
// Purity: everything except fetchFenceConfig is PURE (wh_engine_core-style
// injected-deps law). The dispatch-instruction type lives in wh_engine_core
// (the consumer — its wh_* purity island is untouched; this import is
// type-only).
// =============================================================================

import type { WhEngineWarning, WhGeoReadDispatch } from './wh_engine_core.ts';

// ---------- the combined config read (G-W1) ----------------------------------

/** The three fence keys, in the pinned query order (the battery asserts the
 *  exact `.in()` shape — ONE round trip, three keys, no second query). */
export const FENCE_CONFIG_KEYS = ['geo_read_only', 'geo_write_epoch', 'geo_primary_override'] as const;

/** Minimal structural shape of the supabase-js client surface the fence read
 *  needs (PostgREST builder chain, thenable) — keeps the module offline-
 *  testable with a recording fake (the wh_testutil convention). */
export interface WhFenceQueryResult {
  data: { key: string; value: unknown }[] | null;
  error: { message: string } | null;
}

export interface WhFenceClient {
  from(table: string): {
    select(cols: string): {
      in(col: string, values: readonly string[]): PromiseLike<WhFenceQueryResult>;
    };
  };
}

/** The RAW config values (jsonb rendered by PostgREST: jsonb number ⇒ JS
 *  number, string ⇒ string, bool ⇒ bool, null ⇒ null). Absent row ⇒
 *  undefined — the gate decides what absence means (G-W2/G-W3 fence, the
 *  override absent ⇒ placements). */
export interface WhFenceValues {
  read_only: unknown;
  write_epoch: unknown;
  primary_override: unknown;
}

/** The fence-read result. ok:false is the G-W1 class: UNREADABLE config —
 *  the gate throws it verbatim ⇒ 500, never masqueraded as a fence verdict. */
export type WhFenceRead =
  | { ok: true; values: WhFenceValues }
  | { ok: false; error: unknown };

/**
 * The ONE combined per-request config read (§2 D28: "ONE combined query ...
 * one round trip"). NEVER widened to a second query — the override rides the
 * same read (R3/re-audit #3e). A transport throw and a PostgREST error are
 * both the G-W1 class (ok:false) — never a fabricated verdict.
 */
export async function fetchFenceConfig(supabase: WhFenceClient): Promise<WhFenceRead> {
  let res: WhFenceQueryResult;
  try {
    res = await supabase.from('config').select('key,value').in('key', FENCE_CONFIG_KEYS);
  } catch (e) {
    return { ok: false, error: e };
  }
  if (res.error !== null) return { ok: false, error: res.error };
  const values: WhFenceValues = {
    read_only: undefined,
    write_epoch: undefined,
    primary_override: undefined,
  };
  for (const row of res.data ?? []) {
    if (row.key === 'geo_read_only') values.read_only = row.value;
    else if (row.key === 'geo_write_epoch') values.write_epoch = row.value;
    else if (row.key === 'geo_primary_override') values.primary_override = row.value;
  }
  return { ok: true, values };
}

// ---------- the gate core (G-W1..W5, pure) -----------------------------------

export type WhWriteGateVerdict =
  | { ok: true; writeEpoch: number }
  | { ok: false; code: 'read_only_mode' };

// ONE collapsed 503 verdict — G-W2/G-W3/G-W4 all name the SAME pre-named wire
// code with the SAME pinned message (§2: no new warnings, zero new codes D7).
const FENCED: WhWriteGateVerdict = { ok: false, code: 'read_only_mode' };

/** The §2 503 message — VERBATIM from findings_geo_failover_design.md §2 (the
 *  wire-shape block). One constant so the battery pins it byte-exact. */
export const FENCE_503_MESSAGE =
  'geo read-only: writes are fenced; re-discover before resuming (stale-write guard, geo doc §2.3 #4)';

/**
 * The §2 503 wire shape, VERBATIM:
 *   status 503, body { "v": 1, "qid": "<uuid>",
 *     "error": { "code": "read_only_mode",
 *                "message": FENCE_503_MESSAGE } }
 * No new warning is introduced — the engine-plane fallback warnings stay as
 * pinned (contract:478). Single construction site (the entrypoint call site
 * and the battery both use this).
 */
export function fenceReadOnlyBody(qid: string | null): {
  v: 1;
  qid: string | null;
  error: { code: 'read_only_mode'; message: string };
} {
  return { v: 1, qid, error: { code: 'read_only_mode', message: FENCE_503_MESSAGE } };
}

/**
 * The PURE gate core (§2 G-W1..W5, fail-closed on every branch). `fence` is
 * the ALREADY-READ config (fetchFenceConfig's result) so this function does
 * no I/O; `req.write_epoch` is the additive optional client carry off the
 * raw request body (G-W4).
 *
 * Branch order is the §2 pin: G-W1 (observe) → G-W2 (read_only fence) →
 * G-W3 (epoch presence fence) → G-W4 (epoch carry) → G-W5 (echo on accept).
 */
export function geoWriteGate(fence: WhFenceRead, req: { write_epoch?: unknown }): WhWriteGateVerdict {
  // G-W1 (observe): a read error throws VERBATIM → the call site maps it to
  // 500, NEVER 503 (never masquerade unreadable config as
  // unwritable-or-writable — the geo doc L99 residue discipline). The SAME
  // error object propagates (no wrapping, no rewording).
  if (!fence.ok) throw fence.error;
  const { read_only, write_epoch } = fence.values;

  // G-W2 (read_only fence): `geo_read_only !== literal false` ⇒ fenced.
  // Strict-boolean fail-closed — the exact mirror of geo_resolve.ts:91
  // ('true' string ≠ true): anything that is not the literal jsonb false
  // fences ('true', 0, 1, null, true, absent). The 0017 seed guarantees
  // false exists (0017:120–123).
  if (read_only !== false) return FENCED;

  // G-W3 (epoch presence fence): epoch row absent OR jsonb_typeof ≠ 'number'
  // ⇒ fenced. An engine that cannot vouch for the fence does not write. (The
  // 0019 seed makes epoch=0 present from day one; fail-closed costs nothing
  // because the deploy gate already blocks all traffic pre-flip, D13.
  // PostgREST renders jsonb numbers as JS numbers, so typeof is the
  // jsonb_typeof check.)
  if (typeof write_epoch !== 'number') return FENCED;

  // G-W4 (epoch carry): the request MAY carry additive optional `write_epoch`
  // (integer). Present and < config epoch ⇒ fenced (stale writer: cached
  // pre-promotion truth — the split-brain stopper). Present and > config
  // epoch ⇒ fenced (a request claiming a NEWER epoch than the config is
  // unvouchable — refuse, never best-effort parse, contract:474 law).
  // NON-INTEGER carries refuse too (1.5 / '1' / null — unvouchable, never
  // best-effort parsed). Absent ⇒ the engine's own observation governs
  // (making client-carry mandatory would break the additive-extension
  // contract, r40/r47 precedent "old callers untouched").
  if (req.write_epoch !== undefined) {
    const carried = req.write_epoch;
    if (typeof carried !== 'number' || !Number.isInteger(carried)) return FENCED;
    if (carried < write_epoch) return FENCED;
    if (carried > write_epoch) return FENCED;
  }

  // G-W5 (echo): the ACCEPTED path returns the observed epoch — the write
  // branch echoes it additively on the accepted write-batch response
  // (discovery gains the same additive top-level field, §6). Implemented as
  // part of the gate's return contract so the write wave cannot land without
  // the echo in hand.
  return { ok: true, writeEpoch: write_epoch };
}

// ---------- the R3 dispatch identity (writes AND legacy reads) ----------------

export type WhWriteDispatch =
  | { kind: 'placements' }
  | { kind: 'engine_local'; target: string }
  | { kind: 'remote'; target: string };

/** The shared identity rule (R3): override absent/empty/non-string ⇒ null
 *  (as-built placements); override == own ref ⇒ engine-local; override ==
 *  another ref ⇒ dispatch per override. A non-string override (number/
 *  object — the 0019 validator makes these un-storable, but a drifted row is
 *  not a dispatch instruction) is fail-safe placements, never a guess. An
 *  UNKNOWN own ref ('') is the same fail-safe: engine-localing a remote
 *  override would self-dispatch. */
function resolveDispatchIdentity(
  primaryOverride: unknown,
  ownProjectRef: string,
): { isOwn: boolean; target: string } | null {
  if (typeof ownProjectRef !== 'string' || ownProjectRef === '') return null;
  if (typeof primaryOverride !== 'string' || primaryOverride === '') return null;
  return primaryOverride === ownProjectRef
    ? { isOwn: true, target: ownProjectRef }
    : { isOwn: false, target: primaryOverride };
}

/**
 * WRITE dispatch identity (R3): override == ownProjectRef ⇒ ENGINE-LOCAL —
 * the caller executes the write batch against its own co-hosted DB (direct
 * connection, no self-POST; the engine-local execution itself is the future
 * write branch's job — this returns the INSTRUCTION). override == other ref
 * ⇒ dispatch per override. override absent ⇒ as-built placements dispatch.
 */
export function resolveWriteDispatch(values: WhFenceValues, ownProjectRef: string): WhWriteDispatch {
  const identity = resolveDispatchIdentity(values.primary_override, ownProjectRef);
  if (identity === null) return { kind: 'placements' };
  return identity.isOwn
    ? { kind: 'engine_local', target: identity.target }
    : { kind: 'remote', target: identity.target };
}

/** The B2-specific warning detail token (R3/re-audit #6): LOUD, never
 *  silent — the legacy-read fate is engine-local dispatch and the response
 *  NAMES it. */
export const GEO_PROMOTED_PRIMARY_LOCAL = 'geo_promoted_primary_local';

/**
 * READ dispatch identity (R3/re-audit #6): legacy (no read_plane)
 * primary-plane requests when override == own ⇒ ENGINE-LOCAL + the B2
 * warning detail token. The warning follows the engine's WhEngineWarning
 * plane-event shape EXACTLY (wh_engine_core's geo_fallback_primary precedent:
 * the pinned geo-plane code on this wire + a machine detail token;
 * est_rows 0 — plane events never bias SUM/COUNT partials; no NEW warning
 * code — D7/D34 zero-new-codes law). override == other ⇒ dispatch per
 * override (normal post-promotion serving — no warning). override absent ⇒
 * as-built placements dispatch — the pre-r49 behavior, byte-identical.
 */
export function resolveReadDispatch(values: WhFenceValues, ownProjectRef: string): WhGeoReadDispatch {
  const identity = resolveDispatchIdentity(values.primary_override, ownProjectRef);
  if (identity === null) return { kind: 'placements' };
  if (identity.isOwn) {
    return {
      kind: 'engine_local',
      target: identity.target,
      warning: {
        shard: identity.target,
        code: 'geo_fallback_primary',
        est_rows: 0, // plane events never bias SUM/COUNT partials
        retried: false,
        detail: GEO_PROMOTED_PRIMARY_LOCAL,
      },
    };
  }
  return { kind: 'remote', target: identity.target };
}

// ---------- the write-plan predicate (the gate call site's trigger) -----------

/** Mutating op vocabulary (scatter §5.2 scatter-INSERT/UPSERT envelope). v0's
 *  /query op union is sum|count|min|max|avg (wh_engine_core.ts AGG_OPS) —
 *  NONE of these are mutating, so no parsed v0 request can trip this arm.
 *  When the write wave lands its op vocabulary, THIS set is the locus that
 *  must recognize it (the gate call site is already wired). */
const MUTATING_PLAN_OPS: ReadonlySet<string> = new Set(['insert', 'upsert', 'update', 'delete']);

/**
 * The WRITE-PLAN predicate (§2 D28: "a request is a WRITE PLAN iff its plan
 * declares mutating ops (the scatter-INSERT/UPSERT envelope, scatter §5.2)").
 * Operates on the RAW body — the §4.3 parse drops unknown fields, so the
 * gate must see the raw envelope before parse (the §5.2 idempotency_key
 * marker is parse-invisible today).
 *
 * Arms (any hit ⇒ write plan ⇒ the entrypoint runs G-W1..W5 before anything
 * else):
 *   1. a query.select entry declares a MUTATING op (the write wave's op
 *      vocabulary — insert/upsert/update/delete);
 *   2. the scatter §5.2 write-batch marker `idempotency_key` (the client-
 *      supplied dedupe partner — "client may supply idempotency_key",
 *      scatter §5.2) is present.
 *
 * NOT an arm: a bare `write_epoch` carry. G-W4's carry is meaningful ONLY on
 * the write plane, but reads must NEVER consult the fence (§2: "Reads (both
 * planes) never consult the fence") — a read-shaped body carrying the
 * additive field is served as a read (parse drops it); the gate still sees
 * the carry whenever a write marker routes the body through (the entrypoint
 * forwards rawBody.write_epoch into the gate).
 *
 * v0 REALITY (pinned by battery): arm 1 is structurally unreachable for a
 * v0-parsable request (the op union has no mutating ops — parse rejects
 * them); arm 2 fires only on a body carrying the §5.2 write marker. The
 * gate call site exists and is wired TODAY so the write wave cannot land
 * ungated.
 */
export function isWritePlan(rawBody: unknown): boolean {
  if (rawBody === null || typeof rawBody !== 'object' || Array.isArray(rawBody)) return false;
  const body = rawBody as Record<string, unknown>;
  if (body.idempotency_key !== undefined) return true;
  const query = body.query;
  if (query !== null && typeof query === 'object' && !Array.isArray(query)) {
    const select = (query as Record<string, unknown>).select;
    if (Array.isArray(select)) {
      for (const entry of select) {
        if (entry !== null && typeof entry === 'object' && !Array.isArray(entry)) {
          const op = (entry as Record<string, unknown>).op;
          if (typeof op === 'string' && MUTATING_PLAN_OPS.has(op)) return true;
        }
      }
    }
  }
  return false;
}

// ---------- the engine's own project ref (the co-hosted law) ------------------

/**
 * The engine's OWN project_ref: the SUPABASE_URL subdomain (the engine is
 * deployed on exactly ONE project — the co-hosted/deploy-once law, DEPLOY.md).
 * Returns '' when unparseable ⇒ the dispatch identity degrades to placements
 * (resolveDispatchIdentity's fail-safe): a WRONG own-ref would engine-local a
 * REMOTE override into a self-dispatch — never guess.
 */
export function ownProjectRefFromSupabaseUrl(url: string): string {
  const m = /^[a-zA-Z][a-zA-Z0-9+.-]*:\/\/([a-zA-Z0-9-]+)\./.exec(url);
  return m !== null ? m[1] : '';
}
