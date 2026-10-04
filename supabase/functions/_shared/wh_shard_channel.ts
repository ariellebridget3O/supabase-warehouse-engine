// =============================================================================
// _shared/wh_shard_channel.ts — the shard SERVICE-KEY channel + the RPC REAL
// FETCHER (r69, design_r69_shard_channel.md §3.3/§3.4 — D4/D5/D6/D7/D8/D9).
// =============================================================================
// Normative source: design_r69_shard_channel.md §3.3 (the key channel:
// D4 resolution order, D5 WH_SHARD_KEYS fail-closed parse, D6 single source
// of truth) + §3.4 (the real fetcher: D7 never-throw arms, D8 stamped/whCode
// classification inputs, D9 est_rows) + §6.1 of design_wh_query_rpc.md (the
// byte-pinned POST /rest/v1/rpc/wh_query call shape, P≤64KB, §4.5).
//
// PURITY LAW (wh_* island): this module NEVER reads Deno.env — every input
// (own ref, own key, remote key map, transport, byte cap) is a wiring-owned
// dep. The thin warehouse-engine/index.ts owns the env reads (parseShardKeyEnv
// over WH_SHARD_KEYS, SUPABASE_URL/SUPABASE_SERVICE_ROLE_KEY for the
// resolver deps) and wires ONE resolver instance feeding BOTH the §5.2
// handshake plane auth AND this fetcher (D6 single source of truth).
//
// FAIL-CLOSED LAW: every failure mode returns a {ok:false, warning} outcome
// — the fetcher NEVER throws and NEVER rejects (runFanout's throw-class is
// the programmer-error backstop, not a wire outcome). parseShardKeyEnv fails
// closed to an empty map with ONE fixed-string defect-class log (never value
// fragments, never err.message — the r57 echo law: JSON.parse errors echo
// input fragments).
// =============================================================================

import { WhEngineError } from './wh_engine_core.ts';
import type { WhRpcSpec, WhShardFetcher } from './wh_engine_core.ts';

// ---------- the key channel (D4/D5, §3.3) ----------

export interface ShardKeyResolverDeps {
  /** The engine's OWN project_ref (ownProjectRefFromSupabaseUrl over
   *  SUPABASE_URL — the co-hosted law). '' = unparseable ⇒ the own-ref arm
   *  is DISABLED (AM-8): the own shard then resolves via remoteKeys or
   *  throws shard_key_missing naming the ref. */
  ownRef: string;
  /** SUPABASE_SERVICE_ROLE_KEY (runtime-injected). Empty ⇒ the own-ref arm
   *  THROWS shard_key_missing (never returns ''). */
  ownKey: string;
  /** The parsed WH_SHARD_KEYS map (parseShardKeyEnv — wiring-owned). */
  remoteKeys: ReadonlyMap<string, string>;
}

/** Resolves a shard's service key (D4): own-ref exact NON-EMPTY case-
 *  preserved match → ownKey; else remoteKeys.get; miss → THROW
 *  WhEngineError('internal', 'shard_key_missing: <shard>') — the message
 *  names the REF (fault why-tokens may name refs; enum strings carry no
 *  credential material). NEVER returns ''. */
export type ShardKeyResolver = (shard: string) => string;

export function makeShardKeyResolver(deps: ShardKeyResolverDeps): ShardKeyResolver {
  return (shard: string): string => {
    // own-ref arm: exact, case-preserved, and only when ownRef is non-empty
    // (an empty ownRef never matches — AM-8). The co-hosted law: the own
    // ref resolves with the engine's OWN service key (geo_readplane
    // precedent). Empty ownKey fails CLOSED (never returns '').
    if (deps.ownRef !== '' && shard === deps.ownRef) {
      if (deps.ownKey === '') {
        throw new WhEngineError('internal', `shard_key_missing: ${shard}`);
      }
      return deps.ownKey;
    }
    const remote = deps.remoteKeys.get(shard);
    // A '' remote value is a miss (never returns ''); keys/values are kept
    // verbatim (service keys are JWTs — never trimmed/case-normalized).
    if (remote !== undefined && remote !== '') return remote;
    throw new WhEngineError('internal', `shard_key_missing: ${shard}`);
  };
}

/**
 * parseShardKeyEnv (D5): the WH_SHARD_KEYS value — a JSON object
 * `{"<project-ref>": "<service-role-key>", ...}` — into the key map.
 *   * undefined/'' ⇒ empty map (a NORMAL state: absent secret ⇒ post-flip
 *     remote shards warn shard_key_missing; no defect log — the flip
 *     runbook's secret-set checklist carries it);
 *   * unparseable JSON / non-object (incl. arrays and null) / non-string
 *     value ⇒ WHOLE-MAP REJECT: empty map + ONE defect-class log via
 *     onDefect (default console.error; AM-11 — battery observes via the
 *     callback, no stdout scraping). ONE line per isolate boot (OQ-7).
 * The log is a FIXED string per defect class — env name + defect class,
 * NEVER value fragments and NEVER err.message (r57 echo law). Duplicate
 * keys last-wins (JSON.parse semantics); trimmed keys are preserved
 * verbatim (service keys are JWTs — never trimmed).
 */
export function parseShardKeyEnv(
  raw: string | undefined,
  onDefect?: (msg: string) => void,
): Map<string, string> {
  const defect = onDefect ?? ((msg: string) => console.error(msg));
  if (raw === undefined || raw === '') return new Map();
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    defect('WH_SHARD_KEYS defect class unparseable-json: secret value is not valid JSON — failing closed to an empty key map (every remote shard will warn shard_key_missing)');
    return new Map();
  }
  if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) {
    defect('WH_SHARD_KEYS defect class not-an-object: secret value is not a JSON object — failing closed to an empty key map (every remote shard will warn shard_key_missing)');
    return new Map();
  }
  const out = new Map<string, string>();
  for (const [key, value] of Object.entries(parsed as Record<string, unknown>)) {
    if (typeof value !== 'string') {
      // whole-map REJECT (D5 pin): one defect-class log, empty map.
      defect('WH_SHARD_KEYS defect class non-string-value: a key value is not a string — whole-map reject, failing closed to an empty key map');
      return new Map();
    }
    out.set(key, value);
  }
  return out;
}

// ---------- the real fetcher (D7/D8/D9, §3.4) ----------

/** Default response byte cap: P≤64KB (wh_query contract §4.5). */
export const DEFAULT_MAX_PARTIAL_BYTES = 65536;

export interface RpcShardFetcherDeps {
  /** The shard key resolver — the SAME instance the handshake uses (D6
   *  single source of truth). Throws shard_key_missing on a miss (caught
   *  into the warning, arm 1). */
  resolveKey: ShardKeyResolver;
  /** Injectable transport (prod: the platform fetch; tests: fakes). A
   *  rejection is caught INSIDE the fetcher (arm 0 — load-bearing: the
   *  fetcher never rejects). */
  rawFetch?: (
    url: string,
    init: { method: string; headers: Record<string, string>; body?: string },
  ) => Promise<{
    ok?: boolean;
    status?: number;
    headers?: { get(name: string): string | null };
    text: () => Promise<string>;
  }>;
  /** Response byte cap, measured in UTF-8 BYTES via TextEncoder (not UTF-16
   *  code units — text group keys are legal). Default 65536. */
  maxPartialBytes?: number;
}

const WH_BODY_CODE_RE = /^WH[0-9]{3}$/;

/** Strict WH body-code shape guard (D8 arm 4, AM-5/AM-12/OQ-6): parse the
 *  error body ONCE, ONLY on the 400-class or a 5xx whose content-type is
 *  application/json; accept ONLY an object whose `code` matches
 *  ^WH[0-9]{3}$. The parse exists SOLELY as a detail carrier (on a 400 there
 *  is no envelope arm) — the class decision stays status/header-based (r2
 *  law; body text NEVER interpolated, r57 echo law). Anything else ⇒
 *  undefined (plain http class). */
function parseWhBodyCode(text: string, eligible: boolean): string | undefined {
  if (!eligible) return undefined;
  let parsed: unknown;
  try {
    parsed = JSON.parse(text);
  } catch {
    return undefined;
  }
  if (parsed === null || typeof parsed !== 'object' || Array.isArray(parsed)) return undefined;
  const code = (parsed as Record<string, unknown>).code;
  return typeof code === 'string' && WH_BODY_CODE_RE.test(code) ? code : undefined;
}

/** The FLAT §6.2 wire crash-gate (D8 arm 5, rpc arm, 2xx): exactly what the
 *  seeded fn emits (db/shard-migrations/0015_wh_query_rpc.sql:389-404
 *  rows-kind, :418-432 scalar-kind) — object with v===1, string table,
 *  number schema_version, boolean truncated, number rowCount, object aggs;
 *  rows-kind additionally kind 'grouped'|'topk' + rows array; scalar-kind
 *  additionally the single-row body object under `partial`. A crash-guard
 *  ONLY — the deep gates (schema_mismatch / F2 / F14 / wh_merge) stay
 *  engine-side UNTOUCHED; this adds no second validator. */
function isWhRpcWireShape(wire: unknown): boolean {
  if (wire === null || typeof wire !== 'object' || Array.isArray(wire)) return false;
  const w = wire as Record<string, unknown>;
  if (w.v !== 1) return false;
  if (typeof w.table !== 'string') return false;
  if (typeof w.schema_version !== 'number') return false;
  if (typeof w.truncated !== 'boolean') return false;
  if (typeof w.rowCount !== 'number') return false;
  if (w.aggs === null || typeof w.aggs !== 'object' || Array.isArray(w.aggs)) return false;
  if (w.kind === 'grouped' || w.kind === 'topk') return Array.isArray(w.rows);
  if (w.kind === 'scalar') return w.partial !== null && typeof w.partial === 'object';
  return false;
}

/**
 * makeRpcShardFetcher (D7/D8/D9) — the RPC-primary real fetcher. Returns a
 * WhShardFetcher (3-arg widened). ALL failure arms return
 * `{ok:false, warning}` — the fetcher NEVER throws, never rejects:
 *   0. transport throw (rawFetch rejection or text() failure) ⇒ network
 *      (caught INSIDE — load-bearing);
 *   1. key-resolve throw ⇒ shard_key_missing (classify ⇒ excluded);
 *   2. rpc present ⇒ POST the §6.1 shape (body JSON.stringify({p_template_hash,
 *      p_params}), headers Accept-Profile public + apikey + Authorization
 *      Bearer + Content-Type application/json — §6.1 method/URL/body + the
 *      engine plane law SHARD_REQUEST_HEADERS for Accept-Profile); rpc
 *      absent ⇒ GET same minus Content-Type (the residual select arm —
 *      post-flip reachable ONLY from the replica/geo-dispatch planes);
 *   3. byte cap over maxPartialBytes (UTF-8 bytes) ⇒ abort_on_oversize;
 *   4. non-2xx ⇒ {ok:false, warning:{httpStatus, stamped?, code?}} —
 *      stamped (ADVISORY-ONLY, AM-14) = content-type application/json on
 *      5xx-class ONLY (header-PRESENCE law; below 5xx the field is ABSENT);
 *      code = the WH[0-9]{3} body detail carrier (parseWhBodyCode);
 *   5. 2xx: select arm ⇒ shard_path_select_disabled (a raw PostgREST array
 *      cannot honestly stamp schema_version — degrade loudly, never lie);
 *      rpc arm ⇒ crash-gate the flat wire (isWhRpcWireShape) and return it
 *      RAW as `envelope` (NO engine-shape stamping here — the consumption
 *      site's adaptWireEnvelope converts) with estRows := rowCount ?? 0;
 *      any shape failure ⇒ envelope_invalid.
 */
export function makeRpcShardFetcher(deps: RpcShardFetcherDeps): WhShardFetcher {
  const rawFetch = deps.rawFetch ??
    ((url: string, init: { method: string; headers: Record<string, string>; body?: string }) => fetch(url, init));
  const maxPartialBytes = deps.maxPartialBytes ?? DEFAULT_MAX_PARTIAL_BYTES;
  return async (shard: string, url: string, rpc?: WhRpcSpec) => {
    // (1) key resolve — a throw degrades to the shard_key_missing warning.
    let key: string;
    try {
      key = deps.resolveKey(shard);
    } catch {
      return { ok: false as const, warning: { code: 'shard_key_missing' } };
    }
    // (2) the request build: rpc present ⇒ POST (the §6.1 call shape);
    // absent ⇒ the residual select-shape GET (same auth minus Content-Type).
    const authHeaders: Record<string, string> = {
      'Accept-Profile': 'public',
      apikey: key,
      Authorization: `Bearer ${key}`,
    };
    const init = rpc !== undefined
      ? {
        method: 'POST',
        headers: { ...authHeaders, 'Content-Type': 'application/json' },
        body: JSON.stringify({ p_template_hash: rpc.p_template_hash, p_params: rpc.p_params }),
      }
      : { method: 'GET', headers: authHeaders };
    // (0) transport throw arm — the catch-inside is LOAD-BEARING.
    let res: Awaited<ReturnType<typeof rawFetch>>;
    try {
      res = await rawFetch(url, init);
    } catch {
      return { ok: false as const, warning: { code: 'network' } };
    }
    let text: string;
    try {
      text = await res.text();
    } catch {
      return { ok: false as const, warning: { code: 'network' } };
    }
    // (3) the byte cap — UTF-8 bytes (TextEncoder), never UTF-16 units.
    if (new TextEncoder().encode(text).length > maxPartialBytes) {
      return { ok: false as const, warning: { code: 'abort_on_oversize' } };
    }
    const status = typeof res.status === 'number' ? res.status : undefined;
    const non2xx = res.ok === false || (status !== undefined && (status < 200 || status > 299));
    if (non2xx) {
      // (4) D8 classification inputs. stamped: header-PRESENCE law, 5xx-class
      // only (the platform gateway does not emit PostgREST headers — but a
      // gateway-generated 5xx CAN carry application/json: the known false-
      // positive class; the flag is advisory-only and never gates a
      // decision, AM-14). Below 5xx the field is ABSENT. Body content is
      // never read for classification — the ONE carve-out is the WH-code
      // detail carrier below (strict shape guard, never the class decision).
      const is5xx = status !== undefined && status >= 500 && status <= 599;
      const stamped = is5xx
        ? res.headers?.get('content-type')?.includes('application/json') === true
        : undefined;
      const is4xx = status !== undefined && status >= 400 && status <= 499;
      const whCode = parseWhBodyCode(text, is4xx || (is5xx && stamped === true));
      return {
        ok: false as const,
        warning: {
          ...(status !== undefined ? { httpStatus: status } : {}),
          ...(stamped !== undefined ? { stamped } : {}),
          ...(whCode !== undefined ? { code: whCode } : {}),
        },
      };
    }
    // (5) 2xx — the select arm refuses (OQ-2 degrade-not-lie); the rpc arm
    // crash-gates the flat wire and returns it RAW.
    if (rpc === undefined) {
      return { ok: false as const, warning: { code: 'shard_path_select_disabled' } };
    }
    let wire: unknown;
    try {
      wire = JSON.parse(text);
    } catch {
      return { ok: false as const, warning: { code: 'envelope_invalid' } };
    }
    if (!isWhRpcWireShape(wire)) {
      return { ok: false as const, warning: { code: 'envelope_invalid' } };
    }
    const shaped = wire as { rowCount?: number };
    return { ok: true as const, envelope: wire, estRows: shaped.rowCount ?? 0 };
  };
}
