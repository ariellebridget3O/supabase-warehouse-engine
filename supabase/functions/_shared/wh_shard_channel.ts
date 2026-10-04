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

// ---------- the WH_PROXY rawFetch lever (r123, design_r122_acct2_proxy.md §1.3/§1.4) ----------
//
// The engine-side half of the acct2 proxy plane: a boot-validated
// `wh_shard_proxy_map` KV value (the FM 0019 validator MIRRORED here —
// defense in depth; the engine never trusts a value it did not shape-check)
// plus the `proxiedRawFetch` transform (§1.4): (url, init) → POST <proxyUrl>
// with a JSON spec carrying the target url/method/headers/body verbatim,
// WRAPPED in the OWN-REF DIRECT carve-out (r123 P0 fix — the co-hosted law:
// the engine's own shard is intra-project and NEVER rides the cross-account
// proxy; remote shards only). Purity law intact: BOTH pieces are pure — the
// SHELL (warehouse-engine/index.ts) owns every env read and the ONE boot-once
// KV read (⟫B4, timeout-raced — see raceWhProxyKvBoot) and wires this with
// the WH_PROXY_FETCHER / WH_PROXY_TOKEN envs.
//
// ECHO LAW (r57, wh_shard_channel.ts:20-23): every defect log below is a
// FIXED string — the KV value is NEVER echoed, never value fragments
// (a JWT-shaped string in the value would otherwise leak into stdout).

/** The strict proxy-fn URL shape: exactly `https://<20-char-ref>.supabase.co/
 *  functions/v1/proxy` (lowercase ref; default 443; no trailing dot — raw
 *  byte shape, deliberately narrower than DNS identity, ⟫A7 stance). */
const WH_PROXY_FN_URL_RE = /^https:\/\/[a-z0-9]{20}\.supabase\.co\/functions\/v1\/proxy$/;
/** Each refs entry is an EXACT 20-char lowercase project ref. */
const WH_PROXY_REF_RE = /^[a-z0-9]{20}$/;
/** Token NEVER in config — enforced, not just documented (⟫A5): any
 *  JWT-shaped string anywhere in the value rejects the WHOLE value. */
const JWT_SHAPED_RE = /^eyJ/;

/** The validated `wh_shard_proxy_map` value (the shape the FM writer emits). */
export interface WhProxyMapValue {
  /** The acct2 proxy fn URL (engine-side validated: strict shape above). */
  url: string;
  /** The allowlisted WH shard refs the proxy may reach. */
  refs: string[];
}

/**
 * parseWhProxyMapValue — the ENGINE-SIDE mirror of the FM `wh_shard_proxy_map`
 * validator (fm db/migrations/0019_geo_b2_prestage.sql:175-220, ⟫A5; the
 * non-empty-refs arm token-equivalent to the FM 0027 validator):
 *   * value must be a JSON object with EXACTLY the members `{url, refs}`
 *     (extra members REJECTED; url: string; refs: NON-EMPTY array of strings);
 *   * NO JWT-shaped string (`^eyJ`) anywhere in the value — the proxy token
 *     NEVER rides the config KV (the r69 §3.3 doctrine made ENFORCED);
 *   * `url` must match ^https://[a-z0-9]{20}\.supabase\.co/functions/v1/proxy$
 *     AND its host-ref must be ∈ refs (consistency, fail-closed);
 *   * every refs entry must match ^[a-z0-9]{20}$.
 * ANY fault ⇒ null + ONE fixed-string defect log via onDefect (default
 * console.error) — the caller stays inert (lever OFF, default platform
 * fetch). The mirror exists because the KV is WRITTEN by the flip runbook
 * and READ here: a malformed or poisoned value must never arm the lever,
 * even if the FM-side validator regresses (defense in depth).
 */
export function parseWhProxyMapValue(
  value: unknown,
  onDefect?: (msg: string) => void,
): WhProxyMapValue | null {
  const defect = onDefect ?? ((msg: string) => console.error(msg));
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    defect('wh_shard_proxy_map defect class not-an-object: config value is not a JSON object — WH_PROXY_FETCHER=on stays inert (default platform fetch in use)');
    return null;
  }
  const record = value as Record<string, unknown>;
  const members = Object.keys(record).sort();
  if (
    members.length !== 2 || members[0] !== 'refs' || members[1] !== 'url' ||
    typeof record.url !== 'string' || !Array.isArray(record.refs) ||
    record.refs.some((entry) => typeof entry !== 'string')
  ) {
    defect('wh_shard_proxy_map defect class invalid-members: config value is not an object with exactly {url: string, refs: string[]} — WH_PROXY_FETCHER=on stays inert (default platform fetch in use)');
    return null;
  }
  const url = record.url;
  const refs = record.refs as string[];
  if ([url, ...refs].some((s) => JWT_SHAPED_RE.test(s))) {
    defect('wh_shard_proxy_map defect class jwt-shaped-string: a string in the config value is JWT-shaped — the proxy token NEVER rides the config KV — WH_PROXY_FETCHER=on stays inert (default platform fetch in use)');
    return null;
  }
  if (!WH_PROXY_FN_URL_RE.test(url)) {
    defect('wh_shard_proxy_map defect class bad-url: url does not match the acct2 proxy-fn URL shape — WH_PROXY_FETCHER=on stays inert (default platform fetch in use)');
    return null;
  }
  // FM 0027 token-equivalence (r123 fresh-eyes P3): the FM validator requires
  // refs NON-EMPTY — reject the empty array HERE explicitly so the mirrors
  // stay token-equivalent (host-ref ∈ refs below already closed every
  // reachable path transitively; this is the explicit-formality mirror).
  if (refs.length === 0) {
    defect('wh_shard_proxy_map defect class empty-refs: refs must be a NON-EMPTY array — WH_PROXY_FETCHER=on stays inert (default platform fetch in use)');
    return null;
  }
  if (refs.some((entry) => !WH_PROXY_REF_RE.test(entry))) {
    defect('wh_shard_proxy_map defect class bad-ref: a refs entry is not a 20-char lowercase project ref — WH_PROXY_FETCHER=on stays inert (default platform fetch in use)');
    return null;
  }
  // The regex above pins the byte shape `https://<20>.supabase.co/...`, so
  // the host-ref is exactly bytes 8..28 (no URL parse, no normalization —
  // raw-exact matching, ⟫A7).
  const hostRef = url.slice('https://'.length, 'https://'.length + 20);
  if (!refs.includes(hostRef)) {
    defect('wh_shard_proxy_map defect class url-not-in-refs: the url host ref is not listed in refs — WH_PROXY_FETCHER=on stays inert (default platform fetch in use)');
    return null;
  }
  return { url, refs };
}

/** The boot-once KV-read timeout (r123 fresh-eyes P3 fix): a HANGING
 *  PostgREST at isolate boot must not wedge module evaluation (Deno.serve
 *  would never start) with the lever ON. 10s — a healthy PostgREST answers
 *  in milliseconds; a hang degrades to the KV-absent state (lever inert +
 *  ONE fixed-string defect log, shell-owned). */
export const WH_PROXY_KV_BOOT_TIMEOUT_MS = 10_000;

/**
 * raceWhProxyKvBoot (r123 fresh-eyes P3 fix): the boot-once
 * `wh_shard_proxy_map` KV read raced against a wall-clock timeout.
 *   * read settles (resolve OR reject) within timeoutMs ⇒ its outcome
 *     propagates UNCHANGED (a rejection still reaches the shell's try/catch
 *     and its fixed-string defect log — nothing is swallowed here);
 *   * read neither resolves nor rejects within timeoutMs ⇒ resolves with
 *     `timedOut()` — the caller's timeout sentinel (the shell: `null` +
 *     the fixed-string timeout defect log inside the callback), so a
 *     hanging PostgREST degrades to the lever-inert state instead of
 *     wedging module evaluation. The losing read's eventual settlement is
 *     IGNORED (Promise.race already attached handlers — no unhandled
 *     rejection).
 * The timeout timer is ALWAYS cleared once the race settles — a read that
 * wins must not leave a dangling timer (offline-test sanitizer law).
 * Purity: a pure race over the caller's promise — the read itself stays
 * shell-owned (the ONE db() chain, ⟫B4). The kvRead param is PromiseLike
 * (not Promise) because supabase-js query builders are thenables, not
 * Promises; the raced result is `T | U` (the timeout sentinel is a DISTINCT
 * type — the shell discriminates on null, never on a faked read shape).
 */
export function raceWhProxyKvBoot<T, U>(
  kvRead: PromiseLike<T>,
  timeoutMs: number,
  timedOut: () => U,
): Promise<T | U> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<U>((resolve) => {
    timer = setTimeout(() => resolve(timedOut()), timeoutMs);
  });
  return Promise.race([kvRead, timeout]).finally(() => {
    if (timer !== undefined) clearTimeout(timer);
  });
}

/** The rawFetch seam type (RpcShardFetcherDeps.rawFetch, above) restated
 *  structurally so the proxy wrapper is drop-in at the SAME seam — the
 *  fetcher's D7 never-throw arms and the D8 classification consume it
 *  unchanged. */
export type WhProxyRawFetch = (
  url: string,
  init: { method: string; headers: Record<string, string>; body?: string },
) => Promise<{
  ok?: boolean;
  status?: number;
  headers?: { get(name: string): string | null };
  text: () => Promise<string>;
}>;

export interface ProxiedRawFetchDeps {
  /** The acct2 proxy fn URL (parseWhProxyMapValue-validated upstream). */
  proxyUrl: string;
  /** The DEDICATED WH_PROXY_TOKEN (the shell owns the env read; NEVER
   *  WHE_BEARER_TOKEN — the snapshotKey doctrine, index.ts:152-155). */
  proxyToken: string;
  /** Injectable transport (prod: the platform fetch; tests: fakes). */
  fetchImpl?: (
    url: string,
    init: { method: string; headers: Record<string, string>; body: string },
  ) => Promise<{
    ok?: boolean;
    status?: number;
    headers?: { get(name: string): string | null };
    text: () => Promise<string>;
  }>;
}

/**
 * makeProxiedRawFetch (§1.4) — the WH_PROXY lever transform: the shard
 * fetcher's rawFetch dep, rerouted through the acct2 proxy fn:
 *   (url, init) → POST <proxyUrl> {
 *     Authorization: Bearer <WH_PROXY_TOKEN>,   // the OUTER plane auth
 *     Content-Type: application/json,
 *     body: {url, http_method: init.method, headers: init.headers,
 *            body: init.body ?? null, cache: 0, browser_headers: false}
 *   }
 * Contract notes (ALL load-bearing):
 *   * The shard creds (apikey / Authorization service JWT / Accept-Profile)
 *     ride `headers` VERBATIM inside the spec — ep forwards them
 *     (allowAuthorization: true; only hop-by-hop / x-supabase-* dropped).
 *   * `cache: 0` is the NUMBER zero — it defeats ep's 300s default TTL on
 *     BOTH cache layers (WH reads must never serve stale).
 *   * `browser_headers: false` (⟫A9) — without it ep injects
 *     Accept/sec-fetch-* browser-masquerade headers, making the proxied POST
 *     non-byte-equivalent to the direct path.
 *   * NEVER-THROW SEAM CONTRACT (preserved EXACTLY): this wrapper does NOT
 *     catch transport rejections — a rejection propagates to the fetcher's
 *     arm-0 catch-inside (wh_shard_channel.ts, the load-bearing try around
 *     rawFetch) which degrades to the `network` warning. Swallowing here
 *     into a fake Response-like would MISCLASSIFY a network fault as an
 *     http-status warning — the catch stays where the seam puts it.
 *   * The proxy's OWN errors (401/403/429/502 JSON) arrive as ordinary
 *     non-2xx Responses and are returned VERBATIM — the fetcher's non-2xx
 *     warning arm classifies them; nothing here throws on them.
 */
export function makeProxiedRawFetch(deps: ProxiedRawFetchDeps): WhProxyRawFetch {
  const transport = deps.fetchImpl ??
    ((url: string, init: { method: string; headers: Record<string, string>; body: string }) => fetch(url, init));
  return (url, init) => {
    const spec = {
      url,
      http_method: init.method,
      headers: init.headers,
      body: init.body ?? null,
      cache: 0,
      browser_headers: false,
    };
    return transport(deps.proxyUrl, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${deps.proxyToken}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(spec),
    });
  };
}

export interface OwnRefBypassRawFetchDeps {
  /** The engine's OWN project_ref (ownProjectRefFromSupabaseUrl over
   *  SUPABASE_URL — the co-hosted law; the SHELL owns the env read and
   *  already holds this value for the shard-key resolver). '' DISABLES the
   *  bypass arm (AM-8 mirror: an empty own ref never matches — every target
   *  stays on the proxied transport; the shell additionally refuses to ARM
   *  the lever with an unknown own ref, so this factory arm is the
   *  belt-and-braces backstop). */
  ownRef: string;
  /** The lever transform (makeProxiedRawFetch output) for every NON-own
   *  host. */
  proxiedRawFetch: WhProxyRawFetch;
  /** The DEFAULT platform fetch for the OWN host (prod: global fetch;
   *  tests: fakes). Injectable for the offline battery — the prod default
   *  is the same `fetch` the unwrapped fetcher seam uses. */
  directFetch?: WhProxyRawFetch;
}

/**
 * makeOwnRefBypassRawFetch (r123 P0 fix — the live-e2e RED closure): wraps
 * the WH_PROXY lever transform with the OWN-REF DIRECT carve-out. The
 * engine's OWN shard is INTRA-PROJECT (the co-hosted law — engine and own
 * shard share the isolate host), so its traffic must NEVER ride the
 * cross-account proxy: the r123 live e2e (v14 lever ON) showed the own-ref
 * shard fetch through the acct2 proxy drawing a RELAYED 401 "Invalid API
 * key" from the own host's public edge for the own-ref
 * SUPABASE_SERVICE_ROLE_KEY, while the SAME credential succeeds on the
 * isolate-internal direct path (the pre-r123 behavior). The carve-out:
 *   * TARGET host is the engine's OWN project host — the target url
 *     BYTE-EXACTLY starts with `https://<ownRef>.supabase.co/` ⇒ the
 *     DEFAULT platform fetch receives (url, init) VERBATIM. The match is
 *     the ⟫A7 raw-exact stance: no URL parse, no normalization, and the
 *     trailing `/` in the needle is LOAD-BEARING (kills the r69 M2
 *     prefix-mutant class — `https://<ownRef>.supabase.co.evil.io/` and a
 *     ref-EXTENDED spelling never match). A case/trailing-dot/port
 *     variation is a MISS and stays on the proxy (fail-closed conservative
 *     — the engine builds shard URLs canonically from lowercase directory
 *     refs, exactly as parseWhProxyMapValue argues);
 *   * EVERY other host (the remote shards — the acct2 egress-pool purpose)
 *     ⇒ the proxied transform, byte-identical to the unwrapped lever.
 * NEVER-THROW SEAM CONTRACT preserved EXACTLY: the wrapper catches NOTHING
 * and adds no fallible parse (a raw prefix test, never `new URL`) —
 * transport rejections from EITHER transport propagate to the fetcher's
 * arm-0 catch (the `network` warning), unchanged.
 */
export function makeOwnRefBypassRawFetch(deps: OwnRefBypassRawFetchDeps): WhProxyRawFetch {
  const direct = deps.directFetch ??
    ((url: string, init: { method: string; headers: Record<string, string>; body?: string }) => fetch(url, init));
  // '' ownRef disables the arm (AM-8 mirror) — the needle stays null and can
  // never match (belt-and-braces; the shell refuses to arm on '' too).
  const ownHostPrefix = deps.ownRef === '' ? null : `https://${deps.ownRef}.supabase.co/`;
  return (url, init) =>
    ownHostPrefix !== null && url.startsWith(ownHostPrefix)
      ? direct(url, init)
      : deps.proxiedRawFetch(url, init);
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
