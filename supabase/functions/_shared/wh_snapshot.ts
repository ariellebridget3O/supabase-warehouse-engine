// =============================================================================
// _shared/wh_snapshot.ts — §4.6 directory snapshot protocol (HMAC-SHA256)
// =============================================================================
// Contract: research/findings_wh_catalog_contract.md §4.6 (normative):
//   * Transport: BODY field only — `directory_snapshot` on the response (the
//     engine attaches) and on the next request (the client replays). One
//     transport, byte-pinned; no header variant.
//   * Encoding: base64(json({version, issued_at, embed-payload})) + "." +
//     base64(HMAC-SHA256(<signed bytes>, key=engine secret)).
//   * SIGNED BYTES (pinned interpretation, erratum §4.6): the exact received
//     payload-segment bytes — byte-exact as received, no JSON
//     re-serialization, no whitespace/URL normalization (the fixed-byte
//     tamper class). Verify HMACs the raw substring, never a re-encoded copy.
//   * Verify chain (order is contract): signature -> TTL (issued_at <= 60s
//     old; a 5s future-skew tolerance covers isolate clock drift) ->
//     version == current directory_version (one cheap config probe upstream).
//   * ANY failure => silent full embed — the CALLER recomputes the directory
//     read and returns a fresh snapshot. A bad snapshot NEVER errors the
//     request: fail-open to correctness, fail-closed to trust (unsigned/
//     spoofed payloads are ignored, not honored). Hence verify NEVER throws:
//     every failure is a structured {ok:false, reason}.
//   * KEY: a DEDICATED engine secret (WH_SNAPSHOT_KEY), never the
//     WHE_BEARER_TOKEN — the client knows WHE_BEARER_TOKEN, and a client that could
//     forge valid snapshots would break the trust model entirely.
//
// signSnapshotBlob is the permissive low-level signer (pure crypto over a
// given blob — the engine-bug class "validly signed garbage" is reachable
// with it, and the battery proves verify still rejects those). The high-level
// signDirectorySnapshot builds the well-formed blob.
// =============================================================================

export const DEFAULT_SNAPSHOT_MAX_AGE_MS = 60_000;
export const SNAPSHOT_FUTURE_SKEW_MS = 5_000;

export interface WhSnapshotVerifyOk {
  ok: true;
  version: number;
  issuedAt: string;
  payload: unknown;
}
export interface WhSnapshotVerifyFail {
  ok: false;
  reason: string;
}
export type WhSnapshotVerifyResult = WhSnapshotVerifyOk | WhSnapshotVerifyFail;

// ---------- base64 (UTF-8-safe, chunked — embeds can be tens of KB) ----------

function b64encodeUtf8(s: string): string {
  const bytes = new TextEncoder().encode(s);
  let bin = '';
  for (let i = 0; i < bytes.length; i += 0x8000) {
    bin += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
  }
  return btoa(bin);
}

function b64encodeBytes(bytes: Uint8Array): string {
  let bin = '';
  for (let i = 0; i < bytes.length; i += 0x8000) {
    bin += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
  }
  return btoa(bin);
}

function b64decodeToBytes(s: string): Uint8Array {
  const bin = atob(s); // throws on invalid base64 — callers treat as reject
  const out = new Uint8Array(bin.length);
  for (let i = 0; i < bin.length; i++) out[i] = bin.charCodeAt(i);
  return out;
}

// ---------- HMAC-SHA256 (WebCrypto) ----------

async function hmacSha256Raw(key: string, message: Uint8Array): Promise<Uint8Array> {
  const keyBytes = new TextEncoder().encode(key);
  const cryptoKey = await crypto.subtle.importKey(
    'raw',
    keyBytes as unknown as ArrayBufferView<ArrayBuffer>,
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign'],
  );
  const sig = await crypto.subtle.sign('HMAC', cryptoKey, message as unknown as ArrayBufferView<ArrayBuffer>);
  return new Uint8Array(sig);
}

function timingSafeBytesEqual(a: Uint8Array, b: Uint8Array): boolean {
  if (a.length !== b.length) return false;
  let diff = 0;
  for (let i = 0; i < a.length; i++) diff |= a[i]! ^ b[i]!;
  return diff === 0;
}

// ---------- sign ----------

/**
 * Permissive low-level signer: HMACs the given blob verbatim. No field
 * validation — the caller (signDirectorySnapshot or a test forging the
 * engine-bug class) owns the blob shape.
 */
export async function signSnapshotBlob(blob: Record<string, unknown>, key: string, nowMs?: number): Promise<string> {
  void nowMs; // accepted for call-site symmetry; issued_at lives INSIDE the blob
  const seg = b64encodeUtf8(JSON.stringify(blob));
  const sig = await hmacSha256Raw(key, new TextEncoder().encode(seg));
  return `${seg}.${b64encodeBytes(sig)}`;
}

/**
 * The §4.6 issuer: well-formed blob {version, issued_at, payload} + HMAC.
 * issued_at = ISO timestamp of nowMs (default Date.now()).
 */
export async function signDirectorySnapshot(
  version: number,
  payload: unknown,
  key: string,
  nowMs?: number,
): Promise<string> {
  const blob = {
    version,
    issued_at: new Date(nowMs ?? Date.now()).toISOString(),
    payload,
  };
  return signSnapshotBlob(blob as unknown as Record<string, unknown>, key, nowMs);
}

// ---------- verify (never throws) ----------

function isPlainObject(v: unknown): v is Record<string, unknown> {
  return v !== null && typeof v === 'object' && !Array.isArray(v);
}

/**
 * §4.6 verify chain: sig -> TTL -> version. NEVER throws — any malformed
 * input yields {ok:false, reason}. The reason strings are internal-only
 * (the caller falls back to a full embed; it does not relay the reason).
 */
export async function verifyDirectorySnapshot(
  value: string,
  opts: {
    key: string;
    currentVersion: number;
    nowMs?: number;
    maxAgeMs?: number;
  },
): Promise<WhSnapshotVerifyResult> {
  try {
    if (typeof value !== 'string') return { ok: false, reason: 'snapshot is not a string' };
    const parts = value.split('.');
    if (parts.length !== 2 || parts[0] === '' || parts[1] === '') {
      return { ok: false, reason: `malformed snapshot (segments=${parts.length})` };
    }
    const [seg, sigB64] = parts as [string, string];

    // 1. SIGNATURE — over the exact received segment bytes (no re-encode).
    const expected = await hmacSha256Raw(opts.key, new TextEncoder().encode(seg));
    let received: Uint8Array;
    try {
      received = b64decodeToBytes(sigB64);
    } catch {
      return { ok: false, reason: 'snapshot signature is not valid base64' };
    }
    if (!timingSafeBytesEqual(expected, received)) {
      return { ok: false, reason: 'snapshot signature mismatch' };
    }

    // 2. BLOB PARSE (precondition for the TTL gate) + field guards.
    let blob: Record<string, unknown>;
    try {
      const json = new TextDecoder().decode(b64decodeToBytes(seg));
      const parsed: unknown = JSON.parse(json);
      if (!isPlainObject(parsed)) return { ok: false, reason: 'snapshot blob is not an object' };
      blob = parsed;
    } catch {
      return { ok: false, reason: 'snapshot blob is not parseable' };
    }
    if (typeof blob.version !== 'number' || !Number.isInteger(blob.version) || blob.version < 0) {
      return { ok: false, reason: 'snapshot version is not a non-negative integer' };
    }
    if (typeof blob.issued_at !== 'string' || !Number.isFinite(Date.parse(blob.issued_at))) {
      return { ok: false, reason: 'snapshot issued_at is missing or unparsable' };
    }
    if (!('payload' in blob)) {
      return { ok: false, reason: 'snapshot payload is missing' };
    }

    // 3. TTL — issued_at <= maxAge old; small future skew tolerated.
    //    r40 review P3: NaN/Infinity clock options skipped BOTH gates
    //    (every comparison with NaN is false) — reject non-finite clocks.
    const now = opts.nowMs ?? Date.now();
    const maxAge = opts.maxAgeMs ?? DEFAULT_SNAPSHOT_MAX_AGE_MS;
    if (!Number.isFinite(now) || !Number.isFinite(maxAge) || maxAge < 0) {
      return { ok: false, reason: 'snapshot clock options are not finite' };
    }
    const age = now - Date.parse(blob.issued_at);
    if (age > maxAge) {
      return { ok: false, reason: `snapshot expired (age ${age}ms > ${maxAge}ms)` };
    }
    if (age < -SNAPSHOT_FUTURE_SKEW_MS) {
      return { ok: false, reason: `snapshot issued in the future (skew ${age}ms)` };
    }

    // 4. VERSION — == current directory version (stale snapshot => full embed).
    if (blob.version !== opts.currentVersion) {
      return { ok: false, reason: `snapshot version ${blob.version} != current ${opts.currentVersion}` };
    }

    return { ok: true, version: blob.version, issuedAt: blob.issued_at, payload: blob.payload };
  } catch (e) {
    return { ok: false, reason: `snapshot rejected: ${(e as Error)?.message ?? 'error'}` };
  }
}
