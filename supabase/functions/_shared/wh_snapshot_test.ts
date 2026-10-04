// =============================================================================
// _shared/wh_snapshot_test.ts — r40 §4.6 directory snapshot battery (RED-first)
// =============================================================================
// Contract: research/findings_wh_catalog_contract.md §4.6 (transport = BODY
// field, encoding = base64(json({version, issued_at, payload})) + "." +
// base64(HMAC-SHA256(signed bytes, key)), verify chain = sig -> TTL(60s) ->
// version == current, ANY failure => silent full embed — a bad snapshot
// NEVER errors the request).
//
// Pinned interpretation (module header, erratum §4.6): the SIGNED BYTES are
// the exact received payload-segment bytes (byte-exact, no decode/re-encode/
// whitespace normalization) — the fixed-byte tamper class.
//
// Tamper battery (SKILL §10 laws): tampers are FIXED-BYTE at authoring time
// (middle of the segment — the last base64 char can absorb bit changes in
// its unused bits, the r23 coincidence class); every tamper pins
// tampered != original; the battery also passes under a verifyHs256-bypass
// mutant only by FAILING (parent-executed mutant proof, r32 pattern).
//
// Runs offline:  deno test --no-check -q --allow-all supabase/functions/_shared/wh_snapshot_test.ts
// =============================================================================

import { signDirectorySnapshot, signSnapshotBlob, verifyDirectorySnapshot } from './wh_snapshot.ts';

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

const KEY = 'wh-snapshot-test-key-0123456789abcdef';
const OTHER_KEY = 'wh-snapshot-test-key-FFFFF6789abcdef';
const NOW = Date.parse('2026-09-29T12:00:00.000Z');
const ROWS = [
  { shard: 'abc123', key_min: null, key_max: null, hash_slot: 0, state: 'serving', platform_status: 'ACTIVE_HEALTHY', schema_version: 3, last_health_at: '2026-09-29T11:59:00Z', logical_name: 'orders', shard_key_type: 'none', shard_key_column: null, table_schema_version: 3 },
  { shard: 'def456', key_min: '100', key_max: '199', hash_slot: null, state: 'serving', platform_status: 'ACTIVE_HEALTHY', schema_version: 3, last_health_at: '2026-09-29T11:59:00Z', logical_name: 'orders', shard_key_type: 'range', shard_key_column: 'id', table_schema_version: 3 },
];

async function green(): Promise<string> {
  return signDirectorySnapshot(42, ROWS, KEY, NOW);
}

const verify = (value: string, opts: { currentVersion?: number; nowMs?: number; maxAgeMs?: number } = {}) =>
  verifyDirectorySnapshot(value, {
    key: KEY,
    currentVersion: opts.currentVersion ?? 42,
    // deterministic clock: the battery never trusts the real wall clock
    // (signing pins NOW; the default verify clock must too — a real-clock
    // verify made every green path "expired" when the authoring date passed).
    nowMs: opts.nowMs ?? NOW,
    ...(opts.maxAgeMs !== undefined ? { maxAgeMs: opts.maxAgeMs } : {}),
  });

async function main(): Promise<void> {
  console.log('§4.6 green path');
  const snap = await green();
  eq('snapshot has exactly one dot', snap.split('.').length, 2);
  const g = await verify(snap);
  eq('green path verifies ok', g.ok, true);
  if (g.ok) {
    eq('version round-trips', g.version, 42);
    eq('issued_at round-trips', g.issuedAt, '2026-09-29T12:00:00.000Z');
    eq('payload round-trips byte-faithfully', g.payload, ROWS);
  }

  console.log('§4.6 verify chain — structural rejects (never throw, never ok)');
  const structural: [string, string][] = [
    ['empty string', ''],
    ['no dot', 'justabigbase64blobnoseparator'],
    ['three segments', `${snap}.${'extra'}`],
    ['invalid b64 sig', `${snap.split('.')[0]}.!!!not-b64!!!`],
    ['non-JSON segment', `bm90IGpzb24.${snap.split('.')[1]}`],
  ];
  for (const [name, value] of structural) {
    const r = await verify(value);
    eq(`reject: ${name}`, [r.ok, r.ok ? null : typeof r.reason], [false, 'string']);
  }

  console.log('§4.6 verify chain — tamper battery (fixed-byte, SKILL §10)');
  const [seg, sigB64] = snap.split('.');
  // Fixed-at-authoring tamper positions: MIDDLE of the segment (full 6-bit
  // groups — the last char can lose a bit-flip into unused padding bits) and
  // a middle char of the signature.
  const midSeg = Math.floor(seg.length / 2);
  const segChar = seg[midSeg] === 'A' ? 'B' : 'A';
  const tamperedSeg = seg.slice(0, midSeg) + segChar + seg.slice(midSeg + 1);
  ok('tampered segment differs from original', tamperedSeg !== seg, 'no-op tamper');
  const midSig = Math.floor(sigB64.length / 2);
  const sigChar = sigB64[midSig] === 'A' ? 'B' : 'A';
  const tamperedSig = sigB64.slice(0, midSig) + sigChar + sigB64.slice(midSig + 1);
  ok('tampered sig differs from original', tamperedSig !== sigB64, 'no-op tamper');

  const tSeg = await verify(`${tamperedSeg}.${sigB64}`);
  eq('tampered segment -> reject (fixed-byte tamper class)', tSeg.ok, false);
  const tSig = await verify(`${seg}.${tamperedSig}`);
  eq('tampered signature -> reject', tSig.ok, false);

  // r40 review blind spot #1: the LAST-DATA-CHAR unused-bits coincidence
  // (r23 class) — 4 low-bit flips of the final b64 char decode to IDENTICAL
  // bytes. The sig covers the received STRING, so these must still reject.
  // This pin kills the natural "canonicalize seg before HMAC" regression
  // mutant (decode->re-encode), which the mid-seg tamper alone does NOT catch.
  let lastData = seg.length - 1;
  while (seg[lastData] === '=') lastData--; // skip padding; land on a data char
  const lastChar = seg[lastData];
  const b64ALPHA = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/';
  const lastIdx = b64ALPHA.indexOf(lastChar);
  const altChar = b64ALPHA[(lastIdx + 1) % 64];
  const unusedBitTamper = seg.slice(0, lastData) + altChar + seg.slice(lastData + 1);
  const tUnused = await verify(`${unusedBitTamper}.${sigB64}`);
  eq('last-data-char unused-bits flip -> reject (sig covers the STRING, not decoded bytes)', tUnused.ok, false);

  // Truncation + extension of the SIGNED region.
  const tTrunc = await verify(`${seg.slice(0, -8)}.${sigB64}`);
  eq('truncated segment -> reject', tTrunc.ok, false);

  console.log('§4.6 verify chain — TTL (60s default, 5s future skew)');
  const fresh = await verify(snap, { nowMs: NOW + 30_000 });
  eq('30s old -> ok', fresh.ok, true);
  const edge = await verify(snap, { nowMs: NOW + 60_000 });
  eq('exactly 60s old -> ok (boundary-inclusive)', edge.ok, true);
  const expired = await verify(snap, { nowMs: NOW + 60_001 });
  eq('60s+1ms old -> reject (expired TTL)', expired.ok, false);
  const futureSkew = await verify(snap, { nowMs: NOW - 5_000 });
  eq('5s future -> ok (skew tolerance)', futureSkew.ok, true);
  const future = await verify(snap, { nowMs: NOW - 5_001 });
  eq('5s+1ms future -> reject', future.ok, false);

  // r40 review P3: degenerate clock options must reject, not skip both gates
  // (NaN comparisons are always false — the 11-day-old false-accept probe).
  const nanClock = await verify(snap, { nowMs: NaN });
  eq('nowMs NaN -> reject (clock guard)', nanClock.ok, false);
  const nanMaxAge = await verify(snap, { maxAgeMs: NaN });
  eq('maxAgeMs NaN -> reject (clock guard)', nanMaxAge.ok, false);
  const negMaxAge = await verify(snap, { maxAgeMs: -1 });
  eq('negative maxAgeMs -> reject (clock guard)', negMaxAge.ok, false);

  console.log('§4.6 verify chain — version gate');
  const stale = await verify(snap, { currentVersion: 43 });
  eq('snapshot version 42 vs current 43 -> reject (stale version)', stale.ok, false);
  const equal = await verify(snap, { currentVersion: 42 });
  eq('snapshot version == current -> ok', equal.ok, true);

  console.log('§4.6 verify chain — key');
  const spoofed = await verifyDirectorySnapshot(snap, { key: OTHER_KEY, currentVersion: 42 });
  eq('wrong key -> reject (spoofed key)', spoofed.ok, false);

  console.log('§4.6 version/issued_at field guards (validly-signed garbage: engine-bug class)');
  const forgedCases: [string, unknown][] = [
    ['float version', { version: 42.5, issued_at: '2026-09-29T12:00:00.000Z', payload: ROWS }],
    ['negative version', { version: -1, issued_at: '2026-09-29T12:00:00.000Z', payload: ROWS }],
    ['string version', { version: '42', issued_at: '2026-09-29T12:00:00.000Z', payload: ROWS }],
    ['missing issued_at', { version: 42, payload: ROWS }],
    ['garbage issued_at', { version: 42, issued_at: 'not-a-date', payload: ROWS }],
    ['missing payload', { version: 42, issued_at: '2026-09-29T12:00:00.000Z' }],
  ];
  for (const [name, blob] of forgedCases) {
    // signSnapshotBlob is the permissive low-level signer (pure crypto, no
    // field validation) — exactly how an engine bug would produce a validly
    // signed but malformed blob. verify must still reject it.
    const forged = await signSnapshotBlob(blob as Record<string, unknown>, KEY, NOW);
    const r = await verify(forged, { currentVersion: 42 });
    eq(`signed-but-malformed blob (${name}) -> field gate rejects`, r.ok, false);
  }
  // JSON round-trip of non-ASCII payload (UTF-8-safe b64).
  const unicode = await signDirectorySnapshot(42, { note: '代号-仓库名 ✓' }, KEY, NOW);
  const ur = await verify(unicode);
  eq('non-ASCII payload round-trips (UTF-8-safe b64)', ur.ok && (ur as { payload?: unknown }).payload, { note: '代号-仓库名 ✓' });

  console.log('§4.6 never-throw law');
  const nightmares: unknown[] = [null, undefined, 42, {}, [], true, 'x', `${'A'.repeat(5000)}.${'B'.repeat(100)}`];
  for (const n of nightmares) {
    const label = JSON.stringify(n)?.slice(0, 12) ?? 'undefined';
    let r: { ok: boolean } | null = null;
    try {
      r = await verifyDirectorySnapshot(n as string, { key: KEY, currentVersion: 42 });
    } catch {
      r = null;
    }
    eq(`nightmare ${label} -> {ok:false}, never throws`, r === null ? 'THREW' : r.ok, false);
  }

  console.log(`\n${passed} passed, ${failed} failed`);
  if (failed > 0) {
    console.error('FAILURES:\n' + failures.map((f) => `  * ${f}`).join('\n'));
    throw new Error(`${failed} snapshot pin(s) failed`);
  }
}

Deno.test('wh_snapshot §4.6 battery', main);
