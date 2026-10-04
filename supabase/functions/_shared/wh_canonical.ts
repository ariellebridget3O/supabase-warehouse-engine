// =============================================================================
// _shared/wh_canonical.ts — canonical value space + typed comparators (r38).
// =============================================================================
// Normative source: findings_wh_scatter_gather.md §2.4 (canonical type system;
// the lexicographic "10"<"9" motivating failure) + §2.2 (SQL NULL placement;
// E12 group-key distinctness). PURITY LAW: zero runtime imports.
//
// Canonical value space: int8 -> BigInt (exact above 2^53 — the JS-number
// path cannot carry an int8 key), numeric -> scaled BigInt fixed-point
// (plan.scale fractional digits), text -> string compared by CODE POINT
// (JS `<` compares UTF-16 units — the U+10000-vs-U+FFFD pin requires true
// code-point order), timestamptz -> fixed-width UTC ISO-8601 string with a
// 6-digit fraction (fixed width makes lexicographic === chronological).
// NULL is not a value: it never equals anything and its placement is
// flag-driven (nullsFirst), independent of sort direction.
// =============================================================================

import type { CanonicalType, WhColumnPlan } from './wh_types.ts';

export type WhCanonicalValue = bigint | string | null;

export type WhCanonicalErrorKind = 'arity' | 'type';

export class WhCanonicalizeError extends Error {
  readonly kind: WhCanonicalErrorKind;
  constructor(kind: WhCanonicalErrorKind, message: string) {
    super(message);
    this.name = 'WhCanonicalizeError';
    this.kind = kind;
  }
}

const INT8_MIN = -(2n ** 63n);
const INT8_MAX = 2n ** 63n - 1n;
const INT8_RE = /^-?\d+$/;
/** decimal, no exponent: int part optional-ish, frac optional; '.5' and
 *  '123.' -style accepted only with digits on one side (PG numeric text). */
const NUMERIC_RE = /^([+-]?)(\d+(?:\.\d*)?|\.\d+)$/;

function needPlanType(plan: WhColumnPlan): CanonicalType {
  if (plan === null || typeof plan !== 'object' || typeof plan.type !== 'string') {
    throw new TypeError('wh_canonical: malformed column plan');
  }
  return plan.type;
}

function badType(msg: string): never {
  throw new WhCanonicalizeError('type', msg);
}

// ---------- days-from-civil (Hinnant) — real-calendar, year-aware ----------
function daysFromCivil(y: number, m: number, d: number): number {
  let yy = y;
  if (m <= 2) yy -= 1;
  const era = Math.floor(yy / 400);
  const yoe = yy - era * 400; // [0, 399]
  const mp = m <= 2 ? m + 9 : m - 3; // Mar=0..Feb=11
  const doy = Math.floor((153 * mp + 2) / 5) + d - 1; // [0, 365]
  const doe = yoe * 365 + Math.floor(yoe / 4) - Math.floor(yoe / 100) + doy;
  return era * 146097 + doe - 719468;
}

function civilFromDays(z: number): [number, number, number] {
  let zz = z + 719468;
  const era = Math.floor(zz / 146097);
  const doe = zz - era * 146097; // [0, 146096]
  const yoe = Math.floor((doe - Math.floor(doe / 1460) + Math.floor(doe / 36524) - Math.floor(doe / 146096)) / 365);
  const y = yoe + era * 400;
  const doy = doe - (365 * yoe + Math.floor(yoe / 4) - Math.floor(yoe / 100));
  const mp = Math.floor((5 * doy + 2) / 153);
  const d = doy - Math.floor((153 * mp + 2) / 5) + 1;
  const m = mp < 10 ? mp + 3 : mp - 9;
  return [y + (m <= 2 ? 1 : 0), m, d];
}

function isLeap(y: number): boolean {
  return (y % 4 === 0 && y % 100 !== 0) || y % 400 === 0;
}

function daysInMonth(y: number, m: number): number {
  const table = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  if (m === 2 && isLeap(y)) return 29;
  return table[m - 1];
}

function fmtUtc(micros: number): string {
  const days = Math.floor(micros / 86_400_000_000);
  let rem = micros - days * 86_400_000_000;
  const [y, mo, d] = civilFromDays(days);
  // P2-2 (r38 review): float64 micros silently corrupt outside the PG
  // timestamp range — year 9999 + 1µs rolls into year 10000 (and order-
  // inverts vs 9999-12-31T23:59:59.999968Z). PG timestamps span
  // 0001-01-01..9999-12-31 — anything outside is a hard error.
  if (y < 1 || y > 9999) badType(`timestamp out of the PG range (years 1-9999)`);
  const h = Math.floor(rem / 3_600_000_000);
  rem -= h * 3_600_000_000;
  const mi = Math.floor(rem / 60_000_000);
  rem -= mi * 60_000_000;
  const s = Math.floor(rem / 1_000_000);
  const frac = rem - s * 1_000_000;
  const p = (n: number, w: number): string => String(n).padStart(w, '0');
  const yStr = y < 0 ? '-' + String(-y).padStart(4, '0') : String(y).padStart(4, '0');
  return `${yStr}-${p(mo, 2)}-${p(d, 2)}T${p(h, 2)}:${p(mi, 2)}:${p(s, 2)}.${p(frac, 6)}Z`;
}

/** timestamptz: strict parse + UTC normalization to fixed-width ISO.
 *  Accepts ISO 8601 with Z / ±HH:MM / ±HHMM / ±HH offsets, a space
 *  separator (PG text rendering), date-only (midnight UTC), and integer
 *  epoch milliseconds. Rejects rollover dates (Feb 30), month 13, leap
 *  seconds, >6-digit fractions and fractional millis — all lossy or
 *  non-canonical. */
function canonicalTimestamp(v: unknown): string {
  if (typeof v === 'number') {
    // P2-2 (r38 review): epoch millis beyond the safe-integer range have
    // already lost bits in the JSON parse — same law as int8: force the
    // exact string path.
    if (!Number.isSafeInteger(v)) badType(`epoch millis ${v} is not a safe integer — use the exact string path`);
    if (!Number.isInteger(v)) badType(`epoch millis must be an integer, got ${v}`);
    return fmtUtc(v * 1000); // ms -> micros
  }
  if (typeof v !== 'string' || v.length === 0) badType('timestamptz expects an ISO string or epoch millis');

  // date-only shortcut
  let m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(v);
  if (m) {
    const [, ys, ms, ds] = m;
    return fmtUtc(daysFromCivil(Number(ys), Number(ms), Number(ds)) * 86_400_000_000);
  }

  m = /^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?(Z|[+-]\d{2}:?\d{2}|[+-]\d{2})$/.exec(v);
  if (!m) {
    badType(`not a canonicalizable timestamptz rendering: '${v.length > 40 ? v.slice(0, 40) + '…' : v}'`);
  }
  const [, ys, mos, ds, hs, mins, ss, fracs, offs] = m as RegExpExecArray & string[];
  const Y = Number(ys), Mo = Number(mos), D = Number(ds);
  const H = Number(hs), Mi = Number(mins), S = Number(ss);
  if (Mo < 1 || Mo > 12) badType(`month out of range: ${Mo}`);
  if (D < 1 || D > daysInMonth(Y, Mo)) badType(`day out of range for ${Y}-${Mo}: ${D}`);
  if (H > 23) badType(`hour out of range: ${H}`);
  if (Mi > 59) badType(`minute out of range: ${Mi}`);
  if (S > 59) badType(`leap second / second out of range: ${S}`);
  const fracMicros = fracs === undefined ? 0 : Number(fracs.padEnd(6, '0'));
  let offsetSec = 0;
  if (offs !== undefined && offs !== 'Z') {
    const sign = offs[0] === '-' ? -1 : 1;
    const digits = offs.slice(1).replace(':', '');
    const oh = Number(digits.slice(0, 2));
    const om = digits.length >= 4 ? Number(digits.slice(2, 4)) : 0;
    if (oh > 23 || om > 59) badType(`offset out of range: ${offs}`);
    offsetSec = sign * (oh * 3600 + om * 60);
  }
  const totalSecs = daysFromCivil(Y, Mo, D) * 86400 + H * 3600 + Mi * 60 + S - offsetSec;
  return fmtUtc(totalSecs * 1_000_000 + fracMicros);
}

/** §2.4 canonicalization. null/undefined pass through as null (SQL NULL is
 *  not a value). Everything else must be the EXACT carrier for the planned
 *  type — lossy renderings are rejected, never coerced. */
export function canonicalizeValue(v: unknown, plan: WhColumnPlan): WhCanonicalValue {
  const type = needPlanType(plan);
  if (v === null || v === undefined) return null;

  if (type === 'text') {
    if (typeof v !== 'string') badType(`text column got ${typeof v} (strict: numbers must arrive as text)`);
    return v;
  }

  if (type === 'int8') {
    // BigInt: range-checked; string: the exact text path (strict integer
    // digits — PG int8 text never renders exponents or fractions); number:
    // only safe integers (anything above 2^53 has already lost bits in the
    // JSON parse — force the string path).
    if (typeof v === 'bigint') {
      if (v < INT8_MIN || v > INT8_MAX) badType(`int8 out of range: ${v}`);
      return v;
    }
    if (typeof v === 'string') {
      if (!INT8_RE.test(v)) badType(`int8 text rendering must be plain digits: '${v}'`);
      const b = BigInt(v);
      if (b < INT8_MIN || b > INT8_MAX) badType(`int8 out of range: ${v}`);
      return b;
    }
    if (typeof v === 'number') {
      if (!Number.isSafeInteger(v)) badType(`int8 number ${v} is not a safe integer — use the exact string path`);
      return BigInt(v);
    }
    badType(`int8 column got ${typeof v}`);
  }

  if (type === 'numeric') {
    const scale = plan.scale;
    if (typeof scale !== 'number' || !Number.isInteger(scale) || scale < 0 || scale > 18) {
      throw new TypeError(`numeric plan requires an integer scale in [0,18], got ${String(scale)}`);
    }
    if (typeof v === 'bigint') return v; // merged-partial fixed-point round-trip
    let s: string;
    if (typeof v === 'number') {
      // shortest-repr rendering; exponent form (1e21+) is already lossy-ish —
      // reject and force the string path. NaN/Infinity are not numbers in PG.
      // P2-3 (r38 review): doubles at/above 2^53 are mangled by the JSON
      // parse — mirror the int8 law and force the exact string path.
      if (!Number.isFinite(v)) badType(`numeric got non-finite number`);
      if (!Number.isSafeInteger(Math.trunc(v))) badType(`numeric number ${v} exceeds the safe-integer range — use the exact string path`);
      s = String(v);
    } else if (typeof v === 'string') {
      s = v;
    } else {
      badType(`numeric column got ${typeof v}`);
    }
    const m = NUMERIC_RE.exec(s);
    if (!m) badType(`numeric text rendering must be a plain decimal (no exponent): '${s}'`);
    const [, sign, body] = m as RegExpExecArray & string[];
    const [intPart, fracPart] = body.split('.');
    let frac = fracPart ?? '';
    // trailing zeros are exact re-renders (1200.50 at scale 0) — strip ALL of
    // them BEFORE the over-precision check; any other excess precision is
    // lossy.
    while (frac.endsWith('0')) frac = frac.slice(0, -1);
    if (frac.length > scale) badType(`numeric over-precision: '${s}' has more than ${scale} fractional digits`);
    const scaled = BigInt((intPart === undefined || intPart === '' ? '0' : intPart) + frac.padEnd(scale, '0'));
    return sign === '-' ? -scaled : scaled;
  }

  if (type === 'timestamptz') return canonicalTimestamp(v);

  badType(`unknown canonical type: ${type}`);
}

function cmpBigInt(a: bigint, b: bigint): number {
  return a < b ? -1 : a > b ? 1 : 0;
}

/** Code-point string compare (JS `<`/`>` compare UTF-16 units, which inverts
 *  astral-plane vs U+E000–U+FFFF ordering — the §2.4 byte-wise law). */
function cmpCodePoints(a: string, b: string): number {
  const ia = a[Symbol.iterator]();
  const ib = b[Symbol.iterator]();
  for (;;) {
    const ra = ia.next();
    const rb = ib.next();
    if (ra.done && rb.done) return 0;
    if (ra.done) return -1;
    if (rb.done) return 1;
    const ca = (ra.value as string).codePointAt(0) as number;
    const cb = (rb.value as string).codePointAt(0) as number;
    if (ca !== cb) return ca < cb ? -1 : 1;
  }
}

/** Total order on canonical values for one column plan. NULL is ordered by
 *  the nullsFirst FLAG alone (never by direction — direction lives in the
 *  sort plan); NULL is never equal to any value including ''. */
export function compareCanonical(
  a: WhCanonicalValue,
  b: WhCanonicalValue,
  plan: WhColumnPlan,
  nullsFirst: boolean,
): number {
  if (a === null && b === null) return 0;
  if (a === null) return nullsFirst ? -1 : 1;
  if (b === null) return nullsFirst ? 1 : -1;
  const type = needPlanType(plan);
  if (type === 'int8' || type === 'numeric') {
    if (typeof a !== 'bigint' || typeof b !== 'bigint') {
      throw new TypeError(`compareCanonical: ${type} values must be canonical BigInts`);
    }
    return cmpBigInt(a, b);
  }
  if (typeof a !== 'string' || typeof b !== 'string') {
    throw new TypeError(`compareCanonical: ${type} values must be canonical strings`);
  }
  return type === 'text' ? cmpCodePoints(a, b) : a < b ? -1 : a > b ? 1 : 0; // timestamptz: fixed width
}

export interface WhSortKey {
  col: string;
  dir: 'asc' | 'desc';
  nullsFirst: boolean;
}

function planFor(plans: WhColumnPlan[] | Record<string, WhColumnPlan>, col: string): WhColumnPlan {
  const p = Array.isArray(plans)
    ? plans.find((x) => x.col === col)
    : plans[col];
  if (!p) throw new TypeError(`makeTypedComparator: no column plan for sort key '${col}'`);
  return p;
}

/** Top-K comparator over canonical rows: per-key compareCanonical, dir flips
 *  the VALUE compare only (null placement stays flag-driven), ties fall
 *  through to the next key. No JS `<` on untyped values anywhere. */
export function makeTypedComparator(
  sortPlan: WhSortKey[],
  plans: WhColumnPlan[] | Record<string, WhColumnPlan>,
): (a: Record<string, WhCanonicalValue>, b: Record<string, WhCanonicalValue>) => number {
  if (!Array.isArray(sortPlan)) throw new TypeError('makeTypedComparator: sortPlan must be an array');
  const keys = sortPlan.map((k) => ({ ...k, plan: planFor(plans, k.col) }));
  return (ra: Record<string, WhCanonicalValue>, rb: Record<string, WhCanonicalValue>): number => {
    for (const key of keys) {
      const va = ra[key.col];
      const vb = rb[key.col];
      if (va === null && vb === null) continue;
      if (va === null) return key.nullsFirst ? -1 : 1;
      if (vb === null) return key.nullsFirst ? 1 : -1;
      const c = compareCanonical(va, vb, key.plan, key.nullsFirst);
      if (c !== 0) return key.dir === 'desc' ? -c : c;
    }
    return 0;
  };
}

/** E12 group-key encoding: ordered slots + per-slot TYPE TAG — injective over
 *  canonical values. null / '' / 'null' are pairwise distinct; a missing slot
 *  (undefined) coerces to null so it collides into the SQL-NULL group; arity
 *  participates in the key; object-form keys are rejected (object key order
 *  is not semantic). Expects CANONICAL slots (bigint | string | null). */
export function canonicalGroupKey(k: unknown[]): string {
  if (!Array.isArray(k)) {
    throw new WhCanonicalizeError('arity', 'group key must be an ordered array (object-form keys are forbidden)');
  }
  const parts = k.map((slot) => {
    if (slot === undefined || slot === null) return 'n';
    if (typeof slot === 'bigint') return `i${slot}e`;
    if (typeof slot === 'string') return `j${JSON.stringify(slot)}`;
    throw new WhCanonicalizeError('type', `group key slot must be canonical (bigint|string|null), got ${typeof slot}`);
  });
  return `${k.length}:${parts.join(',')}`;
}
