// =============================================================================
// _shared/wh_canonical_test.ts — canonical type system + typed comparators.
// =============================================================================
// Normative source: findings_wh_scatter_gather.md §2.4 (canonical types; the
// lexicographic "10"<"9" motivating failure) + §2.2 (SQL NULL placement per
// key; NULL is never equal to any value including '') + E12 (null/''/'null'
// group-key distinctness).
//
// RED-first: written BEFORE the wh_canonical.ts implementation and observed
// failing against the stub. Offline + pure: runs with --no-check, no perms.
// =============================================================================

import {
  canonicalGroupKey,
  canonicalizeValue,
  compareCanonical,
  makeTypedComparator,
  WhCanonicalizeError,
} from './wh_canonical.ts';
import type { WhCanonicalValue } from './wh_canonical.ts';
import type { WhColumnPlan } from './wh_types.ts';

let passed = 0;
let failed = 0;

function eq(name: string, actual: unknown, expected: unknown): void {
  const a = JSON.stringify(actual, (_k, v) => (typeof v === 'bigint' ? `${v.toString()}n` : v));
  const e = JSON.stringify(expected, (_k, v) => (typeof v === 'bigint' ? `${v.toString()}n` : v));
  if (a === e) {
    passed++;
    console.log(`  ok  ${name}`);
  } else {
    failed++;
    console.error(`FAIL  ${name}\n      expected: ${e}\n      actual:   ${a}`);
  }
}

/** eq for comparator results: pins the sign, not the magnitude. */
function eqSign(name: string, actual: number, expected: -1 | 0 | 1): void {
  const sign = Math.sign(actual) as -1 | 0 | 1;
  eq(name, sign, expected);
}

function throwsWh(name: string, fn: () => unknown, kind: WhCanonicalizeError['kind'] | null): void {
  try {
    fn();
    failed++;
    console.error(`FAIL  ${name} — expected a throw, got none`);
  } catch (err) {
    if (err instanceof WhCanonicalizeError && (kind === null || err.kind === kind)) {
      passed++;
      console.log(`  ok  ${name} (WhCanonicalizeError kind=${err.kind})`);
    } else {
      failed++;
      console.error(`FAIL  ${name} — wrong throw: ${err}`);
    }
  }
}

const INT8: WhColumnPlan = { col: 'v', type: 'int8' };
const NUM2: WhColumnPlan = { col: 'v', type: 'numeric', scale: 2 };
const NUM0: WhColumnPlan = { col: 'v', type: 'numeric', scale: 0 };
const TEXT: WhColumnPlan = { col: 'v', type: 'text' };
const TS: WhColumnPlan = { col: 'v', type: 'timestamptz' };

// -----------------------------------------------------------------------------
// int8 → BigInt: exact survival above 2^53; lossy renderings rejected.
// -----------------------------------------------------------------------------
Deno.test('int8 canonicalization: BigInt/string/number inputs; >2^53 survives EXACTLY; lossy rejected', () => {
  eq('BigInt input round-trips', canonicalizeValue(9007199254740993n, INT8), 9007199254740993n);
  eq(
    '9007199254740993n is distinct from its double-rounded neighbor (the whole point of BigInt keys)',
    9007199254740993n === 9007199254740992n,
    false,
  );
  eq('string input >2^53 round-trips exactly', canonicalizeValue('9007199254740993', INT8), 9007199254740993n);
  eq('string input negative', canonicalizeValue('-42', INT8), -42n);
  eq('number input (safe integer)', canonicalizeValue(42, INT8), 42n);
  eq('null passes through', canonicalizeValue(null, INT8), null);
  eq('undefined passes through as null', canonicalizeValue(undefined, INT8), null);

  // A JS number above 2^53 cannot be proven lossless (rounding already
  // happened in the JSON parse) — the text path is the exact carrier.
  throwsWh('number 2**53 rejected (unsafe — force the exact string path)', () => canonicalizeValue(2 ** 53, INT8), 'type');
  throwsWh('number 1.5 rejected (non-integral)', () => canonicalizeValue(1.5, INT8), 'type');
  throwsWh("string 'abc' rejected", () => canonicalizeValue('abc', INT8), 'type');
  throwsWh("string '1e3' rejected (PG int8 text never renders exponents)", () => canonicalizeValue('1e3', INT8), 'type');
  throwsWh("string '1.0' rejected", () => canonicalizeValue('1.0', INT8), 'type');
  throwsWh("string '' rejected", () => canonicalizeValue('', INT8), 'type');
  throwsWh('out-of-int8-range bigint rejected', () => canonicalizeValue(2n ** 63n, INT8), 'type');
  throwsWh('out-of-int8-range string rejected', () => canonicalizeValue('99999999999999999999', INT8), 'type');
  throwsWh('object input rejected', () => canonicalizeValue({}, INT8), 'type');
});

// -----------------------------------------------------------------------------
// numeric → scaled BigInt fixed-point via plan.scale.
// -----------------------------------------------------------------------------
Deno.test('numeric canonicalization: fixed-point scaled BigInt per plan.scale; over-precision rejected', () => {
  eq("'123.45' at scale 2", canonicalizeValue('123.45', NUM2), 12345n);
  eq("'123.4' at scale 2 pads", canonicalizeValue('123.4', NUM2), 12340n);
  eq("'123' at scale 2 pads", canonicalizeValue('123', NUM2), 12300n);
  eq("'-123.45' at scale 2", canonicalizeValue('-123.45', NUM2), -12345n);
  eq("'.5' at scale 2", canonicalizeValue('.5', NUM2), 50n);
  eq("'0.10' at scale 2", canonicalizeValue('0.10', NUM2), 10n);
  eq("'1200.0' at scale 0 strips the trailing zero exactly (not lossy)", canonicalizeValue('1200.0', NUM0), 1200n);
  throwsWh("'1200.50' at scale 0 rejected (dropping .50 would corrupt sums — lossy)", () => canonicalizeValue('1200.50', NUM0), 'type');
  eq('number 123.45 at scale 2 (shortest-repr rendering)', canonicalizeValue(123.45, NUM2), 12345n);
  eq('BigInt input passes through (merged-partial fixed-point round-trip)', canonicalizeValue(12345n, NUM2), 12345n);
  eq('null passes through', canonicalizeValue(null, NUM2), null);

  throwsWh("'123.456' at scale 2 rejected (over-precision is lossy)", () => canonicalizeValue('123.456', NUM2), 'type');
  throwsWh("'0.001' at scale 2 rejected", () => canonicalizeValue('0.001', NUM2), 'type');
  throwsWh("string '1e21' rejected", () => canonicalizeValue('1e21', NUM2), 'type');
  throwsWh('exponential number rendering rejected (use the string path)', () => canonicalizeValue(1e21, NUM2), 'type');
  throwsWh('NaN rejected', () => canonicalizeValue(NaN, NUM2), 'type');
  throwsWh('Infinity rejected', () => canonicalizeValue(Infinity, NUM2), 'type');
});

// -----------------------------------------------------------------------------
// text → byte-wise code-point compare. '10' < '9' is CORRECT for text.
// -----------------------------------------------------------------------------
Deno.test('text canonicalization + code-point ordering: text-typed 10 vs 9 is LEXICOGRAPHIC (pinned per type)', () => {
  eq('string passes through', canonicalizeValue('eu', TEXT), 'eu');
  eq('empty string passes through (distinct from null)', canonicalizeValue('', TEXT), '');
  eq("string 'null' passes through (distinct from null)", canonicalizeValue('null', TEXT), 'null');
  throwsWh('number input rejected for a text column (strict)', () => canonicalizeValue(10, TEXT), 'type');

  // Pinned per TYPE: text '10' sorts BEFORE '9' ('1' = U+0031 < '9' = U+0039).
  eqSign("compareCanonical(text '10', '9') < 0 — the type-correct lexicographic order", compareCanonical('10', '9', TEXT, false), -1);
  eqSign('reversed', compareCanonical('9', '10', TEXT, false), 1);
  eqSign('prefix orders before longer', compareCanonical('ab', 'abc', TEXT, false), -1);
  eqSign('non-BMP code point compares by code point, not UTF-16 unit', compareCanonical('\u{10000}', '\uFFFD', TEXT, false), 1);
  eqSign('empty vs space', compareCanonical('', ' ', TEXT, false), -1);
});

// -----------------------------------------------------------------------------
// timestamptz → UTC-normalized fixed-width ISO-8601 (microsecond fixed point).
// -----------------------------------------------------------------------------
Deno.test('timestamptz canonicalization: offsets normalize to UTC; epoch millis accepted; micros preserved', () => {
  eq(
    '2026-01-01T00:00:00+02:00 === 2025-12-31T22:00:00Z (offset normalization)',
    canonicalizeValue('2026-01-01T00:00:00+02:00', TS),
    canonicalizeValue('2025-12-31T22:00:00Z', TS),
  );
  eq(
    'compact offset +0200 also normalizes to the same instant',
    canonicalizeValue('2026-01-01T00:00:00+0200', TS),
    canonicalizeValue('2025-12-31T22:00:00Z', TS),
  );
  eq(
    'epoch millis input normalizes to the same instant',
    canonicalizeValue(Date.parse('2025-12-31T22:00:00Z'), TS),
    canonicalizeValue('2025-12-31T22:00:00Z', TS),
  );
  eq(
    'space separator + +00 offset (PG text rendering) normalizes the same',
    canonicalizeValue('2025-12-31 22:00:00+00', TS),
    canonicalizeValue('2025-12-31T22:00:00Z', TS),
  );
  eq(
    'sub-second microseconds survive exactly (fixed 6-digit fraction)',
    canonicalizeValue('2026-01-01T00:00:00.000500Z', TS),
    '2026-01-01T00:00:00.000500Z',
  );
  eq(
    'sub-second ordering is chronological (fixed-width fraction; trailing-zero trim would break it)',
    compareCanonical(
      canonicalizeValue('2026-01-01T00:00:00.000000Z', TS),
      canonicalizeValue('2026-01-01T00:00:00.000500Z', TS),
      TS,
      false,
    ) < 0,
    true,
  );
  eq(
    'cross-offset ordering: normalized instants order chronologically',
    compareCanonical(
      canonicalizeValue('2026-01-01T00:00:00+02:00', TS),
      canonicalizeValue('2026-01-01T00:00:00Z', TS),
      TS,
      false,
    ) < 0,
    true,
  );
  eq('negative epoch millis work', canonicalizeValue(-1, TS), '1969-12-31T23:59:59.999000Z');
  eq('date-only accepted as midnight UTC', canonicalizeValue('2026-01-01', TS), '2026-01-01T00:00:00.000000Z');

  throwsWh("garbage string rejected", () => canonicalizeValue('not a date', TS), 'type');
  throwsWh('slash date rejected', () => canonicalizeValue('01/02/2026', TS), 'type');
  throwsWh('Feb 30 rejected (real-calendar check, not rollover)', () => canonicalizeValue('2026-02-30T00:00:00Z', TS), 'type');
  throwsWh('month 13 rejected', () => canonicalizeValue('2026-13-01T00:00:00Z', TS), 'type');
  throwsWh('leap second :60 rejected', () => canonicalizeValue('2026-01-01T23:59:60Z', TS), 'type');
  throwsWh('7-digit fraction rejected (beyond micro precision, lossy)', () => canonicalizeValue('2026-01-01T00:00:00.1234567Z', TS), 'type');
  throwsWh('fractional epoch millis rejected', () => canonicalizeValue(1234.5, TS), 'type');
  eq('null passes through', canonicalizeValue(null, TS), null);
});

// -----------------------------------------------------------------------------
// compareCanonical: SQL NULL placement (nullsFirst flag per key).
// -----------------------------------------------------------------------------
Deno.test('compareCanonical NULL placement: nullsFirst both directions; NULL never equals any value (incl. empty string)', () => {
  eqSign('null first: (null, 5n) < 0', compareCanonical(null, 5n, INT8, true), -1);
  eqSign('null last: (null, 5n) > 0', compareCanonical(null, 5n, INT8, false), 1);
  eqSign('null first: (5n, null) > 0', compareCanonical(5n, null, INT8, true), 1);
  eqSign('null last: (5n, null) < 0', compareCanonical(5n, null, INT8, false), -1);
  eqSign('(null, null) are peers (SQL GROUP BY clusters NULLs)', compareCanonical(null, null, INT8, true), 0);
  eqSign("null vs '' in text: ordered, NEVER equal (nullsFirst)", compareCanonical(null, '', TEXT, true), -1);
  eqSign("null vs '' in text: ordered, NEVER equal (nullsLast)", compareCanonical(null, '', TEXT, false), 1);
  eqSign("'' vs null mirrored", compareCanonical('', null, TEXT, false), -1);
  eqSign('null vs null text peers', compareCanonical(null, null, TEXT, false), 0);
  // null placement is flag-driven, not direction-driven — direction lives in
  // the sort plan (makeTypedComparator), which flips only the value compare.
  eqSign('bigint total order', compareCanonical(-5n, 5n, INT8, false), -1);
  eqSign('numeric fixed-point order at scale', compareCanonical(12345n, 12300n, NUM2, false), 1);
  eqSign('numeric negative', compareCanonical(-1n, 1n, NUM0, false), -1);
});

// -----------------------------------------------------------------------------
// E12 — group-key canonicalization: null vs '' vs 'null' pairwise distinct.
// -----------------------------------------------------------------------------
Deno.test('canonicalGroupKey (E12): [null], [\'\'], [\'null\'] pairwise distinct; undefined slot collides into the SQL-NULL group', () => {
  const kNull = canonicalGroupKey([null]);
  const kEmpty = canonicalGroupKey(['']);
  const kStrNull = canonicalGroupKey(['null']);
  const kEu = canonicalGroupKey(['eu']);
  const keys = new Set([kNull, kEmpty, kStrNull, kEu]);
  eq('the 4 E12 keys are pairwise distinct', keys.size, 4);
  eq('undefined slot coerces to null (missing slot joins the SQL-NULL group)', canonicalGroupKey([undefined]), kNull);
  eq('composite: [null,"x"] distinct from ["null","x"]', canonicalGroupKey([null, 'x']) !== canonicalGroupKey(['null', 'x']), true);
  eq('composite: slot count is encoded (arity participates in the key)', canonicalGroupKey(['a']) !== canonicalGroupKey(['a', 'a']), true);
  eq('type-tagged: bigint 5 distinct from text "5"', canonicalGroupKey([5n]) !== canonicalGroupKey(['5']), true);
  eq('stable: same input, same key (deterministic encoding)', canonicalGroupKey([5n, 'eu', null]), canonicalGroupKey([5n, 'eu', null]));
  throwsWh('object-form group key rejected at the canonical layer too (kind=arity)', () => canonicalGroupKey({ region: 'eu' } as unknown as unknown[]), 'arity');
  throwsWh('non-canonical slot type (number) rejected (kind=type)', () => canonicalGroupKey([1]), 'type');
});

// -----------------------------------------------------------------------------
// makeTypedComparator: per-key compare, dir flips the VALUE compare only,
// nullsFirst placement is direction-independent, ties fall through to the
// next key.
// -----------------------------------------------------------------------------
Deno.test('makeTypedComparator: multi-key sort with dir flips + null placement independent of direction + tie-break', () => {
  const plans: Record<string, WhColumnPlan> = { a: { col: 'a', type: 'int8' }, b: { col: 'b', type: 'int8' } };
  const rows: Record<string, WhCanonicalValue>[] = [
    { a: null, b: 1n }, // r1
    { a: 5n, b: 2n }, // r2
    { a: 5n, b: 1n }, // r3
    { a: 3n, b: 9n }, // r4
  ];
  const cmpDescNullsFirst = makeTypedComparator(
    [{ col: 'a', dir: 'desc', nullsFirst: true }, { col: 'b', dir: 'asc', nullsFirst: false }],
    plans,
  );
  const sorted1 = rows.slice().sort(cmpDescNullsFirst);
  eq(
    'desc a (nulls STILL FIRST despite desc) then asc b tie-break: r1(null), r3(5,1), r2(5,2), r4(3,9)',
    sorted1.indexOf(rows[0]) < sorted1.indexOf(rows[2]) && sorted1.indexOf(rows[2]) < sorted1.indexOf(rows[1]) &&
      sorted1.indexOf(rows[1]) < sorted1.indexOf(rows[3]),
    true,
  );
  const cmpDescNullsLast = makeTypedComparator(
    [{ col: 'a', dir: 'desc', nullsFirst: false }, { col: 'b', dir: 'asc', nullsFirst: false }],
    plans,
  );
  const sorted2 = rows.slice().sort(cmpDescNullsLast);
  eq(
    'same plan with nullsLast: 5,5,3 then null last (tie on a=5 broken by b asc)',
    sorted2[0] === rows[2] && sorted2[1] === rows[1] && sorted2[2] === rows[3] && sorted2[3] === rows[0],
    true,
  );
  const cmpAsc = makeTypedComparator([{ col: 'a', dir: 'asc', nullsFirst: false }], plans);
  eqSign('asc int8 comparator: 10n > 9n (typed numeric order)', cmpAsc({ a: 10n }, { a: 9n }), 1);
  eq('comparator is stable on full ties (returns 0)', cmpAsc({ a: 5n }, { a: 5n }), 0);
});

// -----------------------------------------------------------------------------
// P0-2 pin — the lexicographic inversion is KILLED per TYPE: text '10' < '9'
// but number-typed 10 > 9. Both behaviors pinned in their own type.
// -----------------------------------------------------------------------------
Deno.test('typed comparator kills the "10"<"9" inversion: BOTH behaviors pinned per their canonical types', () => {
  eqSign("TEXT column: '10' < '9' (correct for text)", compareCanonical('10', '9', TEXT, false), -1);
  eqSign('INT8 column: 10 > 9 (correct for numbers — string compare would invert)', compareCanonical(10n, 9n, INT8, false), 1);
  eqSign('NUMERIC column: 10.00 > 9.00 (fixed-point bigint, same guard)', compareCanonical(1000n, 900n, NUM2, false), 1);
  const plans: Record<string, WhColumnPlan> = { t: TEXT, n: INT8 };
  const cmpT = makeTypedComparator([{ col: 't', dir: 'asc', nullsFirst: false }], plans);
  const cmpN = makeTypedComparator([{ col: 'n', dir: 'asc', nullsFirst: false }], plans);
  eqSign("sort-plan text key: '10' before '9'", cmpT({ t: '10' }, { t: '9' }), -1);
  eqSign('sort-plan int8 key: 9 before 10 (the inversion a JS `<` comparator would commit on strings)', cmpN({ n: 9n }, { n: 10n }), -1);
});

// -----------------------------------------------------------------------------
// Harness report (hand-rolled runner, no external deps).
// -----------------------------------------------------------------------------
Deno.test('__report__', () => {
  console.log(`\nwh_canonical_test: ${passed} assertions passed, ${failed} failed`);
  if (failed > 0) throw new Error(`${failed} assertion(s) failed`);
});
