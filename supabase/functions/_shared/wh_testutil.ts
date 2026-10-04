// =============================================================================
// _shared/wh_testutil.ts — TEST-ONLY helpers for the wh_* test family.
// =============================================================================
// NEVER imported by production code (wh_types/wh_canonical/wh_merge do not
// know this file exists). The file name intentionally does NOT match the
// `*_test.ts` pattern so `deno test` never executes it as a test file.
//
// Independence note (differential-harness honesty): the shard-partial BUILDER
// below emulates what a shard's PostgREST computes (shard-local
// group-then-aggregate) using NAIVE primitives only — plain JS Map keyed on
// JSON.stringify of raw slot values, BigInt(v) sums, JS `<`/`>` on BigInt for
// min/max. It deliberately does NOT use wh_canonical/wh_merge helpers, so a
// bug in the production canonicalization cannot cancel itself against the
// builder; the reference reducer (in wh_differential_test.ts) is a third,
// also-independent implementation.
// =============================================================================

import type { WhPartialEnvelope, WhPartialRow } from './wh_types.ts';

/** Deterministic seeded PRNG (mulberry32) — all fuzz/differential seeds are
 *  pinned so every run is byte-reproducible. */
export function mulberry32(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** Bigint-aware structural equality (JSON.stringify throws on BigInt). */
export function deepEq(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (typeof a === 'bigint' || typeof b === 'bigint') {
    // bigint never equals a non-bigint (no silent cross-type equality)
    return typeof a === 'bigint' && typeof b === 'bigint' && a === b;
  }
  if (Array.isArray(a) && Array.isArray(b)) {
    if (a.length !== b.length) return false;
    return a.every((x, i) => deepEq(x, b[i]));
  }
  if (a !== null && b !== null && typeof a === 'object' && typeof b === 'object') {
    const ka = Object.keys(a as Record<string, unknown>).sort();
    const kb = Object.keys(b as Record<string, unknown>).sort();
    if (ka.length !== kb.length || ka.some((k, i) => k !== kb[i])) return false;
    return ka.every((k) =>
      deepEq((a as Record<string, unknown>)[k], (b as Record<string, unknown>)[k])
    );
  }
  return false; // NaN !== anything, etc. — the suites never need NaN equality
}

/** Bigint-safe stringify for failure messages. */
export function show(x: unknown): string {
  return JSON.stringify(x, (_k, v) => (typeof v === 'bigint' ? `${v.toString()}n` : v));
}

/** Fisher-Yates shuffle driven by a seeded rng (for the commutativity arm). */
export function shuffled<T>(arr: readonly T[], rng: () => number): T[] {
  const out = arr.slice();
  for (let i = out.length - 1; i > 0; i--) {
    const j = Math.floor(rng() * (i + 1));
    [out[i], out[j]] = [out[j], out[i]];
  }
  return out;
}

export interface BuilderAggSpec {
  name: string;
  op: 'sum' | 'count' | 'min' | 'max' | 'avg';
  col?: string;
}

export interface BuildShardOpts {
  shard: string;
  table: string;
  schemaVersion?: number;
  /** Group-by column names in order. Absent/empty => scalar partial
   *  (exactly one row with k: []). */
  groupKeyCols?: string[];
  aggs: BuilderAggSpec[];
  /** Wire rendering for numeric aggregate values: 'string' = the exact text
   *  path (preferred carrier), 'number' = JSON numbers (PostgREST default),
   *  'mixed' = per-value coin flip via `rnd`. counts always render as numbers
   *  (PG counts are JS-safe in v0 corpora). */
  render?: 'string' | 'number' | 'mixed';
  /** rng for 'mixed' rendering. */
  rnd?: () => number;
  /** numeric corpora scale (0 or 2): shard-local sums aggregate SCALED and
   *  re-render decimal text (independent impl of the same law wh_merge's
   *  renderScaled pins — independence is the point). */
  valScale?: number;
}

/** Independent scaled-decimal parse: '13.45' @2 -> 1345n. Trailing-zero and
 *  over-precision corpus values never occur (generator emits <= scale
 *  fractional digits). */
function parseScaled(v: string | number, scale: number): bigint {
  const s = String(v);
  const neg = s.startsWith('-');
  const body = neg ? s.slice(1) : s;
  const [ip, fp = ''] = body.split('.');
  const scaled = BigInt((ip || '0') + fp.padEnd(scale, '0').slice(0, Math.max(fp.length, scale)));
  return neg ? -scaled : scaled;
}

/** Independent scaled-decimal render: 1345n @2 -> '13.45'. */
function renderDecimal(v: bigint, scale: number): string {
  const neg = v < 0n;
  const digits = (neg ? -v : v).toString().padStart(scale + 1, '0');
  const i = digits.length - scale;
  const text = scale === 0 ? digits : `${digits.slice(0, i)}.${digits.slice(i)}`;
  return neg ? '-' + text : text;
}

/**
 * Emulate ONE shard's PostgREST partial: shard-local group-then-aggregate
 * over this shard's rows. Independent of wh_canonical/wh_merge by design
 * (naive JSON.stringify keying on RAW slot values + BigInt arithmetic).
 *
 * SQL semantics implemented here (per PG docs):
 *   - aggregates ignore NULL inputs;
 *   - sum over zero non-null inputs => null (the §2.0 empty-input rule);
 *   - count => 0 over zero rows (never null);
 *   - min/max over zero non-null inputs => null;
 *   - avg partial is the PAIR (sum, count): {s, c} with s:null ⟺ c:0.
 */
export function buildShardPartial(
  shardRows: Record<string, unknown>[],
  opts: BuildShardOpts,
): WhPartialEnvelope {
  const render = opts.render ?? 'string';
  const rnd = opts.rnd ?? (() => 0); // deterministic default: 'string' branch
  const scale = opts.valScale ?? 0;
  const renderNum = (v: bigint): string | number => {
    if (scale > 0) return render === 'number' ? Number(renderDecimal(v, scale)) : renderDecimal(v, scale);
    if (render === 'string') return v.toString();
    if (render === 'number') return Number(v);
    return rnd() < 0.5 ? v.toString() : Number(v);
  };

  const evalAgg = (rows: Record<string, unknown>[], spec: BuilderAggSpec): unknown => {
    if (spec.op === 'count') {
      if (spec.col === undefined) return rows.length; // COUNT(*) — nulls included
      return rows.filter((r) => r[spec.col as string] !== null).length; // count(col)
    }
    const col = spec.col as string;
    const vals = rows
      .map((r) => r[col])
      .filter((v) => v !== null && v !== undefined)
      .map((v) => parseScaled(v as string | number, scale));
    if (spec.op === 'sum') return vals.length === 0 ? null : renderNum(vals.reduce((x, y) => x + y, 0n));
    if (spec.op === 'min') return vals.length === 0 ? null : renderNum(vals.reduce((x, y) => (y < x ? y : x)));
    if (spec.op === 'max') return vals.length === 0 ? null : renderNum(vals.reduce((x, y) => (y > x ? y : x)));
    // avg: the PAIR (sum, count) — s:null ⟺ c:0
    return { s: vals.length === 0 ? null : renderNum(vals.reduce((x, y) => x + y, 0n)), c: vals.length };
  };

  const aggDecls: Record<string, { op: string; col?: string }> = {};
  for (const a of opts.aggs) aggDecls[a.name] = a.col === undefined ? { op: a.op } : { op: a.op, col: a.col };

  const gkCols = opts.groupKeyCols ?? [];
  let rows: WhPartialRow[];

  if (gkCols.length === 0) {
    // scalar: exactly one row, empty key
    const a: Record<string, unknown> = {};
    for (const spec of opts.aggs) a[spec.name] = evalAgg(shardRows, spec);
    rows = [{ k: [], a }];
  } else {
    // shard-local GROUP BY on RAW slot values (undefined coerced to null)
    const groups = new Map<string, { slots: unknown[]; rows: Record<string, unknown>[] }>();
    for (const r of shardRows) {
      const slots = gkCols.map((c) => (r[c] === undefined ? null : r[c]));
      const key = JSON.stringify(slots);
      let g = groups.get(key);
      if (!g) {
        g = { slots, rows: [] };
        groups.set(key, g);
      }
      g.rows.push(r);
    }
    rows = [...groups.values()].map((g) => {
      const a: Record<string, unknown> = {};
      for (const spec of opts.aggs) a[spec.name] = evalAgg(g.rows, spec);
      return { k: g.slots, a };
    });
  }

  return {
    v: 1,
    shard: opts.shard,
    table: opts.table,
    schema_version: opts.schemaVersion ?? 1,
    partial: {
      kind: gkCols.length === 0 ? 'scalar' : 'grouped',
      ...(gkCols.length === 0 ? {} : { groupKeys: gkCols }),
      aggs: aggDecls as WhPartialEnvelope['partial']['aggs'],
      rows,
      rowCount: rows.length,
      more: false,
    },
  };
}
