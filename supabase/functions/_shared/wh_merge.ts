// =============================================================================
// _shared/wh_merge.ts — aggregate MERGE ALGEBRA over shard partials (r38).
// =============================================================================
// Normative source: research/findings_wh_scatter_gather.md §2.0-§2.2 (merge
// table + rigor), §2.4 (canonical types), §8 worked examples E1/E2/E7/E12/E14.
//
// PURITY LAW: imports ONLY from the wh_* island (wh_types, wh_canonical —
// the latter type-plus-value for compareCanonical/WhCanonicalizeError); zero
// npm/network/fs/env access. Offline-testable with --allow-env alone.
// =============================================================================
//   * SUM: BigInt accumulation over canonicalized partials; the §2.0
//     EMPTY-INPUT rule — when NO non-NULL input existed anywhere (every shard
//     emitted sum:null, or there were no shards), the final SUM is NULL, not
//     0 (E14). A non-null partial means >= 1 non-NULL input existed.
//   * COUNT: numeric, never NULL — 0 over empty input; COUNT(*) counts rows,
//     COUNT(col) counts non-NULL inputs (the partial already encodes which).
//   * MIN/MAX: a null partial is the IDENTITY (E7: shards {5,7} and
//     {null,null} -> 5; treating shard-null as a value yields 0 — wrong);
//     all-NULL everywhere finalizes NULL.
//   * AVG: the partial is the PAIR (s, c) with s:null <=> c:0; pair merge is
//     (E2: (100,10) + (100000,1000) -> 100100/1010 ~= 99.11, never the
//     mean-of-means 55). Finalize emits the EXACT rational {num, den}; a
//     group with zero non-NULL inputs finalizes NULL.
//   * GROUP BY: canonical key = canonicalGroupKey over canonicalized slots;
//     row-arity violations (incl. object-form keys) are arity_mismatch; slot
//     canonicalization failures are type_mismatch; null/''/'null' stay
//     pairwise distinct (E12).
//   * MONOID CLOSURE: merge(A,B) is itself a valid v1 partial (shard
//     '<merged>'), so Promise.all windowing and pairwise trees are free —
//     assoc + comm hold by construction and are fuzz-pinned.
//
// Error discipline (wh_types): wire/plan-value violations throw
// WhMergeError with a pinned code; malformed PLAN objects (programmer
// errors) throw plain TypeError.
// =============================================================================

import { WhMergeError } from './wh_types.ts';
export { WhMergeError };
import type { WhAggSpec, WhColumnPlan, WhPartialEnvelope, WhPartialRow } from './wh_types.ts';
import { canonicalGroupKey, canonicalizeValue, compareCanonical, WhCanonicalizeError } from './wh_canonical.ts';
import type { WhCanonicalValue } from './wh_canonical.ts';

export interface WhAggPlan {
  op: 'sum' | 'count' | 'min' | 'max' | 'avg';
  col?: string;
  colPlan?: WhColumnPlan;
}

export interface WhMergePlan {
  table: string;
  groupKeys?: WhColumnPlan[];
  aggs: Record<string, WhAggPlan>;
}

export type WhFinalAggValue = bigint | number | string | null | { num: bigint; den: bigint };

export interface WhGroupFinal {
  k: WhCanonicalValue[];
  aggs: Record<string, WhFinalAggValue>;
}

const OPS = new Set(['sum', 'count', 'min', 'max', 'avg']);

function envInvalid(msg: string): never {
  throw new WhMergeError('envelope_invalid', msg);
}
function arityMismatch(msg: string): never {
  throw new WhMergeError('arity_mismatch', msg);
}
function typeMismatch(msg: string): never {
  throw new WhMergeError('type_mismatch', msg);
}

/** Validate the PLAN (programmer error => TypeError). */
function validatePlan(plan: WhMergePlan): void {
  if (plan === null || typeof plan !== 'object') throw new TypeError('wh_merge: plan must be an object');
  if (typeof plan.table !== 'string' || plan.table.length === 0) throw new TypeError('wh_merge: plan.table required');
  if (plan.aggs === null || typeof plan.aggs !== 'object') throw new TypeError('wh_merge: plan.aggs required');
  const names = Object.keys(plan.aggs);
  if (names.length === 0) throw new TypeError('wh_merge: plan.aggs must declare at least one aggregate');
  for (const name of names) {
    const a = plan.aggs[name];
    if (!a || !OPS.has(a.op)) throw new TypeError(`wh_merge: agg '${name}' has no valid op`);
    if (a.op !== 'count' && !a.colPlan) {
      throw new TypeError(`wh_merge: agg '${name}' (${a.op}) requires a colPlan`);
    }
    if (a.colPlan) {
      const t = a.colPlan.type;
      if (t !== 'int8' && t !== 'numeric' && t !== 'text' && t !== 'timestamptz') {
        throw new TypeError(`wh_merge: agg '${name}' colPlan.type invalid: ${String(t)}`);
      }
      // P2-4 (r38 review): sum/min/max/avg over text/timestamptz is not SQL
      // (PG sum(text) does not exist) and would JS-add strings — plan-time
      // TypeError, never silent garbage.
      if (a.op !== 'count' && t !== 'int8' && t !== 'numeric') {
        throw new TypeError(`wh_merge: agg '${name}' (${a.op}) requires an int8|numeric colPlan, got ${t}`);
      }
      if (t === 'numeric' && (!Number.isInteger(a.colPlan.scale) || (a.colPlan.scale as number) < 0 || (a.colPlan.scale as number) > 18)) {
        throw new TypeError(`wh_merge: agg '${name}' numeric colPlan requires integer scale in [0,18]`);
      }
    } else if (a.op !== 'count') {
      throw new TypeError(`wh_merge: agg '${name}' (${a.op}) requires a colPlan`);
    }
  }
  if (plan.groupKeys !== undefined) {
    if (!Array.isArray(plan.groupKeys)) throw new TypeError('wh_merge: plan.groupKeys must be an array of column plans');
    for (const g of plan.groupKeys) {
      if (!g || typeof g.col !== 'string' || (g.type !== 'text' && g.type !== 'int8' && g.type !== 'numeric' && g.type !== 'timestamptz')) {
        throw new TypeError('wh_merge: plan.groupKeys entries must be {col, type} column plans');
      }
    }
  }
}

interface ValidatedEnv {
  shard: string;
  table: string;
  schemaVersion: number;
  rows: WhPartialRow[];
  aggs: Record<string, WhAggSpec>;
  rowCount: number;
}

/** Strict v1 envelope validation at the consumption site. `mode` is the
 *  merge mode ('grouped'|'scalar') — a kind/mode mismatch is envelope_invalid. */
function validateEnvelope(raw: unknown, plan: WhMergePlan, mode: 'grouped' | 'scalar'): ValidatedEnv {
  if (raw === null || typeof raw !== 'object') envInvalid('envelope must be an object');
  const e = raw as Record<string, unknown>;
  if (e.v !== 1) envInvalid(`envelope.v must be the literal 1, got ${JSON.stringify(e.v) ?? 'undefined'}`);
  if (typeof e.shard !== 'string' || e.shard.length === 0) envInvalid('envelope.shard must be a non-empty string');
  if (e.table !== plan.table) envInvalid(`envelope.table '${String(e.table)}' !== plan.table '${plan.table}'`);
  if (typeof e.schema_version !== 'number' || !Number.isInteger(e.schema_version)) {
    envInvalid('envelope.schema_version must be an integer');
  }
  const p = e.partial;
  if (p === null || typeof p !== 'object') envInvalid('envelope.partial must be an object');
  const partial = p as Record<string, unknown>;
  if (partial.kind !== mode) envInvalid(`partial.kind '${String(partial.kind)}' !== merge mode '${mode}'`);
  if (typeof partial.rowCount !== 'number' || !Number.isInteger(partial.rowCount) || partial.rowCount < 0) {
    envInvalid('partial.rowCount must be a non-negative integer');
  }
  if (typeof partial.more !== 'boolean') envInvalid('partial.more must be a boolean');
  // P2-1 (r38 review): a truncated partial (more:true) must never merge as
  // complete — §2.0 partition completeness; the engine retries the shard.
  if (partial.more) envInvalid('partial.more=true (truncated partial) is not mergeable — retry the shard');
  if (!Array.isArray(partial.rows)) envInvalid('partial.rows must be an array');
  // P1-1 (r38 review): a scalar partial is EXACTLY one identity row
  // (wh_types contract) — 0 rows crashes finalize, >1 silently double-folds.
  if (mode === 'scalar' && (partial.rows as unknown[]).length !== 1) {
    envInvalid(`scalar partial must carry exactly one row, got ${(partial.rows as unknown[]).length}`);
  }
  // rowCount convention (codebase-wide): the number of rows THIS partial
  // carries (rows.length) — shard partials group-then-emit, and the merged
  // partial keeps the invariant true (monoid closure pin: m12 rowCount = 3
  // groups from 2+2 shard rows). Cross-check the declared field.
  if (partial.rowCount !== (partial.rows as unknown[]).length) {
    envInvalid(`partial.rowCount ${partial.rowCount} !== rows.length ${(partial.rows as unknown[]).length}`);
  }

  // groupKeys: grouped => present and exactly the plan's columns (order
  // matters); scalar => must NOT be declared.
  const planGroupCols = (plan.groupKeys ?? []).map((g) => g.col);
  if (mode === 'grouped') {
    if (!Array.isArray(partial.groupKeys)) envInvalid('grouped partial must declare groupKeys');
    const gk = partial.groupKeys as unknown[];
    if (gk.length !== planGroupCols.length || gk.some((c, i) => c !== planGroupCols[i])) {
      envInvalid(`partial.groupKeys [${gk.map(String).join(',')}] !== plan groupKeys [${planGroupCols.join(',')}]`);
    }
  } else if (partial.groupKeys !== undefined) {
    envInvalid('scalar partial must NOT declare groupKeys');
  }

  // aggs: every plan-referenced aggregate must be declared with matching
  // op/col; every envelope-declared aggregate must carry a known op.
  const decls = partial.aggs;
  if (decls === null || typeof decls !== 'object') envInvalid('partial.aggs must be an object');
  const d = decls as Record<string, Record<string, unknown>>;
  for (const [name, pa] of Object.entries(plan.aggs)) {
    const decl = d[name];
    if (!decl || typeof decl !== 'object') envInvalid(`plan agg '${name}' missing from partial.aggs`);
    if (decl.op !== pa.op) envInvalid(`agg '${name}' declared op '${String(decl.op)}' !== plan op '${pa.op}'`);
    if ((decl.col ?? undefined) !== (pa.col ?? undefined)) {
      envInvalid(`agg '${name}' declared col '${String(decl.col)}' !== plan col '${String(pa.col)}'`);
    }
  }
  for (const [name, decl] of Object.entries(d)) {
    if (!Object.prototype.hasOwnProperty.call(d, name)) continue;
    if (!decl || typeof decl !== 'object' || !OPS.has(String(decl.op))) {
      envInvalid(`envelope agg '${name}' has unknown op '${String(decl?.op)}'`);
    }
  }
  // P3 (r38 review): envelope aggs must be EXACTLY the plan's set — extra
  // declared aggregates mean the shard ran a different query than planned.
  for (const name of Object.keys(d)) {
    if (!(name in plan.aggs) && Object.prototype.hasOwnProperty.call(plan.aggs, name) === false) {
      envInvalid(`envelope declares agg '${name}' which the plan does not`);
    }
  }

  for (const row of partial.rows as unknown[]) {
    if (row === null || typeof row !== 'object') envInvalid('partial row must be an object');
    const r = row as Record<string, unknown>;
    if (!Array.isArray(r.k)) arityMismatch('group key must be an ordered array (object-form keys are forbidden)');
    const want = mode === 'grouped' ? planGroupCols.length : 0;
    if ((r.k as unknown[]).length !== want) {
      arityMismatch(`group key arity ${(r.k as unknown[]).length} !== plan arity ${want}`);
    }
    if (r.a === null || typeof r.a !== 'object') envInvalid('partial row must carry an aggregate-value map');
    const a = r.a as Record<string, unknown>;
    for (const name of Object.keys(plan.aggs)) {
      if (!Object.prototype.hasOwnProperty.call(a, name)) envInvalid(`partial row missing declared agg '${name}'`);
    }
  }
  return {
    shard: e.shard,
    table: e.table,
    schemaVersion: e.schema_version,
    rows: partial.rows as WhPartialRow[],
    aggs: d as unknown as Record<string, WhAggSpec>,
    rowCount: partial.rowCount,
  };
}

/** Canonicalize one partial aggregate VALUE (or null = empty contribution). */
function canonAggValue(name: string, pa: WhAggPlan, v: unknown): WhCanonicalValue | number {
  if (pa.op === 'count') {
    if (typeof v !== 'number' || !Number.isInteger(v) || v < 0) {
      typeMismatch(`count partial '${name}' must be a non-negative integer, got ${JSON.stringify(v) ?? 'null'}`);
    }
    return v;
  }
  if (v === null || v === undefined) return null;
  try {
    return canonicalizeValue(v, pa.colPlan as WhColumnPlan);
  } catch (err) {
    if (err instanceof WhCanonicalizeError) {
      typeMismatch(`agg '${name}': ${(err as Error).message}`);
    }
    throw err;
  }
}

// ---------- accumulators (merge state per aggregate) ----------
type Acc =
  | { kind: 'count'; total: number }
  | { kind: 'sum'; total: bigint; seen: boolean }
  | { kind: 'minmax'; op: 'min' | 'max'; cur: WhCanonicalValue | null; colPlan: WhColumnPlan }
  | { kind: 'avg'; s: bigint | null; c: number };

function newAcc(name: string, pa: WhAggPlan): Acc {
  if (pa.op === 'count') return { kind: 'count', total: 0 };
  if (pa.op === 'sum') return { kind: 'sum', total: 0n, seen: false };
  if (pa.op === 'min' || pa.op === 'max') return { kind: 'minmax', op: pa.op, cur: null, colPlan: pa.colPlan as WhColumnPlan };
  return { kind: 'avg', s: null, c: 0 };
}

function accMerge(name: string, pa: WhAggPlan, acc: Acc, raw: unknown): void {
  if (acc.kind === 'count') {
    // COUNT partials are plain non-negative integers — never NULL (PG counts
    // are never NULL; null here means a malformed partial, type_mismatch).
    if (typeof raw !== 'number' || !Number.isInteger(raw) || raw < 0) {
      typeMismatch(`count partial '${name}' must be a non-negative integer, got ${JSON.stringify(raw) ?? 'null'}`);
    }
    acc.total += raw;
    return;
  }
  if (acc.kind === 'avg') {
    // The PAIR {s, c} with s:null <=> c:0 (§2.2) — validate the invariant,
    // then merge. Never the mean; never a bare number.
    if (raw === null || typeof raw !== 'object' || Array.isArray(raw)) {
      typeMismatch(`avg partial '${name}' must be the {s, c} pair, got ${JSON.stringify(raw) ?? 'null'}`);
    }
    const pair = raw as Record<string, unknown>;
    const s = pair.s;
    const c = pair.c;
    if (typeof c !== 'number' || !Number.isInteger(c) || c < 0) {
      typeMismatch(`avg partial '${name}' count must be a non-negative integer, got ${JSON.stringify(c) ?? 'null'}`);
    }
    if ((s === null || s === undefined) !== (c === 0)) {
      typeMismatch(`avg partial '${name}' is inconsistent: s:${JSON.stringify(s)} with c:${c} (s:null <=> c:0)`);
    }
    let sb: WhCanonicalValue = null;
    if (s !== null && s !== undefined) {
      try {
        sb = canonicalizeValue(s, pa.colPlan as WhColumnPlan);
      } catch (err) {
        if (err instanceof WhCanonicalizeError) {
          typeMismatch(`agg '${name}': ${(err as Error).message}`);
        }
        throw err;
      }
    }
    acc.c += c;
    if (sb !== null) acc.s = (acc.s ?? 0n) + (sb as bigint);
    return;
  }
  const v = canonAggValue(name, pa, raw);
  if (v === null) return; // empty contribution: identity for every agg
  if (acc.kind === 'sum') {
    acc.total += v as bigint;
    acc.seen = true;
    return;
  }
  // minmax — compare via the TYPED comparator (P1-2: JS `<` on canonical
  // strings is UTF-16-unit order, inverting astral-plane text; BigInt `<`
  // is fine but one path for all types keeps the law in one place).
  const c = compareCanonical(v as WhCanonicalValue, acc.cur as WhCanonicalValue, acc.colPlan, false);
  acc.cur =
    acc.cur === null || (acc.op === 'min' ? c < 0 : c > 0)
      ? (v as WhCanonicalValue)
      : acc.cur;
}

/** Render a scaled fixed-point bigint back to DECIMAL TEXT at the plan's
 *  scale — the P0-1 fix: a bare toString() emits the SCALED integer (613.95
 *  @2 -> "61395"), which re-canonicalizes ×10^scale and silently inflates
 *  every numeric(scale>=1) merge through the wire. renderScaled(61395n, 2)
 *  = "613.95"; renderScaled(-5n, 2) = "-0.05"; scale 0 = plain digits. */
export function renderScaled(v: bigint, scale: number): string {
  const neg = v < 0n;
  const digits = (neg ? -v : v).toString().padStart(scale + 1, '0');
  const i = digits.length - scale;
  const text = scale === 0 ? digits : `${digits.slice(0, i)}.${digits.slice(i)}`;
  return neg ? '-' + text : text;
}

/** Render an accumulator back to a WIRE value (text path for bigints — the
 *  exact carrier per §2.0), so the merged partial is re-consumable. Numeric
 *  values re-render at the plan's scale (P0-1); text/timestamptz canonical
 *  strings pass through as-is. */
function accToWire(name: string, pa: WhAggPlan, acc: Acc): unknown {
  if (acc.kind === 'count') return acc.total;
  if (acc.kind === 'sum') return acc.seen ? renderScaledBigInt(acc.total, pa) : null;
  if (acc.kind === 'minmax') return acc.cur === null ? null : renderScaledCanonical(acc.cur, pa);
  return { s: acc.s === null ? null : renderScaledBigInt(acc.s as bigint, pa), c: acc.c };
}

function renderScaledBigInt(v: bigint, pa: WhAggPlan): string {
  const t = pa.colPlan?.type;
  if (t === 'numeric') return renderScaled(v, pa.colPlan?.scale ?? 0);
  return v.toString(); // int8 (validatePlan restricts numeric aggregates to int8|numeric)
}

function renderScaledCanonical(v: WhCanonicalValue, pa: WhAggPlan): string {
  if (typeof v === 'bigint') return renderScaledBigInt(v, pa);
  return v as string; // text/timestamptz canonical strings; null unreachable (cur===null checked by the caller)
}

function accToFinal(name: string, pa: WhAggPlan, acc: Acc): WhFinalAggValue {
  if (acc.kind === 'count') return acc.total;
  if (acc.kind === 'sum') return acc.seen ? acc.total : null;
  if (acc.kind === 'minmax') return acc.cur;
  // avg: exact rational; zero non-NULL inputs => NULL (empty-input rule).
  return acc.c === 0 ? null : { num: acc.s as bigint, den: BigInt(acc.c) };
}

interface GroupState {
  slots: WhCanonicalValue[];
  key: string;
  accs: Map<string, Acc>;
}

/** Shared streaming merge: fold validated envelopes into group states. */
function mergeEnvelopes(plan: WhMergePlan, envelopes: readonly unknown[], mode: 'grouped' | 'scalar'): {
  schemaVersion: number;
  groups: Map<string, GroupState>;
  rowCount: number;
} {
  validatePlan(plan);
  const aggNames = Object.keys(plan.aggs);
  const groups = new Map<string, GroupState>();
  let schemaVersion = 1;
  let first = true;
  let rowCount = 0;

  for (const raw of envelopes) {
    const env = validateEnvelope(raw, plan, mode);
    if (first) {
      schemaVersion = env.schemaVersion;
      first = false;
    } else if (env.schemaVersion !== schemaVersion) {
      envInvalid(`envelope schema_version ${env.schemaVersion} !== fleet schema_version ${schemaVersion}`);
    }
    rowCount += env.rowCount;
    for (const row of env.rows) {
      // canonicalize group slots (scalar: k is [], nothing to do)
      const slots: WhCanonicalValue[] = [];
      for (let i = 0; i < (plan.groupKeys ?? []).length; i++) {
        const gk = (plan.groupKeys as WhColumnPlan[])[i];
        let slot: WhCanonicalValue;
        try {
          slot = canonicalizeValue((row.k as unknown[])[i], gk);
        } catch (err) {
          if (err instanceof WhCanonicalizeError || err instanceof WhMergeError) {
            typeMismatch(`group key slot ${i} ('${gk.col}'): ${(err as Error).message}`);
          }
          throw err;
        }
        slots.push(slot);
      }
      const key = canonicalGroupKey(slots);
      let g = groups.get(key);
      if (!g) {
        g = { slots, key, accs: new Map() };
        for (const name of aggNames) g.accs.set(name, newAcc(name, plan.aggs[name]));
        groups.set(key, g);
      }
      for (const name of aggNames) {
        if (!(name in (row.a as Record<string, unknown>))) {
          envInvalid(`partial row missing declared agg '${name}'`);
        }
        try {
          accMerge(name, plan.aggs[name], g.accs.get(name) as Acc, (row.a as Record<string, unknown>)[name]);
        } catch (err) {
          if (err instanceof WhMergeError) {
            typeMismatch(`shard '${env.shard}' agg '${name}': ${(err as Error).message}`);
          }
          throw err;
        }
      }
    }
  }
  return { schemaVersion, groups, rowCount };
}

/** Merge grouped partials (QC2). Returns a valid v1 GROUPED envelope
 *  (shard '<merged>') — monoid closure: re-consumable as-is. */
export function mergeGroupedPartials(plan: WhMergePlan, envelopes: readonly unknown[]): WhPartialEnvelope {
  const { schemaVersion, groups, rowCount } = mergeEnvelopes(plan, envelopes, 'grouped');
  const decls: Record<string, { op: WhAggPlan['op']; col?: string }> = {};
  for (const [name, pa] of Object.entries(plan.aggs)) {
    decls[name] = pa.col === undefined ? { op: pa.op } : { op: pa.op, col: pa.col };
  }
  const rows = [...groups.values()].map((g) => {
    const a: Record<string, unknown> = {};
    for (const name of Object.keys(plan.aggs)) a[name] = accToWire(name, plan.aggs[name], g.accs.get(name) as Acc);
    return { k: g.slots, a };
  });
  return {
    v: 1,
    shard: '<merged>',
    table: plan.table,
    schema_version: schemaVersion,
    partial: {
      kind: 'grouped',
      groupKeys: (plan.groupKeys ?? []).map((g) => g.col),
      aggs: decls as WhPartialEnvelope['partial']['aggs'],
      rows,
      rowCount: rows.length, // codebase convention: rowCount = this partial's row count (monoid closure keeps it true)
      more: false,
    },
  };
}

/** Merge scalar (bare-aggregate) partials. Returns a valid v1 SCALAR
 *  envelope (one identity row — valid over ZERO envelopes too). */
export function mergeScalarAggs(plan: WhMergePlan, envelopes: readonly unknown[]): WhPartialEnvelope {
  const { schemaVersion, groups, rowCount } = mergeEnvelopes(plan, envelopes, 'scalar');
  let state = groups.get('0:');
  const decls: Record<string, { op: WhAggPlan['op']; col?: string }> = {};
  for (const [name, pa] of Object.entries(plan.aggs)) {
    decls[name] = pa.col === undefined ? { op: pa.op } : { op: pa.op, col: pa.col };
  }
  if (!state) {
    state = { slots: [], key: '0:', accs: new Map() };
    for (const name of Object.keys(plan.aggs)) state.accs.set(name, newAcc(name, plan.aggs[name]));
  }
  const a: Record<string, unknown> = {};
  for (const name of Object.keys(plan.aggs)) a[name] = accToWire(name, plan.aggs[name], state.accs.get(name) as Acc);
  return {
    v: 1,
    shard: '<merged>',
    table: plan.table,
    schema_version: schemaVersion,
    partial: {
      kind: 'scalar',
      aggs: decls as WhPartialEnvelope['partial']['aggs'],
      rows: [{ k: [], a }],
      rowCount: 1, // the identity is one wire row (even over zero envelopes)
      more: false,
    },
  };
}

/** Deterministic canonical group order: per-slot asc with SQL NULL last
 *  (the E1 pin: ['ap','eu','us']; the E12 pin: ['','eu','null',null]). */
function canonicalSlotOrder(x: WhCanonicalValue, y: WhCanonicalValue, plan: WhColumnPlan): number {
  return compareCanonical(x, y, plan, false);
}

function sortedGroupKeys(plan: WhMergePlan, groups: Map<string, GroupState>): GroupState[] {
  const gks = plan.groupKeys ?? [];
  return [...groups.values()].sort((g1, g2) => {
    for (let i = 0; i < gks.length; i++) {
      const c = canonicalSlotOrder(g1.slots[i], g2.slots[i], gks[i]);
      if (c !== 0) return c;
    }
    return 0;
  });
}

/** r148 (design_r147_g2_rankbyagg §1-2): the additive OPTIONAL rank_by
 *  directive — `query.rank_by = {agg, direction?}`. `agg` NAMES a plan
 *  aggregate (the buildMergePlan envelope key the consumer sees: alias ??
 *  'op(col)' / 'count(*)' — the W8 flagship avg(amount) ships UNALIASED, so
 *  alias-only binding would make the canonical ask unrankable; the P1-2
 *  fold both r147 audits converged on). The {op,col} reference form is NOT
 *  admitted (col-strict r130 law: no second matching path). direction
 *  defaults to 'desc' (top-N). Shape-only at parse; binding + scope +
 *  grouped-only are PLAN-TIME gates (wh_engine_core buildMergePlan). */
export interface WhRankBy {
  agg: string;
  direction?: 'asc' | 'desc';
}

/** The post-merge rank comparator over FINALIZED grouped rows
 *  (design_r147_g2_rankbyagg §1 binding clauses 5-8). Pure function of
 *  (merged set, rankBy, tie law) — the R1-R6 determinism law: band layout,
 *  shard count and arrival order cannot change the output.
 *
 *  Value law (clause 5, EXACT — never float): avg pairs {num, den} compare
 *  by BigInt cross-multiply num1*den2 vs num2*den1 (a float-quotient is a
 *  REGISTERED MUTANT — battery arm 6b); count/count_col are JS numbers
 *  (integer-gated accumulation); sum finals are scaled BigInts (ONE colPlan
 *  scale per agg — same-scale compare is monotone; the wire 's' TEXT law is
 *  bypassed, never parsed here — canonicalization already happened at merge
 *  ingestion). den > 0 is STRUCTURAL: accToFinal emits a pair only when
 *  c > 0 (c === 0 finalizes NULL first). min/max are OUT of the v1 rank
 *  scope (the binding gate rejects them) — their string/bigint finals must
 *  never reach this comparator (fail-closed TypeError if they do).
 *
 *  NULL law (clause 6): NULLs LAST, FIXED in BOTH directions — placement
 *  NEVER flips with direction (the makeTypedComparator placement law);
 *  two NULLs are peers (tie → key-asc). NULL groups are RANKED (in rows),
 *  never excluded — exclusion would break the K-prefix contract.
 *
 *  Tie law (clause 7): canonical key-ASCENDING among value peers, EXPLICIT
 *  in the comparator (never sort-stability — the input order is an
 *  implementation detail; the battery kills the stability-reliant mutant
 *  by reversing the pre-sort order before ranking). */
export function rankComparator(
  rankBy: WhRankBy,
  groupKeys: WhColumnPlan[],
): (a: WhGroupFinal, b: WhGroupFinal) => number {
  const valueCompare = (va: WhFinalAggValue, vb: WhFinalAggValue): number => {
    if (va === null && vb === null) return 0;
    if (va === null) return 1; // NULLs last — FIXED, direction-independent
    if (vb === null) return -1;
    if (typeof va === 'object' && typeof vb === 'object') {
      // exact rational cross-multiply — the E2 hard clause, never a float
      const l = va.num * vb.den;
      const r = vb.num * va.den;
      return l < r ? -1 : l > r ? 1 : 0;
    }
    if (typeof va === 'object' || typeof vb === 'object') {
      // one agg name = one accumulator kind; mixed shapes are structurally
      // impossible — fail-closed loud, never silently wrong
      throw new TypeError('rankComparator: mixed finalized shapes for one aggregate name');
    }
    if (typeof va === 'bigint' && typeof vb === 'bigint') return va < vb ? -1 : va > vb ? 1 : 0;
    if (typeof va === 'number' && typeof vb === 'number') return va < vb ? -1 : va > vb ? 1 : 0;
    // min/max string finals: outside the v1 rank scope (binding gate)
    throw new TypeError('rankComparator: finalized value outside the v1 rank scope (avg|count|sum)');
  };
  return (a, b) => {
    const c = valueCompare(a.aggs[rankBy.agg], b.aggs[rankBy.agg]);
    if (c !== 0) return rankBy.direction === 'asc' ? c : -c;
    // explicit canonical key-ascending tiebreak — clause 7
    for (let i = 0; i < groupKeys.length; i++) {
      const kc = canonicalSlotOrder(a.k[i], b.k[i], groupKeys[i]);
      if (kc !== 0) return kc;
    }
    return 0;
  };
}

/** Finalize a merged GROUPED envelope into final rows. Group keys are
 *  canonical (bigint for int8 slots, string for text/timestamps, null for
 *  SQL NULL); output order is deterministic (canonical asc, nulls last). */
export function finalizeGroups(merged: unknown, plan: WhMergePlan): WhGroupFinal[] {
  const env = validateEnvelope(merged, plan, 'grouped');
  // Rebuild accumulators from the merged rows (single pass — the merged
  // envelope is already the fully-folded state).
  const groups = new Map<string, GroupState>();
  for (const row of env.rows) {
    const slots: WhCanonicalValue[] = [];
    for (let i = 0; i < (plan.groupKeys ?? []).length; i++) {
      slots.push(canonicalizeValue((row.k as unknown[])[i], (plan.groupKeys as WhColumnPlan[])[i]));
    }
    const key = canonicalGroupKey(slots);
    let g = groups.get(key);
    if (!g) {
      g = { slots, key, accs: new Map() };
      for (const name of Object.keys(plan.aggs)) g.accs.set(name, newAcc(name, plan.aggs[name]));
      groups.set(key, g);
    }
    for (const [name, pa] of Object.entries(plan.aggs)) {
      accMerge(name, pa, g.accs.get(name) as Acc, (row.a as Record<string, unknown>)[name]);
    }
  }
  return sortedGroupKeys(plan, groups).map((g) => {
    const aggs: Record<string, WhFinalAggValue> = {};
    for (const [name, pa] of Object.entries(plan.aggs)) {
      aggs[name] = accToFinal(name, pa, g.accs.get(name) as Acc);
    }
    return { k: g.slots, aggs };
  });
}

/** Finalize a merged SCALAR envelope into {aggName: final value}. */
export function finalizeScalarAggs(merged: unknown, plan: WhMergePlan): Record<string, WhFinalAggValue> {
  const env = validateEnvelope(merged, plan, 'scalar');
  const row = env.rows[0];
  const out: Record<string, WhFinalAggValue> = {};
  const scratch = new Map<string, Acc>();
  for (const [name, pa] of Object.entries(plan.aggs)) {
    const acc = newAcc(name, pa);
    accMerge(name, pa, acc, (row.a as Record<string, unknown>)[name]);
    scratch.set(name, acc);
  }
  for (const [name, acc] of scratch) out[name] = accToFinal(name, plan.aggs[name], acc);
  return out;
}

export type { WhPartialRow };
