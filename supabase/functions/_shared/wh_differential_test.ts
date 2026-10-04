// =============================================================================
// _shared/wh_differential_test.ts — OFFLINE differential harness (r38).
// =============================================================================
// Three-way agreement, no live DB (scatter §8.1; PAT-gated live capture lands
// later as extra fixtures with "source":"pg-captured"):
//   (1) the INDEPENDENT reference reducer in THIS file (definitionally-written
//       single-DB evaluator: collect all rows, group naively, aggregate per PG
//       semantics with BigInt, empty -> NULL except COUNT);
//   (2) the production distributed path: per-shard partials (buildShardPartial
//       — a THIRD independent implementation, see wh_testutil.ts) merged via
//       wh_merge + finalized;
//   (3) hand-derived fixtures from the doc's worked examples (E1/E2/E7/E12/
//       E14) — the SQL ground truth checks the checker: the reference must
//       reproduce every fixture's expected result, and the merge must agree
//       with BOTH.
// Independence: the reference shares ZERO code with wh_merge/wh_canonical/
// buildShardPartial (own grouping on JSON.stringify of raw slots, own BigInt
// aggregation, own ordering).
// =============================================================================

import { finalizeGroups, finalizeScalarAggs, mergeGroupedPartials, mergeScalarAggs } from './wh_merge.ts';
import type { WhFinalAggValue, WhMergePlan } from './wh_merge.ts';
import type { WhPartialEnvelope } from './wh_types.ts';
import { buildShardPartial, deepEq, mulberry32, show } from './wh_testutil.ts';

let passed = 0;
let failed = 0;

function eq(name: string, actual: unknown, expected: unknown): void {
  if (deepEq(actual, expected)) {
    passed++;
    console.log(`  ok  ${name}`);
  } else {
    failed++;
    console.error(`FAIL  ${name}\n      expected: ${show(expected)}\n      actual:   ${show(actual)}`);
  }
}

// -----------------------------------------------------------------------------
// The reference reducer (single-DB stand-in) — deliberately naive.
// -----------------------------------------------------------------------------

type Row = Record<string, unknown>;

interface Query {
  groupKeys: { col: string; type: 'text' | 'int8' | 'numeric' | 'timestamptz' }[];
  aggs: Record<string, { op: 'sum' | 'count' | 'min' | 'max' | 'avg'; col?: string; colPlan?: { col: string; type: string; scale?: number } }>;
}

/** PG aggregate semantics over a row set, with BigInt. scale-aware for
 *  numeric corpora ('13.45' @2 -> 1345n; corpus magnitudes keep Number math
 *  exact). */
function referenceEvaluate(q: Query, rows: Row[], scale: number): Record<string, WhFinalAggValue> {
  const out: Record<string, WhFinalAggValue> = {};
  for (const [name, a] of Object.entries(q.aggs)) {
    if (a.op === 'count') {
      out[name] = a.col === undefined ? rows.length : rows.filter((r) => r[a.col as string] !== null && r[a.col as string] !== undefined).length;
      continue;
    }
    const vals = rows
      .map((r) => r[a.col as string])
      .filter((v) => v !== null && v !== undefined)
      .map((v) => BigInt(Math.round(Number(v) * 10 ** scale)));
    if (a.op === 'sum') out[name] = vals.length === 0 ? null : vals.reduce((x, y) => x + y, 0n);
    else if (a.op === 'min') out[name] = vals.length === 0 ? null : vals.reduce((x, y) => (y < x ? y : x));
    else if (a.op === 'max') out[name] = vals.length === 0 ? null : vals.reduce((x, y) => (y > x ? y : x));
    else out[name] = vals.length === 0 ? null : { num: vals.reduce((x, y) => x + y, 0n), den: BigInt(vals.length) };
  }
  return out;
}

/** The unsharded single-DB answer. Groups naively on raw slots
 *  (undefined coerced to null — SQL NULL clusters); insertion order. */
function referenceQuery(q: Query, allRows: Row[], scale = 0): { k: unknown[]; aggs: Record<string, WhFinalAggValue> }[] | Record<string, WhFinalAggValue> {
  if (q.groupKeys.length === 0) return referenceEvaluate(q, allRows, scale);
  const groups = new Map<string, { slots: unknown[]; rows: Row[] }>();
  for (const r of allRows) {
    const slots = q.groupKeys.map((g) => (r[g.col] === undefined ? null : r[g.col]));
    const key = JSON.stringify(slots);
    let g = groups.get(key);
    if (!g) {
      g = { slots, rows: [] };
      groups.set(key, g);
    }
    g.rows.push(r);
  }
  return [...groups.values()].map((g) => ({ k: g.slots, aggs: referenceEvaluate(q, g.rows, scale) }));
}

/** Order-insensitive comparison space: normalize raw int8 slots to BigInt and
 *  sort by a JSON key (both sides) — group ORDER is not the contract here
 *  (wh_merge_test pins the canonical order; this file pins MEMBERSHIP+VALUES
 *  against the reference). */
function canonComparable(x: unknown): unknown {
  if (Array.isArray(x)) return x.map(canonComparable);
  if (x !== null && typeof x === 'object') {
    const o: Record<string, unknown> = {};
    for (const k of Object.keys(x as Record<string, unknown>).sort()) o[k] = canonComparable((x as Record<string, unknown>)[k]);
    return o;
  }
  return typeof x === 'number' ? BigInt(x) : x;
}

function normalized(x: unknown): unknown {
  return canonComparable(x);
}

function sortedRows(x: unknown, grouped: boolean): unknown {
  if (!grouped || !Array.isArray(x)) return x;
  return x.slice().map((g) => {
    const gg = g as { k: unknown[]; aggs: unknown };
    return { k: canonComparable(gg.k), aggs: canonComparable(gg.aggs) };
  }).sort((a, b) => (show(a) < show(b) ? -1 : 1));
}

// -----------------------------------------------------------------------------
// Property driver: randomized datasets -> three-way agreement.
// -----------------------------------------------------------------------------

const TEXT_POOL = ['eu', 'us', 'ap', '', 'null', 'z'];

interface Dataset {
  groupCols: string[];
  groupTypes: ('text' | 'int8')[];
  valType: 'int8' | 'numeric';
  valScale: number;
  op: 'sum' | 'count' | 'min' | 'max' | 'avg';
  shards: Row[][];
}

function genDataset(rng: () => number, op: Dataset['op']): Dataset {
  const nShards = 1 + Math.floor(rng() * 9); // 1..9 (includes single-shard fleets)
  const nullHeavy = rng() < 0.4;
  const pNull = nullHeavy ? 0.5 : 0.15;
  const valType: 'int8' | 'numeric' = rng() < 0.5 ? 'int8' : 'numeric';
  const valScale = valType === 'numeric' ? (rng() < 0.5 ? 0 : 2) : 0;
  const grouped = rng() < 0.6;
  const groupCols = grouped ? (rng() < 0.5 ? ['g1'] : ['g1', 'g2']) : [];
  const groupTypes: ('text' | 'int8')[] = groupCols.map((_, i) => (i === 0 ? (rng() < 0.6 ? 'text' : 'int8') : rng() < 0.5 ? 'int8' : 'text'));
  const shards: Row[][] = [];
  for (let s = 0; s < nShards; s++) {
    const n = s === nShards - 1 && nShards > 1 ? 0 : Math.floor(rng() * 41); // an empty shard in every multi-shard fleet
    const rows: Row[] = [];
    for (let r = 0; r < n; r++) {
      const row: Row = {};
      groupCols.forEach((c, i) => {
        if (groupTypes[i] === 'text') row[c] = rng() < pNull * 0.5 ? null : TEXT_POOL[Math.floor(rng() * TEXT_POOL.length)];
        else row[c] = rng() < pNull ? null : Math.floor(rng() * 5);
      });
      row.val = rng() < pNull
        ? null
        : (valScale === 2 ? (Math.floor(rng() * 10000) - 2500) / 100 : Math.floor(rng() * 101) - 25);
      if (valScale === 2 && row.val !== null) row.val = (row.val as number).toFixed(2);
      rows.push(row);
    }
    shards.push(rows);
  }
  return { groupCols, groupTypes, valType, valScale, op, shards };
}

function planFor(d: Dataset): WhMergePlan {
  const aggs: WhMergePlan['aggs'] = { c: { op: 'count' }, v: { op: d.op, col: 'val', colPlan: { col: 'val', type: d.valType, scale: d.valScale } } };
  const plan: WhMergePlan = { table: 'fuzz', aggs };
  if (d.groupCols.length > 0) plan.groupKeys = d.groupCols.map((c, i) => ({ col: c, type: d.groupTypes[i] }));
  return plan;
}

function queryFor(d: Dataset): Query {
  return {
    groupKeys: d.groupCols.map((c, i) => ({ col: c, type: d.groupTypes[i] })),
    aggs: {
      c: { op: 'count' },
      v: { op: d.op, col: 'val', colPlan: { col: 'val', type: d.valType, scale: 0 } },
    },
  };
}

function distributedAnswer(d: Dataset, plan: WhMergePlan, render: 'string' | 'number' | 'mixed', rnd?: () => number): unknown {
  // agg specs derive from the PLAN (fixture plans carry their own agg names:
  // E1 s/c, E2 a, E7 m, E12 c, E14 s/c — never hardcoded here).
  const aggs: { name: string; op: 'sum' | 'count' | 'min' | 'max' | 'avg'; col?: string }[] =
    Object.entries(plan.aggs).map(([name, pa]) => ({ name, op: pa.op, col: pa.col }));
  const scale = Object.values(plan.aggs).find((pa) => pa.colPlan?.type === 'numeric')?.colPlan?.scale ?? 0;
  const envs: WhPartialEnvelope[] = d.shards.map((rows, i) =>
    buildShardPartial(rows, {
      shard: `s${i}`,
      table: plan.table,
      groupKeyCols: d.groupCols.length > 0 ? d.groupCols : undefined,
      aggs,
      valScale: scale,
      render,
      rnd,
    })
  );
  const grouped = d.groupCols.length > 0;
  return grouped ? finalizeGroups(mergeGroupedPartials(plan, envs), plan) : finalizeScalarAggs(mergeScalarAggs(plan, envs), plan);
}

function renderCounts(): { string: number; number: number; mixed: number } {
  return { string: 0, number: 0, mixed: 0 };
}

const RENDER_BUDGET = renderCounts();

for (const op of ['sum', 'count', 'min', 'max', 'avg'] as const) {
  for (let seed = 1; seed <= 42; seed++) {
    const dataset = genDataset(mulberry32(0xd1ff0000 + seed * 7919 + op.charCodeAt(0) * 31), op);
    Deno.test(`diff ${op} seed=${seed} (${dataset.shards.length} shards, ${dataset.groupCols.length || 'no'} group keys)`, () => {
      const plan = planFor(dataset);
      const q = queryFor(dataset);
      const allRows = dataset.shards.flat();
      const grouped = dataset.groupCols.length > 0;

      // wire rendering rotates: string (text path) / number (PostgREST
      // default) / mixed (per-value coin flip) — the merge must be
      // carrier-agnostic.
      const render = seed % 3 === 0 ? 'number' : seed % 3 === 1 ? 'string' : 'mixed';
      RENDER_BUDGET[render]++;
      const rnd = mulberry32(0x9e37 + seed);

      const ref = sortedRows(referenceQuery(q, allRows, dataset.valScale), grouped);
      const dist = sortedRows(distributedAnswer(dataset, plan, render, rnd), grouped);
      eq(`diff ${op} seed=${seed}: distributed === reference`, dist, ref);

      // assoc/comm spot: shuffled re-merge equals the arrival merge.
      const aggs: { name: string; op: 'sum' | 'count' | 'min' | 'max' | 'avg'; col?: string }[] = [
        { name: 'c', op: 'count' },
        { name: 'v', op: dataset.op, col: 'val' },
      ];
      const envs: WhPartialEnvelope[] = dataset.shards.map((rows, i) =>
        buildShardPartial(rows, {
          shard: `s${i}`,
          table: plan.table,
          groupKeyCols: dataset.groupCols.length > 0 ? dataset.groupCols : undefined,
          aggs,
          render: 'string',
        })
      );
      const rev = [...envs].reverse();
      const merged = grouped ? finalizeGroups(mergeGroupedPartials(plan, envs), plan) : finalizeScalarAggs(mergeScalarAggs(plan, envs), plan);
      const mergedRev = grouped ? finalizeGroups(mergeGroupedPartials(plan, rev), plan) : finalizeScalarAggs(mergeScalarAggs(plan, rev), plan);
      eq(`diff ${op} seed=${seed}: reversed-merge === arrival-merge`, normalized(mergedRev), normalized(merged));
    });
  }
}

// -----------------------------------------------------------------------------
// Fixtures — hand-derived doc ground truth. The reference must reproduce the
// fixture (the checker is checked), then the merge must agree with both.
// -----------------------------------------------------------------------------

interface Fixture {
  id: string;
  doc: string;
  table: string;
  shards: Record<string, Row[]>;
  query: Query;
  expected: unknown;
}

async function loadFixtures(): Promise<Fixture[]> {
  const names = ['E1_groupby.json', 'E2_avg.json', 'E7_min.json', 'E12_groupkeys.json', 'E14_empty.json'];
  const out: Fixture[] = [];
  for (const n of names) {
    const raw = await Deno.readTextFile(new URL(`./wh_fixtures/${n}`, import.meta.url));
    out.push(JSON.parse(raw) as Fixture);
  }
  return out;
}

Deno.test('fixtures: doc ground truth through reference AND distributed path', async () => {
  const fixtures = await loadFixtures();
  eq('five fixtures loaded', fixtures.length, 5);
  for (const f of fixtures) {
    const allRows = Object.values(f.shards).flat();
    const grouped = f.query.groupKeys.length > 0;
    const ref = sortedRows(referenceQuery(f.query, allRows), grouped);
    eq(`${f.id}: reference reproduces the hand-derived expected`, normalized(ref), normalized(f.expected));

    const d: Dataset = {
      groupCols: f.query.groupKeys.map((g) => g.col),
      groupTypes: f.query.groupKeys.map((g) => (g.type === 'int8' ? 'int8' : 'text')) as ('text' | 'int8')[],
      valType: 'int8',
      op: (Object.values(f.query.aggs).find((a) => a.op !== 'count')?.op ?? 'count') as Dataset['op'],
      shards: Object.values(f.shards),
    };
    const plan: WhMergePlan = { table: f.table, aggs: {} };
    for (const [name, a] of Object.entries(f.query.aggs)) {
      plan.aggs[name] = a.colPlan
        ? { op: a.op, col: a.col, colPlan: { col: a.colPlan.col, type: a.colPlan.type as 'int8' | 'numeric', scale: a.colPlan.scale } }
        : { op: a.op, col: a.col };
    }
    if (grouped) plan.groupKeys = f.query.groupKeys.map((g) => ({ col: g.col, type: g.type as 'text' | 'int8' }));
    // NOTE E12/E14 fixtures use int8-typed val columns; E1's amount is numeric
    // scale 0 — the fixture query carries the exact colPlan per agg.
    for (const [name, a] of Object.entries(f.query.aggs)) {
      if (a.colPlan && plan.aggs[name]) {
        plan.aggs[name].colPlan = { col: a.colPlan.col, type: a.colPlan.type as 'int8' | 'numeric', scale: a.colPlan.scale };
      }
    }
    const dist = sortedRows(distributedAnswer(d, plan, 'string'), grouped);
    eq(`${f.id}: distributed merge agrees with the doc ground truth`, normalized(dist), normalized(f.expected));
  }
});

// -----------------------------------------------------------------------------
// Harness report.
// -----------------------------------------------------------------------------
Deno.test('__report__', () => {
  console.log(`\nwh_differential_test: ${passed} assertions passed, ${failed} failed (render mix: ${JSON.stringify(RENDER_BUDGET)})`);
  if (failed > 0) throw new Error(`${failed} assertion(s) failed`);
});
