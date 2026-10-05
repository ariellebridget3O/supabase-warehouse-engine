// =============================================================================
// _shared/wh_merge_test.ts — RED-first proofs for the aggregate MERGE ALGEBRA.
// =============================================================================
// Normative source: findings_wh_scatter_gather.md §2.0-§2.2 + §8 worked
// examples E1 (3-shard GROUP BY), E2 (AVG pair algebra ⇒ 99.11, not 55),
// E7 (MIN with an all-NULL shard ⇒ 5), E12 (group-key canonicalization),
// E14 (empty fleet: sum NULL, count 0) + §8.1 test plan (property fuzz for
// associativity/commutativity/identity over randomized shard multisets).
//
// The fuzz uses a seeded inline mulberry32 PRNG (pinned seeds) and a small
// inline reference reducer (collect-all-values, BigInt) — the SHARD PARTIALS
// themselves are built by wh_testutil.buildShardPartial, which is independent
// of the production merge/canonical code on purpose.
//
// RED-first: written BEFORE the wh_merge.ts implementation and observed
// failing against the stub. Offline + pure.
// =============================================================================

import {
  finalizeGroups,
  finalizeScalarAggs,
  mergeGroupedPartials,
  mergeScalarAggs,
  WhMergeError,
} from './wh_merge.ts';
import type { WhFinalAggValue, WhMergePlan } from './wh_merge.ts';
import type { WhPartialEnvelope } from './wh_types.ts';
import { buildShardPartial, deepEq, mulberry32, show, shuffled } from './wh_testutil.ts';

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

function eqTrue(name: string, actual: boolean): void {
  eq(name, actual, true);
}

function throwsCode(name: string, fn: () => unknown, code: WhMergeError['code']): void {
  try {
    fn();
    failed++;
    console.error(`FAIL  ${name} — expected WhMergeError(${code}), got no throw`);
  } catch (err) {
    if (err instanceof WhMergeError && err.code === code) {
      passed++;
      console.log(`  ok  ${name} (WhMergeError ${code})`);
    } else {
      failed++;
      console.error(`FAIL  ${name} — wrong throw: ${err}`);
    }
  }
}

// -----------------------------------------------------------------------------
// Plan + envelope builders (hand-specified partials per the doc's examples).
// -----------------------------------------------------------------------------

const E1_PLAN: WhMergePlan = {
  table: 'orders',
  groupKeys: [{ col: 'region', type: 'text' }],
  aggs: {
    s: { op: 'sum', col: 'amount', colPlan: { col: 'amount', type: 'numeric', scale: 0 } },
    c: { op: 'count' },
  },
};

/** Doc E1 partials verbatim (§8): S1 eu 600/6, us 300/3; S2 eu 400/4, ap 50/1;
 *  S3 us 700/7. Amounts are numeric scale-0, rendered via the text path. */
function e1Envelope(shard: string, rows: { k: string[]; s: string; c: number }[]): WhPartialEnvelope {
  return {
    v: 1,
    shard,
    table: 'orders',
    schema_version: 3,
    partial: {
      kind: 'grouped',
      groupKeys: ['region'],
      aggs: { s: { op: 'sum', col: 'amount' }, c: { op: 'count' } },
      rows: rows.map((r) => ({ k: r.k, a: { s: r.s, c: r.c } })),
      rowCount: rows.length,
      more: false,
    },
  };
}

const E1_ENVELOPES: WhPartialEnvelope[] = [
  e1Envelope('S1', [{ k: ['eu'], s: '600', c: 6 }, { k: ['us'], s: '300', c: 3 }]),
  e1Envelope('S2', [{ k: ['eu'], s: '400', c: 4 }, { k: ['ap'], s: '50', c: 1 }]),
  e1Envelope('S3', [{ k: ['us'], s: '700', c: 7 }]),
];

function scalarEnvelope(shard: string, table: string, aggs: Record<string, unknown>, plan: WhMergePlan): WhPartialEnvelope {
  const decls: Record<string, { op: string; col?: string }> = {};
  for (const [name, pa] of Object.entries(plan.aggs)) {
    decls[name] = pa.col === undefined ? { op: pa.op } : { op: pa.op, col: pa.col };
  }
  return {
    v: 1,
    shard,
    table,
    schema_version: 1,
    partial: { kind: 'scalar', aggs: decls as WhPartialEnvelope['partial']['aggs'], rows: [{ k: [], a: aggs }], rowCount: 1, more: false },
  };
}

// -----------------------------------------------------------------------------
// E1 — 3-shard GROUP BY, end to end (§8 E1). Values EXACTLY per the doc:
// eu=(1000,10), us=(1000,10), ap=(50,1).
// NOTE on order: finalizeGroups emits groups in deterministic canonical-key
// order (asc, nulls last) so equality is arrival-order-independent — the doc
// lists eu/us/ap in first-appearance order, which is arrival-dependent and
// therefore NOT a stable contract. Membership + values pinned exactly.
// -----------------------------------------------------------------------------
Deno.test('E1: 3-shard GROUP BY merges to eu=(1000,10), us=(1000,10), ap=(50,1) — doc values exactly', () => {
  const fin = finalizeGroups(mergeGroupedPartials(E1_PLAN, E1_ENVELOPES), E1_PLAN);
  eq('exactly 3 groups', fin.length, 3);
  const byRegion = new Map(fin.map((g) => [(g.k[0] as string), g.aggs]));
  eq('eu', byRegion.get('eu'), { s: 1000n, c: 10 });
  eq('us', byRegion.get('us'), { s: 1000n, c: 10 });
  eq('ap', byRegion.get('ap'), { s: 50n, c: 1 });
  eq('deterministic canonical order (asc, nulls last): ap, eu, us', fin.map((g) => g.k[0]), ['ap', 'eu', 'us']);
});

Deno.test('E1 assoc+comm spot-check: arrival order, reversed order and pairwise tree all agree', () => {
  const merged = mergeGroupedPartials(E1_PLAN, E1_ENVELOPES);
  const rev = mergeGroupedPartials(E1_PLAN, [...E1_ENVELOPES].reverse());
  const tree = mergeGroupedPartials(
    E1_PLAN,
    [
      mergeGroupedPartials(E1_PLAN, [E1_ENVELOPES[0], E1_ENVELOPES[1]]),
      E1_ENVELOPES[2],
    ],
  );
  eq('reversed === arrival', finalizeGroups(rev, E1_PLAN), finalizeGroups(merged, E1_PLAN));
  eq('pairwise tree === arrival', finalizeGroups(tree, E1_PLAN), finalizeGroups(merged, E1_PLAN));
});

// -----------------------------------------------------------------------------
// E2 — AVG pair algebra. S_A: mean 10 over 10 rows (s=100, c=10); S_B: mean
// 100 over 1000 rows (s=100000, c=1000). Pair merge ⇒ (100100, 1010) ⇒ 99.11.
// Mean-of-means would give (10+100)/2 = 55 — WRONG (assoc fails).
// -----------------------------------------------------------------------------
Deno.test('E2: AVG pair merge gives the exact rational 100100/1010 (≈99.11) — never the mean-of-means 55', () => {
  const plan: WhMergePlan = {
    table: 't',
    aggs: { a: { op: 'avg', col: 'x', colPlan: { col: 'x', type: 'int8' } } },
  };
  const envA = scalarEnvelope('A', 't', { a: { s: '100', c: 10 } }, plan);
  const envB = scalarEnvelope('B', 't', { a: { s: '100000', c: 1000 } }, plan);
  const fin = finalizeScalarAggs(mergeScalarAggs(plan, [envA, envB]), plan);
  const avg = fin.a as { num: bigint; den: bigint };
  eq('exact rational num', avg.num, 100100n);
  eq('exact rational den', avg.den, 1010n);
  const asFloat = Number(avg.num) / Number(avg.den);
  eqTrue('≈ 99.11 (the doc value)', Math.abs(asFloat - 99.10891089108911) < 1e-9);
  eqTrue('NOT the mean-of-means 55 (E2 failure mode)', Math.abs(asFloat - 55) > 1);
  const mergedAB = mergeScalarAggs(plan, [envA, envB]);
  const withReplay = finalizeScalarAggs(mergeScalarAggs(plan, [mergedAB, envA, envB]), plan);
  eqTrue('replayed partials do not corrupt the pair merge is NOT pinned here — replays inflate by design (spec §2.2: SUM-style retry inflation)', true);
  eq('replay actually inflates (documents why retries need dedupe for additive ops)', (withReplay.a as { num: bigint }).num, 200200n);
});

// -----------------------------------------------------------------------------
// E7 — MIN with an all-NULL shard. S1 prices [5,7]; S2 [null,null] ⇒ S2's
// min partial is null (= "no non-null values", identity ∅), NOT a value.
// Treating shard-null as 0 would yield 0; correct = 5.
// -----------------------------------------------------------------------------
Deno.test('E7: MIN over shards {5,7} and {null,null} is 5 — the null partial is the identity, not a value', () => {
  const plan: WhMergePlan = {
    table: 'items',
    aggs: { m: { op: 'min', col: 'price', colPlan: { col: 'price', type: 'int8' } } },
  };
  const env1 = scalarEnvelope('S1', 'items', { m: 5 }, plan);
  const env2 = scalarEnvelope('S2', 'items', { m: null }, plan);
  const fin = finalizeScalarAggs(mergeScalarAggs(plan, [env1, env2]), plan);
  eq('global min is 5', fin.m, 5n);

  const planMax: WhMergePlan = {
    table: 'items',
    aggs: { m: { op: 'max', col: 'price', colPlan: { col: 'price', type: 'int8' } } },
  };
  const finMax = finalizeScalarAggs(mergeScalarAggs(planMax, [
    scalarEnvelope('S1', 'items', { m: 5 }, planMax),
    scalarEnvelope('S2', 'items', { m: null }, planMax),
    scalarEnvelope('S3', 'items', { m: 7 }, planMax),
  ]), planMax);
  eq('MAX symmetric: null partials skipped, global max 7', finMax.m, 7n);
  eq(
    'the all-NULL fleet stays NULL (not 0)',
    finalizeScalarAggs(mergeScalarAggs(plan, [env2, scalarEnvelope('S4', 'items', { m: null }, plan)]), plan).m,
    null,
  );
});

// -----------------------------------------------------------------------------
// E12 — GROUP BY over ('eu'), (''), (null), ('null'): pairwise distinct groups.
// -----------------------------------------------------------------------------
Deno.test('E12: GROUP BY keeps null, empty-string and "null" as distinct groups; one row each', () => {
  const plan: WhMergePlan = {
    table: 't',
    groupKeys: [{ col: 'region', type: 'text' }],
    aggs: { c: { op: 'count' } },
  };
  const env = {
    v: 1 as const,
    shard: 'S1',
    table: 't',
    schema_version: 1,
    partial: {
      kind: 'grouped' as const,
      groupKeys: ['region'],
      aggs: { c: { op: 'count' as const } },
      rows: [
        { k: ['eu'], a: { c: 1 } },
        { k: [''], a: { c: 1 } },
        { k: [null], a: { c: 1 } },
        { k: ['null'], a: { c: 1 } },
      ],
      rowCount: 4,
      more: false,
    },
  };
  const fin = finalizeGroups(mergeGroupedPartials(plan, [env]), plan);
  eq('4 distinct groups, one row each', fin.map((g) => [g.k[0], g.aggs.c]), [
    ['', 1],
    ['eu', 1],
    ['null', 1],
    [null, 1],
  ]);
  // null groups with null across shards (SQL semantics), never with ''
  const env2: WhPartialEnvelope = {
    ...env,
    shard: 'S2',
    partial: { ...env.partial, rows: [{ k: [null], a: { c: 2 } }, { k: [''], a: { c: 3 } }], rowCount: 2 },
  };
  const fin2 = finalizeGroups(mergeGroupedPartials(plan, [env, env2]), plan);
  const byKey = new Map(fin2.map((g) => [JSON.stringify(g.k), g.aggs.c]));
  eq('null cluster: 1+2=3', byKey.get(JSON.stringify([null])), 3);
  eq("'' cluster separate: 1+3=4", byKey.get(JSON.stringify([''])), 4);
  eq('still exactly 4 groups', fin2.length, 4);
});

// -----------------------------------------------------------------------------
// E14 — empty fleet: every shard {sum:null, count:0} ⇒ global sum NULL,
// count 0. Also: ZERO envelopes at all (no shard responded) ⇒ same.
// -----------------------------------------------------------------------------
Deno.test('E14: empty fleet — sum finalizes to NULL (not 0), count to 0; holds for zero envelopes too', () => {
  const plan: WhMergePlan = {
    table: 'orders',
    aggs: {
      s: { op: 'sum', col: 'amount', colPlan: { col: 'amount', type: 'numeric', scale: 0 } },
      c: { op: 'count' },
    },
  };
  const e1 = scalarEnvelope('S1', 'orders', { s: null, c: 0 }, plan);
  const e2 = scalarEnvelope('S2', 'orders', { s: null, c: 0 }, plan);
  eq('two all-empty shards', finalizeScalarAggs(mergeScalarAggs(plan, [e1, e2]), plan), { s: null, c: 0 });
  eq('no envelopes at all (identity)', finalizeScalarAggs(mergeScalarAggs(plan, []), plan), { s: null, c: 0 });
  // grouped variant: WHERE 1=0 with GROUP BY yields zero rows per shard
  const gplan: WhMergePlan = { ...plan, groupKeys: [{ col: 'region', type: 'text' }] };
  const genv = {
    v: 1 as const,
    shard: 'S1',
    table: 'orders',
    schema_version: 1,
    partial: { kind: 'grouped' as const, groupKeys: ['region'], aggs: { s: { op: 'sum' as const, col: 'amount' }, c: { op: 'count' as const } }, rows: [], rowCount: 0, more: false },
  };
  eq('grouped empty shards merge to zero groups', finalizeGroups(mergeGroupedPartials(gplan, [genv]), gplan), []);
});

// -----------------------------------------------------------------------------
// Monoid closure — the merged partial is itself a valid partial (re-mergeable
// as-is). This is what makes associativity hold by construction.
// -----------------------------------------------------------------------------
Deno.test('monoid closure: the merged partial re-consumes as a partial; merge(merge(A,B),C) === merge(A,B,C)', () => {
  const m12 = mergeGroupedPartials(E1_PLAN, [E1_ENVELOPES[0], E1_ENVELOPES[1]]);
  eq('merged partial shape is a valid v1 grouped envelope', [m12.v, m12.partial.kind, m12.partial.rowCount, m12.shard], [1, 'grouped', 3, '<merged>']);
  eq(
    're-merge with C === one-shot over A,B,C',
    finalizeGroups(mergeGroupedPartials(E1_PLAN, [m12, E1_ENVELOPES[2]]), E1_PLAN),
    finalizeGroups(mergeGroupedPartials(E1_PLAN, E1_ENVELOPES), E1_PLAN),
  );
  const ms12 = mergeScalarAggs(E1_PLAN.aggs ? { table: 'orders', aggs: E1_PLAN.aggs } : { table: 'x', aggs: {} }, []);
  eq('scalar merge over zero envelopes is a valid scalar envelope (identity row)', ms12.partial.rows.length, 1);
});

// -----------------------------------------------------------------------------
// Envelope pins — every contract violation is a hard, code-pinned rejection.
// -----------------------------------------------------------------------------
Deno.test('envelope pins: unknown v, mistyped fields and kind/plan mismatch are envelope_invalid (never best-effort parse)', () => {
  const bad = (patch: Record<string, unknown>, kind: 'grouped' | 'scalar' = 'grouped'): unknown => {
    const env = JSON.parse(JSON.stringify(E1_ENVELOPES[0]));
    Object.assign(env, patch);
    env.partial.kind = kind;
    return env;
  };
  throwsCode('v:2 rejected', () => mergeGroupedPartials(E1_PLAN, [bad({ v: 2 })]), 'envelope_invalid');
  throwsCode("v:'1' (string) rejected", () => mergeGroupedPartials(E1_PLAN, [bad({ v: '1' })]), 'envelope_invalid');
  throwsCode('v missing rejected', () => mergeGroupedPartials(E1_PLAN, [bad({ v: undefined })]), 'envelope_invalid');
  throwsCode('rowCount missing rejected', () => mergeGroupedPartials(E1_PLAN, [bad({ partial: { ...E1_ENVELOPES[0].partial, rowCount: undefined } })]), 'envelope_invalid');
  throwsCode('more missing rejected', () => mergeGroupedPartials(E1_PLAN, [bad({ partial: { ...E1_ENVELOPES[0].partial, more: undefined } })]), 'envelope_invalid');
  throwsCode('rows missing rejected', () => mergeGroupedPartials(E1_PLAN, [bad({ partial: { ...E1_ENVELOPES[0].partial, rows: undefined } })]), 'envelope_invalid');
  throwsCode('kind mismatch (grouped merge fed a scalar partial) rejected', () => mergeGroupedPartials(E1_PLAN, [bad({}, 'scalar')]), 'envelope_invalid');
  throwsCode('scalar merge fed a grouped partial rejected', () => {
    const plan: WhMergePlan = { table: 'orders', aggs: E1_PLAN.aggs };
    mergeScalarAggs(plan, [E1_ENVELOPES[0]]);
  }, 'envelope_invalid');
  throwsCode('groupKeys mismatch vs plan rejected', () => {
    const env = JSON.parse(JSON.stringify(E1_ENVELOPES[0]));
    env.partial.groupKeys = ['zone'];
    mergeGroupedPartials(E1_PLAN, [env]);
  }, 'envelope_invalid');
  throwsCode('scalar partial with groupKeys declared rejected', () => {
    const plan: WhMergePlan = { table: 'orders', aggs: E1_PLAN.aggs };
    const env = scalarEnvelope('S1', 'orders', { s: '600', c: 6 }, plan) as unknown as Record<string, unknown>;
    (env.partial as Record<string, unknown>).groupKeys = ['region'];
    mergeScalarAggs(plan, [env]);
  }, 'envelope_invalid');
});

Deno.test('envelope pins: row-arity violations (incl. object-form keys) are arity_mismatch', () => {
  throwsCode('k arity 2 vs plan arity 1 rejected', () => {
    const env = JSON.parse(JSON.stringify(E1_ENVELOPES[0]));
    env.partial.rows[0].k = ['eu', 'west'];
    mergeGroupedPartials(E1_PLAN, [env]);
  }, 'arity_mismatch');
  throwsCode('k arity 0 vs plan arity 1 rejected', () => {
    const env = JSON.parse(JSON.stringify(E1_ENVELOPES[0]));
    env.partial.rows[0].k = [];
    mergeGroupedPartials(E1_PLAN, [env]);
  }, 'arity_mismatch');
  throwsCode('object-form group key rejected (object key order is not semantic)', () => {
    const env = JSON.parse(JSON.stringify(E1_ENVELOPES[0]));
    env.partial.rows[0].k = { region: 'eu' };
    mergeGroupedPartials(E1_PLAN, [env]);
  }, 'arity_mismatch');
  throwsCode('scalar partial row with non-empty k rejected', () => {
    const plan: WhMergePlan = { table: 'orders', aggs: E1_PLAN.aggs };
    const env = scalarEnvelope('S1', 'orders', { s: '600', c: 6 }, plan);
    (env.partial.rows[0].k as unknown[]).push('eu');
    mergeScalarAggs(plan, [env]);
  }, 'arity_mismatch');
});

Deno.test('envelope pins: type violations are type_mismatch', () => {
  throwsCode("text 'abc' in an int8 GROUP-BY column rejected", () => {
    const plan: WhMergePlan = {
      table: 't',
      groupKeys: [{ col: 'n', type: 'int8' }],
      aggs: { c: { op: 'count' } },
    };
    const env = {
      v: 1 as const,
      shard: 'S1',
      table: 't',
      schema_version: 1,
      partial: {
        kind: 'grouped' as const,
        groupKeys: ['n'],
        aggs: { c: { op: 'count' as const } },
        rows: [{ k: ['abc'], a: { c: 1 } }],
        rowCount: 1,
        more: false,
      },
    };
    mergeGroupedPartials(plan, [env]);
  }, 'type_mismatch');
  throwsCode("text 'abc' in an int8 sum column rejected", () => {
    const plan: WhMergePlan = {
      table: 't',
      aggs: { s: { op: 'sum', col: 'x', colPlan: { col: 'x', type: 'int8' } } },
    };
    mergeScalarAggs(plan, [scalarEnvelope('S1', 't', { s: 'abc' }, plan)]);
  }, 'type_mismatch');
  throwsCode('inconsistent avg pair (s:null with c:2) rejected', () => {
    const plan: WhMergePlan = {
      table: 't',
      aggs: { a: { op: 'avg', col: 'x', colPlan: { col: 'x', type: 'int8' } } },
    };
    mergeScalarAggs(plan, [scalarEnvelope('S1', 't', { a: { s: null, c: 2 } }, plan)]);
  }, 'type_mismatch');
  throwsCode('avg partial that is not the {s,c} pair rejected', () => {
    const plan: WhMergePlan = {
      table: 't',
      aggs: { a: { op: 'avg', col: 'x', colPlan: { col: 'x', type: 'int8' } } },
    };
    mergeScalarAggs(plan, [scalarEnvelope('S1', 't', { a: 55 }, plan)]);
  }, 'type_mismatch');
  throwsCode('null count partial rejected (PG counts are never NULL)', () => {
    const plan: WhMergePlan = { table: 't', aggs: { c: { op: 'count' } } };
    mergeScalarAggs(plan, [scalarEnvelope('S1', 't', { c: null }, plan)]);
  }, 'type_mismatch');
  throwsCode('negative count partial rejected', () => {
    const plan: WhMergePlan = { table: 't', aggs: { c: { op: 'count' } } };
    mergeScalarAggs(plan, [scalarEnvelope('S1', 't', { c: -1 }, plan)]);
  }, 'type_mismatch');
});

Deno.test('envelope pins: aggregate-declaration mismatches are envelope_invalid', () => {
  throwsCode('plan-referenced agg missing from partial.aggs rejected', () => {
    const env = JSON.parse(JSON.stringify(E1_ENVELOPES[0]));
    delete env.partial.aggs.s;
    for (const r of env.partial.rows) delete r.a.s;
    mergeGroupedPartials(E1_PLAN, [env]);
  }, 'envelope_invalid');
  throwsCode('row missing a declared agg value rejected', () => {
    const env = JSON.parse(JSON.stringify(E1_ENVELOPES[0]));
    delete env.partial.rows[0].a.s;
    mergeGroupedPartials(E1_PLAN, [env]);
  }, 'envelope_invalid');
  throwsCode('declared op mismatch (plan sum, envelope min) rejected', () => {
    const env = JSON.parse(JSON.stringify(E1_ENVELOPES[0]));
    env.partial.aggs.s.op = 'min';
    mergeGroupedPartials(E1_PLAN, [env]);
  }, 'envelope_invalid');
  throwsCode('declared col mismatch rejected', () => {
    const env = JSON.parse(JSON.stringify(E1_ENVELOPES[0]));
    env.partial.aggs.s.col = 'other';
    mergeGroupedPartials(E1_PLAN, [env]);
  }, 'envelope_invalid');
  throwsCode('unknown op in envelope rejected', () => {
    const env = JSON.parse(JSON.stringify(E1_ENVELOPES[0]));
    env.partial.aggs.extra = { op: 'median', col: 'x' };
    mergeGroupedPartials(E1_PLAN, [env]);
  }, 'envelope_invalid');
});

// -----------------------------------------------------------------------------
// Property fuzz — seeded, pinned. For each reducer: randomized shard multisets
// (2-8 shards, 0-50 rows, NULL-heavy sometimes, duplicated/replayed partials
// sometimes, an empty shard always present). Asserts:
//   merge(arrival) === merge(shuffled) === merge(pairwise tree)  (assoc+comm)
//   merge(deduped) === reference(allRows)                        (correctness)
// Replay note (spec §2.2): additive ops (SUM/COUNT/AVG) are NOT idempotent —
// a replayed partial inflates them by design. So replayed datasets pin
// assoc/comm on the replayed multiset and check the reference on the DEDUPED
// multiset; MIN/MAX are idempotent and must be replay-invariant.
// -----------------------------------------------------------------------------

interface FuzzAgg {
  name: string;
  op: 'sum' | 'count' | 'min' | 'max' | 'avg';
  col?: string;
}

interface FuzzDataset {
  table: string;
  groupCols: string[]; // [] => scalar
  groupTypes: ('text' | 'int8')[];
  aggs: FuzzAgg[];
  valType: 'int8' | 'numeric';
  valScale: number; // numeric corpora exercise scale 0 AND 2 (the P0-1 wire round-trip class)
  shards: Record<string, unknown>[][];
  replayIdx: number | null; // index into flattened envelope list to duplicate
}

const TEXT_POOL = ['eu', 'us', 'ap', '', 'null', 'z'];

function fuzzDataset(seed: number, op: FuzzAgg['op'], grouped: boolean, allowReplay: boolean): FuzzDataset {
  const rng = mulberry32(0x5eed0000 + seed * 7919);
  const nShards = 2 + Math.floor(rng() * 7); // 2..8
  const nullHeavy = rng() < 0.4;
  const pNull = nullHeavy ? 0.5 : 0.15;
  const valType: 'int8' | 'numeric' = rng() < 0.5 ? 'int8' : 'numeric';
  const valScale = valType === 'numeric' ? (rng() < 0.5 ? 0 : 2) : 0;
  const groupCols = grouped ? (rng() < 0.5 ? ['g1'] : ['g1', 'g2']) : [];
  const groupTypes: ('text' | 'int8')[] = groupCols.map((_, i) =>
    i === 0 ? (rng() < 0.6 ? 'text' : 'int8') : (rng() < 0.5 ? 'int8' : 'text')
  );
  const aggs: FuzzAgg[] = [{ name: 'c', op: 'count' }];
  if (op !== 'count') {
    aggs.push({ name: 'v', op, col: 'val' });
  } else {
    aggs.push({ name: 'vc', op: 'count', col: 'val' }); // col-scoped count
  }
  const shards: Record<string, unknown>[][] = [];
  for (let s = 0; s < nShards; s++) {
    const n = s === nShards - 1 ? 0 : Math.floor(rng() * 51); // last shard always empty
    const rows: Record<string, unknown>[] = [];
    for (let r = 0; r < n; r++) {
      const row: Record<string, unknown> = {};
      groupCols.forEach((c, i) => {
        if (groupTypes[i] === 'text') {
          const pick = rng();
          row[c] = pick < pNull * 0.5 ? null : TEXT_POOL[Math.floor(rng() * TEXT_POOL.length)];
        } else {
          row[c] = rng() < pNull ? null : Math.floor(rng() * 5);
        }
      });
      row.val = rng() < pNull
        ? null
        : (valScale === 2 ? (Math.floor(rng() * 10000) - 2500) / 100 : Math.floor(rng() * 101) - 25);
      if (valScale === 2 && row.val !== null) row.val = (row.val as number).toFixed(2);
      rows.push(row);
    }
    shards.push(rows);
  }
  const replayIdx = allowReplay && rng() < 0.5 ? Math.floor(rng() * nShards) : null;
  return { table: 'fuzz', groupCols, groupTypes, aggs, valType, valScale, shards, replayIdx };
}

function fuzzPlan(d: FuzzDataset): WhMergePlan {
  const aggs: WhMergePlan['aggs'] = {};
  for (const a of d.aggs) {
    aggs[a.name] = a.op === 'count'
      ? (a.col === undefined ? { op: 'count' } : { op: 'count', col: a.col })
      : { op: a.op, col: a.col, colPlan: { col: a.col as string, type: d.valType, scale: d.valScale } };
  }
  const plan: WhMergePlan = { table: d.table, aggs };
  if (d.groupCols.length > 0) {
    plan.groupKeys = d.groupCols.map((c, i) => ({ col: c, type: d.groupTypes[i] }));
  }
  return plan;
}

function fuzzEnvelopes(d: FuzzDataset): WhPartialEnvelope[] {
  const envs = d.shards.map((rows, i) =>
    buildShardPartial(rows, {
      shard: `s${i}`,
      table: d.table,
      groupKeyCols: d.groupCols.length > 0 ? d.groupCols : undefined,
      aggs: d.aggs,
      valScale: d.valScale,
      render: 'string', // the exact text path (spec §2.0 numeric-rendering note)
    })
  );
  if (d.replayIdx !== null) envs.push(structuredClone(envs[d.replayIdx]));
  return envs;
}

/** Inline reference reducer (independent of wh_merge): collect all rows,
 *  group naively on raw slots, aggregate per PG semantics with BigInt.
 *  scale-aware for numeric corpora (values '13.45' @2 -> 1345n; corpus
 *  magnitudes keep Number math exact). */
function fuzzReference(d: FuzzDataset): unknown {
  const allRows = d.shards.flat();
  const evaluate = (rows: Record<string, unknown>[]): Record<string, WhFinalAggValue> => {
    const out: Record<string, WhFinalAggValue> = {};
    for (const a of d.aggs) {
      if (a.op === 'count') {
        out[a.name] = a.col === undefined ? rows.length : rows.filter((r) => r[a.col as string] !== null).length;
        continue;
      }
      const vals = rows
        .map((r) => r[a.col as string])
        .filter((v) => v !== null && v !== undefined)
        .map((v) => BigInt(Math.round(Number(v) * 10 ** d.valScale)));
      if (a.op === 'sum') out[a.name] = vals.length === 0 ? null : vals.reduce((x, y) => x + y, 0n);
      else if (a.op === 'min') out[a.name] = vals.length === 0 ? null : vals.reduce((x, y) => (y < x ? y : x));
      else if (a.op === 'max') out[a.name] = vals.length === 0 ? null : vals.reduce((x, y) => (y > x ? y : x));
      else out[a.name] = vals.length === 0 ? null : { num: vals.reduce((x, y) => x + y, 0n), den: BigInt(vals.length) };
    }
    return out;
  };
  if (d.groupCols.length === 0) return evaluate(allRows);
  const groups = new Map<string, { slots: unknown[]; rows: Record<string, unknown>[] }>();
  for (const r of allRows) {
    const slots = d.groupCols.map((c) => (r[c] === undefined ? null : r[c]));
    const key = JSON.stringify(slots);
    let g = groups.get(key);
    if (!g) {
      g = { slots, rows: [] };
      groups.set(key, g);
    }
    g.rows.push(r);
  }
  return [...groups.values()].map((g) => ({ k: g.slots, aggs: evaluate(g.rows) }));
}

function fuzzAssert(d: FuzzDataset, label: string): void {
  const plan = fuzzPlan(d);
  const grouped = d.groupCols.length > 0;
  const merge = (envs: readonly unknown[]): unknown =>
    grouped ? finalizeGroups(mergeGroupedPartials(plan, envs), plan) : finalizeScalarAggs(mergeScalarAggs(plan, envs), plan);
  const envs = fuzzEnvelopes(d);
  const hasReplay = d.replayIdx !== null;

  const arrival = merge(envs);
  const mixed = merge(shuffled(envs, mulberry32(0xc0ffee + d.shards.length)));
  const pairwise = merge(
    envs.length >= 4 ? pairwiseMerge(plan, grouped, envs) : envs,
  );
  eq(`${label}: shuffled-order merge === arrival-order merge`, mixed, arrival);
  eq(`${label}: pairwise-tree merge === arrival-order merge`, pairwise, arrival);

  // Reference check on the DEDUPED multiset (replays inflate additive ops).
  const deduped = hasReplay ? envs.slice(0, envs.length - 1) : envs;
  const dedupedMerge = merge(deduped);
  const reference = normalizeReference(fuzzReference(d), grouped);
  eq(`${label}: merge(deduped partials) === reference(all rows)`, dedupedMerge, reference);

  if (hasReplay) {
    // MIN/MAX are idempotent: replay-invariant even where sums are not.
    const onlyMinMax = (fin: unknown): unknown =>
      grouped
        ? (fin as { k: unknown[]; aggs: Record<string, WhFinalAggValue> }[]).map((g) => ({
          k: g.k,
          aggs: Object.fromEntries(
            Object.entries(g.aggs).filter(([n]) => {
              const a = d.aggs.find((x) => x.name === n);
              return a?.op === 'min' || a?.op === 'max';
            }),
          ),
        }))
        : Object.fromEntries(
          Object.entries((fin as Record<string, WhFinalAggValue>)).filter(([n]) => {
            const a = d.aggs.find((x) => x.name === n);
            return a?.op === 'min' || a?.op === 'max';
          }),
        );
    eq(`${label}: MIN/MAX replay-invariant (idempotent merge)`, onlyMinMax(arrival), onlyMinMax(dedupedMerge));
  }
}

function pairwiseMerge(plan: WhMergePlan, grouped: boolean, envs: readonly unknown[]): unknown {
  // tree: (e0⊕e1) ⊕ (e2⊕e3) ⊕ ... — merged partials re-consumed as partials
  const pairs: unknown[] = [];
  for (let i = 0; i < envs.length; i += 2) {
    const chunk = envs.slice(i, i + 2);
    pairs.push(grouped ? mergeGroupedPartials(plan, chunk) : mergeScalarAggs(plan, chunk));
  }
  return pairs;
}

/** Normalize the reference output into the finalize output space: raw numeric
 *  slots → BigInt when the plan says int8 (reference keys are raw values),
 *  then sort into the same deterministic canonical order finalizeGroups emits
 *  (per-slot asc, SQL NULL last) — deepEq is order-sensitive and the reference
 *  reducer is insertion-ordered by construction. This comparator is written
 *  inline (independent of wh_canonical) on purpose. */
function normalizeReference(ref: unknown, grouped: boolean): unknown {
  if (!grouped) return ref;
  const norm = (ref as { k: unknown[]; aggs: Record<string, WhFinalAggValue> }[]).map((g) => ({
    ...g,
    k: g.k.map((slot) => (typeof slot === 'number' ? BigInt(slot) : slot)),
  }));
  const slotCmp = (a: unknown, b: unknown): number => {
    if (a === null && b === null) return 0;
    if (a === null) return 1; // null last
    if (b === null) return -1;
    if (typeof a === 'bigint' && typeof b === 'bigint') return a < b ? -1 : a > b ? 1 : 0;
    const sa = String(a);
    const sb = String(b);
    return sa < sb ? -1 : sa > sb ? 1 : 0;
  };
  return norm.sort((g1, g2) => {
    for (let i = 0; i < Math.max(g1.k.length, g2.k.length); i++) {
      const c = slotCmp(g1.k[i], g2.k[i]);
      if (c !== 0) return c;
    }
    return 0;
  });
}

for (const op of ['sum', 'count', 'min', 'max', 'avg'] as const) {
  for (const grouped of [false, true]) {
    for (let seed = 1; seed <= 12; seed++) {
      const d = fuzzDataset(seed, op, grouped, true);
      Deno.test(`fuzz ${op} ${grouped ? 'grouped' : 'scalar'} seed=${seed}`, () => {
        fuzzAssert(d, `fuzz ${op} ${grouped ? 'grouped' : 'scalar'} seed=${seed}`);
      });
    }
  }
}

// -----------------------------------------------------------------------------
// r138 B3 (d3 §2 B3 / d1 disposition #2): the invariant-5/6 COMPOSITE —
// additive re-pins on the merge plane. Each arm cites its existing killer:
//   (1) more:true never merges — the r130 P2-1 killer class (a mutant that
//       merges a truncated partial silently inflates every group total);
//   (2) rowCount ≡ rows.length — the r130 §2.0 rowCount-consistency killer
//       class (a lying rowCount launders a trimmed partial as complete);
//   (3) avg fuses ONLY the same-col {s,c} pair — the r129/r133 E2-algebra
//       killer class (mean-of-means / bare-number mutants).
// No new mutant required — the composite is a re-pin (r138 battery law).
// -----------------------------------------------------------------------------
Deno.test('r138 B3 (merge plane): more:true never merges; rowCount ≡ rows.length; avg pair fusion is additive with the null identity', () => {
  // (1) the P2-1 wall: a truncated grouped partial is envelope_invalid —
  // wh_merge is the SECOND wall behind the engine F2 gate (which codes it
  // truncated_groupby pre-merge, wh_handshake_test LETHAL 6b).
  throwsCode('B3 (1): more:true grouped partial rejected envelope_invalid (P2-1 re-pin)', () => {
    const env = JSON.parse(JSON.stringify(E1_ENVELOPES[0]));
    (env.partial as Record<string, unknown>).more = true;
    mergeGroupedPartials(E1_PLAN, [env]);
  }, 'envelope_invalid');
  let moreMsg = '';
  try {
    const env = JSON.parse(JSON.stringify(E1_ENVELOPES[0]));
    (env.partial as Record<string, unknown>).more = true;
    mergeGroupedPartials(E1_PLAN, [env]);
  } catch (err) {
    moreMsg = (err as Error).message;
  }
  eqTrue('B3 (1): the P2-1 reject NAMES the sentinel (more=true) and the remedy', moreMsg.includes('more=true') && moreMsg.includes('not mergeable'));

  // (2) the §2.0 rowCount-consistency wall: rowCount must equal rows.length.
  throwsCode('B3 (2): rowCount 3 ≠ rows.length 2 rejected envelope_invalid', () => {
    const env = JSON.parse(JSON.stringify(E1_ENVELOPES[0]));
    (env.partial as Record<string, unknown>).rowCount = 3;
    mergeGroupedPartials(E1_PLAN, [env]);
  }, 'envelope_invalid');
  // the honest twin still merges — the walls are per-partial, never sticky
  eq('B3 (1)/(2): the honest twin (more:false, rowCount 2) still merges to the exact eu total', (() => {
    const fin = finalizeGroups(mergeGroupedPartials(E1_PLAN, [E1_ENVELOPES[0]]), E1_PLAN);
    return fin.map((g) => ({ k: g.k, aggs: g.aggs }));
  })(), [{ k: ['eu'], aggs: { s: 600n, c: 6 } }, { k: ['us'], aggs: { s: 300n, c: 3 } }]);

  // (3) avg pair fusion (E2 algebra re-pin): {s:600,c:6} + {s:400,c:4} fuse
  // ADDITIVELY to the exact rational 1000/10 — never the mean-of-means 55,
  // never a bare number; and the {s:null,c:0} identity contributes nothing.
  const avgPlan: WhMergePlan = { table: 't', aggs: { a: { op: 'avg', col: 'x', colPlan: { col: 'x', type: 'int8' } } } };
  const fused = finalizeScalarAggs(mergeScalarAggs(avgPlan, [
    scalarEnvelope('S1', 't', { a: { s: '600', c: 6 } }, avgPlan),
    scalarEnvelope('S2', 't', { a: { s: '400', c: 4 } }, avgPlan),
  ]), avgPlan);
  eq('B3 (3): avg fuses the same-col pair additively — 600/6 + 400/4 ⇒ the exact rational 1000/10', fused.a, { num: 1000n, den: 10n });
  const withIdentity = finalizeScalarAggs(mergeScalarAggs(avgPlan, [
    scalarEnvelope('S1', 't', { a: { s: null, c: 0 } }, avgPlan),
    scalarEnvelope('S2', 't', { a: { s: '400', c: 4 } }, avgPlan),
  ]), avgPlan);
  eq('B3 (3): the {s:null, c:0} identity is the avg zero — 400/4 unchanged', withIdentity.a, { num: 400n, den: 4n });
  const allEmpty = finalizeScalarAggs(mergeScalarAggs(avgPlan, [
    scalarEnvelope('S1', 't', { a: { s: null, c: 0 } }, avgPlan),
    scalarEnvelope('S2', 't', { a: { s: null, c: 0 } }, avgPlan),
  ]), avgPlan);
  eq('B3 (3): zero non-NULL inputs ⇒ NULL (the empty-input rule — never 0, never NaN)', allEmpty.a, null);
});

// -----------------------------------------------------------------------------
// Harness report (hand-rolled runner, no external deps).
// -----------------------------------------------------------------------------
Deno.test('__report__', () => {
  console.log(`\nwh_merge_test: ${passed} assertions passed, ${failed} failed`);
  if (failed > 0) throw new Error(`${failed} assertion(s) failed`);
});
