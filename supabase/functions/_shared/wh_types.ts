// =============================================================================
// _shared/wh_types.ts — warehouse MERGE-ALGEBRA canonical type system + the
// versioned partial-envelope wire contract (r38, QC1-QC2 read path).
// =============================================================================
// Normative source: research/findings_wh_scatter_gather.md §2.0 (framework +
// partial envelope + empty-input rule), §2.1 (merge table), §2.2 (rigor:
// SUM/COUNT, MIN/MAX NULL-as-identity, AVG pair algebra, GROUP BY canonical
// keys with row-arity + type validation), §2.4 (canonical type system).
//
// PURITY LAW (quota-sync.ts precedent): this module has ZERO imports — it is
// the base of the wh_* purity island and never touches supabase-client.ts,
// npm modules, network, Deno.env or fetch, so `deno test` on the wh_* tests
// runs offline with no permissions beyond --allow-env.
//
// WIRE CONTRACT (v1, pinned here):
//   * envelope.v must be EXACTLY 1. Unknown `v` is a hard envelope_invalid at
//     the consumption site — never a best-effort parse (r37 frozen-contract
//     law: version drift must fail loud, not silently mis-merge).
//   * PostgREST numeric rendering is JSON numbers by default and >2^53 gets
//     mangled by JS number parsing; the exact carrier is the text path
//     (strings). wh_canonical accepts BOTH number and string renderings and
//     rejects the lossy ones (§2.0 "Numeric rendering (r37-corrected)").
//   * AVG partials travel as the PAIR {s: sum, c: count} under the aggregate
//     name (§2.2: pair algebra — never the mean, E2).
//   * scalar partials carry exactly ONE row with an empty group key
//     (k: []) — a PostgREST scalar-aggregate query always yields one row,
//     even over an empty WHERE (sum=NULL, count=0, E14).
//   * 'shard_hll_mismatch' is a RESERVED code: pinned here so the code space
//     is stable, but thrown NEVER in v0 (COUNT DISTINCT / HLL is QC4-tier,
//     out of the v0 merge-algebra scope).
// =============================================================================

/** The four canonical column types (§2.4). Every cross-shard comparison and
 *  every group-key canonicalization consumes the per-column plan. */
export type CanonicalType = 'int8' | 'numeric' | 'text' | 'timestamptz';

/** Per-column plan: canonical type + (numeric only) the fixed-point scale
 *  pinned at plan time. `scale` counts fractional digits: a numeric(x,scale=2)
 *  column canonicalizes 123.45 → 12345n. REQUIRED for 'numeric' (wh_canonical
 *  rejects numeric plans without an integer scale in [0,18]); ignored otherwise. */
export interface WhColumnPlan {
  col: string;
  type: CanonicalType;
  scale?: number;
}

/** One declared aggregate in a partial envelope. `col` is present for
 *  column-scoped aggregates (sum/min/max/avg — and the explicit aliased
 *  count(col) the AVG compiler emits beside sum(col), P2-3) and absent for
 *  COUNT(*). */
export interface WhAggSpec {
  op: 'sum' | 'count' | 'min' | 'max' | 'avg';
  col?: string;
}

/** One partial row: group key tuple (ordered array — object-form keys are
 *  FORBIDDEN, rejected with arity_mismatch at the consumption site) + the
 *  per-aggregate partial values keyed by aggregate name. */
export interface WhPartialRow {
  k: unknown[];
  a: Record<string, unknown>;
}

/** The per-shard partial (§2.0 envelope, `partial` field).
 *
 *  kind='grouped': shard-local GROUP BY — rows[] carry k (length ===
 *  groupKeys.length, arity-checked per row) and per-group partial values.
 *  kind='scalar': bare aggregates — exactly one row with k: []. */
export interface WhPartial {
  kind: 'grouped' | 'scalar';
  groupKeys?: string[];
  aggs: Record<string, WhAggSpec>;
  rows: WhPartialRow[];
  rowCount: number;
  more: boolean;
}

/** The versioned per-shard response envelope (§2.0). v is pinned to the
 *  literal 1; runtime validation (wh_merge) rejects anything else. */
export interface WhPartialEnvelope {
  v: 1;
  shard: string;
  table: string;
  schema_version: number;
  partial: WhPartial;
}

/** Pinned error-code space for the merge algebra.
 *  - envelope_invalid: the wire object does not satisfy the v1 contract
 *    (unknown v, missing/mistyped fields, kind/plan mismatch, missing
 *    plan-referenced aggregates).
 *  - arity_mismatch: a partial row's group key is not an array (object-form
 *    keys are forbidden) or k.length !== plan.groupKeys.length; a scalar
 *    partial row whose k is not empty.
 *  - type_mismatch: a value fails canonicalization against the planned
 *    column type (e.g. text 'abc' in an int8 column), or an avg pair is
 *    inconsistent (sum-side null ⟺ count-side 0).
 *  - shard_hll_mismatch: RESERVED (QC4 HLL tier) — never thrown in v0. */
export type WhMergeErrorCode =
  | 'envelope_invalid'
  | 'arity_mismatch'
  | 'type_mismatch'
  | 'shard_hll_mismatch';

/** The only error type the merge layer throws for wire/plan-value problems.
 *  Programmer/configuration errors (malformed PLAN objects) throw plain
 *  TypeError instead — WhMergeError is reserved for contract violations. */
export class WhMergeError extends Error {
  readonly code: WhMergeErrorCode;

  constructor(code: WhMergeErrorCode, message: string) {
    super(message);
    this.name = 'WhMergeError';
    this.code = code;
  }
}
