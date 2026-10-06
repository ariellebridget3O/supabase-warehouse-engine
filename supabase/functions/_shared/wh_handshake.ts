// =============================================================================
// _shared/wh_handshake.ts — engine ⇄ shard DISCOVERY/VERIFICATION HANDSHAKE
// (r44, design_wh_query_rpc.md §5.2 + §6.3 F14/F2).
// =============================================================================
// Normative source: research/design_wh_query_rpc.md §5.2 (per-gather, once per
// candidate shard: GET the shard's wh_query_templates inventory; shard is
// wh_query-eligible iff (a) required hashes ⊆ returned set, (b) row
// schema_version matches the directory row's version; [F15] max_rows
// fail-fast BEFORE any wh_query call) + §6.3 (merge-op enumeration, the F14
// SUBSET rule, the F2 truncation gate) + findings_wh_catalog_contract.md §4.4
// (warning objects carry est_rows; template_missing/schema_mismatch enum).
//
// PURITY LAW (wh_* island): this module has ZERO imports — all I/O (the
// inventory fetch) and the engine→shard credentials are injected deps; no
// supabase client, no network, no fs, no Deno.env. Offline-testable with
// injected fakes.
//
// NO CROSS-CALL CACHING (design §5.2): the inventory is re-read per gather —
// isolate recycling makes a cross-call cache a disproven pattern, and the
// handshake doubles as the template-inventory audit (shard-vs-manifest drift
// = incident). This module holds NO module-level mutable state.
//
// FAIL-CLOSED LAW (consumption-site guard): every point the wire value is
// touched is guarded — a wrong-shape inventory body parses as inventory-empty
// ([]), which fails the shard closed (template_missing exclusion). The
// functions below NEVER throw on wire data.
// =============================================================================

// ---------- pinned wire shapes ----------

/** One row of the shard's template inventory (the §5.2 GET's select list). */
export interface TemplateInventoryRow {
  template_hash: string;
  qc_class: string;
  logical_table: string;
  schema_version: number;
  state: string;
  max_rows: number;
}

/** §5.2 warning codes (contract §4.4 enum extension; `est_rows` is null here
 *  because the handshake does not know the shard's estimate — the CALLER
 *  fills it from the directory's row_estimate lane when available, per §4.4
 *  "warning objects carry the shard's est_rows so the gather can weigh
 *  degrade-vs-abort"). */
export interface HandshakeWarning {
  code: 'template_missing' | 'schema_mismatch' | 'max_rows_exceeded';
  est_rows: number | null;
}

export interface HandshakeResult {
  eligible: boolean;
  warning: HandshakeWarning | null;
  /** r69 (AM-6/OQ-8): the required hash the shard's inventory matched — the
   *  FIRST required hash in plan order (deterministic). On an eligible
   *  verdict ALL required hashes matched (the loop returns ineligible at the
   *  first miss), so matchedHash ≡ the first derived hash — deterministic by
   *  plan order, inventory-independent. Spread-conditional: ABSENT on
   *  ineligible verdicts (unchanged shapes) and when the plan requires no
   *  hashes (eligible-with-zero-requirements). Consumed by the r69 §3.2 rpc
   *  target construction (p_template_hash). */
  matchedHash?: string;
}

/** The plan-side §5.2 inputs: the template hashes the plan requires (the
 *  wh_query RPC plane) and the plan's limit K. K=null means the plan
 *  declared no limit — the max_rows check is vacuous and the shard's
 *  sentinel still re-caps at execution (defense in depth, §2.1 step 5). */
export interface HandshakePlanRef {
  templateHashes: readonly string[];
  limitK: number | null;
}

// ---------- inventory fetch (injected; §4.6 engine→shard plane) ----------

/** Minimal structural fetch — the platform `fetch` satisfies it directly;
 *  tests inject recording fakes. ok/status are optional so a minimal fake
 *  may omit them (the shape law below does the gating). */
export type WhRawFetch = (
  url: string,
  init: { method: string; headers: Record<string, string> },
) => Promise<{ ok?: boolean; status?: number; text: () => Promise<string> }>;

export interface WhInventoryDeps {
  /** The injected fetch (prod wiring: the platform fetch; tests: fakes). */
  fetcher: WhRawFetch;
  /** engine→shard plane auth (contract §4.6): returns the SHARD's service
   *  key — apikey + Authorization Bearer both carry it. Injected: this
   *  module never holds secrets or reads env (purity law). */
  shardServiceKey: (shard: string) => string;
}

/** The handshake's call shape (deps-injected into the engine core; tests
 *  stub it directly). */
export interface WhShardHandshake {
  readTemplateInventory(shard: string): Promise<TemplateInventoryRow[]>;
}

/** Mirrors wh_engine_core.SHARD_REQUEST_HEADERS (kept local — wh_engine_core
 *  imports THIS module, so the purity island stays acyclic). */
const WH_SHARD_PLANE_PROFILE: Record<string, string> = { 'Accept-Profile': 'public' };

/** The pinned §5.2 inventory GET — URL mirrors compileShardUrl's base
 *  (`https://<shard-ref>.supabase.co/rest/v1/...`) and the select list +
 *  state filter are byte-pinned (design §5.2). */
export function templateInventoryUrl(shard: string): string {
  return `https://${shard}.supabase.co/rest/v1/wh_query_templates` +
    `?select=template_hash,qc_class,logical_table,schema_version,state,max_rows` +
    `&state=in.(active,frozen)`;
}

/** engine→shard plane auth headers (contract §4.6): apikey + Authorization
 *  Bearer both carry the shard service key. */
export function templateInventoryHeaders(serviceKey: string): Record<string, string> {
  return {
    ...WH_SHARD_PLANE_PROFILE,
    apikey: serviceKey,
    Authorization: `Bearer ${serviceKey}`,
  };
}

/** Consumption-site row guard: a row is trusted only when EVERY select-list
 *  field carries the pinned shape. STRICT whole-body law: one malformed row
 *  fails the ENTIRE body closed ([]) — the handshake doubles as the
 *  inventory audit (§5.2) and drift is an incident, not a partially-tolerated
 *  warning; a silent per-row salvage would launder a drifted inventory into
 *  an eligibility pass. */
export function parseInventoryRows(raw: unknown): TemplateInventoryRow[] {
  if (!Array.isArray(raw)) return [];
  const rows: TemplateInventoryRow[] = [];
  for (const item of raw) {
    if (item === null || typeof item !== 'object' || Array.isArray(item)) return [];
    const r = item as Record<string, unknown>;
    if (typeof r.template_hash !== 'string' || r.template_hash.length === 0) return [];
    if (typeof r.qc_class !== 'string' || r.qc_class.length === 0) return [];
    if (typeof r.logical_table !== 'string' || r.logical_table.length === 0) return [];
    if (typeof r.schema_version !== 'number' || !Number.isInteger(r.schema_version)) return [];
    if (typeof r.state !== 'string' || r.state.length === 0) return [];
    if (typeof r.max_rows !== 'number' || !Number.isInteger(r.max_rows) || r.max_rows < 0) return [];
    rows.push({
      template_hash: r.template_hash,
      qc_class: r.qc_class,
      logical_table: r.logical_table,
      schema_version: r.schema_version,
      state: r.state,
      max_rows: r.max_rows,
    });
  }
  return rows;
}

/**
 * §5.2 inventory read — per gather, once per candidate shard. NEVER throws
 * and NEVER caches: every failure mode (dep throw, non-2xx, non-JSON body,
 * wrong shape) parses as inventory-empty ([]), which the caller's
 * checkHandshake fails closed to a template_missing exclusion.
 */
export async function readTemplateInventory(
  deps: WhInventoryDeps,
  shard: string,
): Promise<TemplateInventoryRow[]> {
  let res: { ok?: boolean; status?: number; text: () => Promise<string> };
  try {
    res = await deps.fetcher(
      templateInventoryUrl(shard),
      { method: 'GET', headers: templateInventoryHeaders(deps.shardServiceKey(shard)) },
    );
  } catch {
    return []; // network/throw => fail-closed (§5.2: fail ⇒ excluded, never a 5xx)
  }
  if (res === null || typeof res !== 'object' || typeof res.text !== 'function') return [];
  if (res.ok === false) return []; // explicit non-2xx (PostgREST errors are objects anyway)
  let bodyText: string;
  try {
    bodyText = await res.text();
  } catch {
    return [];
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(bodyText);
  } catch {
    return [];
  }
  return parseInventoryRows(parsed);
}

/** Default handshake implementation: the real inventory fetch over the
 *  injected raw fetch + shard service key resolver (prod wiring in
 *  warehouse-engine/index.ts; tests stub the WhShardHandshake shape). */
export function makeWhHandshake(deps: WhInventoryDeps): WhShardHandshake {
  return {
    readTemplateInventory: (shard: string) => readTemplateInventory(deps, shard),
  };
}

// ---------- the §5.2 eligibility check ----------

/**
 * checkHandshake — design §5.2 exactly:
 *  (a) required hashes ⊆ returned set — a required template absent from the
 *      inventory OR not in state active|frozen ⇒ template_missing;
 *  (b) for each matched template row: row.schema_version must equal the
 *      directory's schema version for the table ⇒ else schema_mismatch
 *      (the directory's tableSchemaVersion is the ddl-wave truth; undefined
 *      directory version ⇒ check vacuous, the consumption-site envelope gate
 *      still applies);
 *  (c) max_rows fail-fast: the plan's limit K exceeding the matched row's
 *      max_rows ⇒ max_rows_exceeded. The refusal happens BEFORE any wh_query
 *      call — the shard's sentinel re-caps at execution = defense in depth
 *      (design §2.1 step 5 / erratum F15): this engine-side check refuses
 *      early, it does not replace the shard-side cap.
 *
 * First required hash to fail wins (deterministic, plan order). Never
 * throws on any input shape (consumption-site guard law).
 *
 * r69 (AM-6): an ELIGIBLE verdict additionally carries `matchedHash` — the
 * first required hash in plan order (spread-conditional; ineligible verdicts
 * and zero-requirement verdicts carry NO matchedHash, keeping every pre-r69
 * verdict shape byte-identical).
 */
export function checkHandshake(
  plan: HandshakePlanRef,
  inventoryRows: readonly TemplateInventoryRow[],
  tableSchemaVersion?: number | null,
): HandshakeResult {
  const required = plan !== null && typeof plan === 'object' && Array.isArray(plan.templateHashes)
    ? plan.templateHashes
    : [];
  for (const hash of required) {
    const row = inventoryRows.find((r) => r !== null && typeof r === 'object' && r.template_hash === hash);
    // (a) required hashes ⊆ returned set (absent or not active/frozen)
    if (row === undefined || (row.state !== 'active' && row.state !== 'frozen')) {
      return { eligible: false, warning: { code: 'template_missing', est_rows: null } };
    }
    // (b) ddl-wave version match against the directory row's version
    if (typeof tableSchemaVersion === 'number' && row.schema_version !== tableSchemaVersion) {
      return { eligible: false, warning: { code: 'schema_mismatch', est_rows: null } };
    }
    // (c) max_rows fail-fast BEFORE any wh_query call (shard sentinel re-caps
    // at execution = defense in depth, §2.1 step 5)
    if (plan !== null && typeof plan === 'object' && typeof plan.limitK === 'number' && row.max_rows < plan.limitK) {
      return { eligible: false, warning: { code: 'max_rows_exceeded', est_rows: null } };
    }
  }
  return { eligible: true, warning: null, ...(required.length > 0 ? { matchedHash: required[0] } : {}) };
}

// ---------- the engine-side manifest (pinned from db/shard-templates) ----------

/** One transcribed row of db/shard-templates/manifest.json (the engine's
 *  copy source — hand-transcribed EXACTLY; wh_handshake_test.ts pins
 *  deep-equality against the file). */
export interface EngineTemplateRow {
  slug: string;
  file: string;
  template_hash: string;
  logical_table: string;
  qc_class: string;
  kind: 'rows' | 'scalar';
  merge_ops: string[];
  group_keys: string[];
  params_schema: Record<string, string>;
  timeout_ms: number;
  max_rows: number;
  schema_version: number;
  state: string;
  aggs: Record<string, { op: string; col?: string }>;
  encoding: Record<string, string>;
  // r129 (design_r128_joinplans §2.3): the additive OPTIONAL join binding —
  // present ONLY on the join-class rows (W6_colocated_join_agg base + the
  // r133 W7_dim_tier_join_agg tier2 twin): the dim relation + the hardcoded
  // SQL join keys the template bodies execute.
  // DERIVATION PARTITION KEY: deriveTemplateHashes derives a row for a plan
  // iff (row carries join) === (plan is a join plan) AND — r133
  // (design_r132_w7_family §2.2) — for join plans, row.join.variant ===
  // request join.variant (absent↔absent W6, 'tier2'↔'tier2' W7). W6/W7
  // merge_ops are IDENTICAL, so the VARIANT (not merge-op disjointness) is
  // what keeps the ≤1-template-per-opset law true and keeps W6/W7 from ever
  // co-deriving. The KEY BINDING check (wh_engine_core plan time,
  // audit-B AB-P2) compares the request's on.{left,right} against
  // join.{left,right} here — fail-closed when the row carries no binding —
  // and the r133 DIM BINDING gate (same plan block) compares request
  // join.table against join.dim. Additive optional: every pre-r129 row
  // literal stays unchanged.
  join?: { dim: string; left: string; right: string; variant?: 'tier2' };
}

/**
 * ENGINE_TEMPLATE_MANIFEST — byte-pinned transcription of
 * db/shard-templates/manifest.json @ FM 1e1c725 (r43 seed wave; template
 * hashes = sha256 of the §2.3 byte law over the exact body block). A plan
 * referencing a hash outside this manifest is a plan-time 4xx (plan
 * honesty, design §6.3); a partial claiming such a hash is rejected at the
 * consumption site (F14).
 * r129 (design_r128_joinplans §2.3): W6_colocated_join_agg APPENDED at
 * index 5 (append-only — the geo-plane cells address manifest rows BY
 * INDEX 0-4, so any earlier insert would shift every one of them RED) with
 * the additive `join` binding {dim:'wh_probe_dim', left:'region',
 * right:'region'} (engine-side manifest only — the shard-side registry
 * stores hash+kind+body and needs NO new column). The row deep-equals the
 * statics leg's db/shard-templates/manifest.json entry (qc_class QC6
 * rides the lint QC_CLASSES extension). timeout_ms <as W1> = 8000.
 * r133 (design_r132_w7_family §2.2): W7_dim_tier_join_agg APPENDED at
 * index 6 (append-only law — the index-addressed pins address 0-5) with
 * the SAME join binding + the additive `variant:'tier2'` discriminator
 * (engine-side manifest only). aggs/encoding/merge_ops IDENTICAL to W6 —
 * the col-strict adapter sees the SAME (op,col) tuples; the co-derivation
 * split rides the variant partition. timeout_ms <as W1> = 8000.
 * r144 (agent-ctx/r140-w7-census.md §6 — the W8 spec): W8_dim_tier_join_avg
 * APPENDED at index 7 (append-only law — the index-addressed pins address
 * 0-6): the FIRST GROUPED AVG template — W7's body with the ONE delta byte
 * `x`→`s` on the row_json wire key (482 bytes LF-only no-trailing-NL),
 * same join binding + variant:'tier2'. aggs/encoding ride the W3
 * avg-pair convention (s/c wire keys; c = count_col — the plan-side
 * count≡count_col equivalence in aggEncodingMatches maps plan
 * count(amount) onto it), merge_ops gain `avg_pair` and DROP `sum` — the
 * op-set now partitions WITHIN the tier2 variant (a tier2 SUM plan
 * derives ONLY W7, a tier2 AVG plan derives ONLY W8; W7/W8 can never
 * co-derive). Template-only growth: zero behavioral engine-code change.
 * timeout_ms <as W1> = 8000.
 */
export const ENGINE_TEMPLATE_MANIFEST: readonly EngineTemplateRow[] = [
  {
    slug: 'W1_grouped_sum_count',
    file: 'W1_grouped_sum_count.sql',
    template_hash: 'a934e7e062f59cff5a856afdc7aa743ec9be11c068c7e861ea856c36b40bdbfd',
    logical_table: 'wh_probe_agg',
    qc_class: 'QC2',
    kind: 'rows',
    merge_ops: ['groupby', 'sum', 'count'],
    group_keys: ['region'],
    params_schema: { id_min: 'int8', id_max: 'int8' },
    timeout_ms: 8000,
    max_rows: 1000,
    schema_version: 1,
    state: 'active',
    aggs: { x: { op: 'sum', col: 'amount' }, c: { op: 'count' } },
    encoding: { x: 'text', c: 'number' },
  },
  {
    slug: 'W2_scalar_minmax',
    file: 'W2_scalar_minmax.sql',
    template_hash: 'a095adaa148253aee8d1cc8e976f01b3579beeea5082f3862df4a908c20b2659',
    logical_table: 'wh_probe_agg',
    qc_class: 'QC2',
    kind: 'scalar',
    merge_ops: ['min', 'max', 'count_col'],
    group_keys: [],
    params_schema: { id_min: 'int8', id_max: 'int8' },
    timeout_ms: 8000,
    max_rows: 1000,
    schema_version: 1,
    state: 'active',
    aggs: { min: { op: 'min', col: 'amount' }, max: { op: 'max', col: 'amount' }, c: { op: 'count_col', col: 'amount' } },
    encoding: { min: 'text', max: 'text', c: 'number' },
  },
  {
    slug: 'W3_scalar_avg_pair',
    file: 'W3_scalar_avg_pair.sql',
    template_hash: 'bca9dd2c591ed48a0fa5367179dd5deb1d752ed9141c23e6ad53083becf8ecac',
    logical_table: 'wh_probe_agg',
    qc_class: 'QC2',
    kind: 'scalar',
    merge_ops: ['avg_pair'],
    group_keys: [],
    params_schema: { id_min: 'int8', id_max: 'int8' },
    timeout_ms: 8000,
    max_rows: 1000,
    schema_version: 1,
    state: 'active',
    aggs: { s: { op: 'sum', col: 'amount' }, c: { op: 'count_col', col: 'amount' } },
    encoding: { s: 'text', c: 'number' },
  },
  {
    slug: 'W4_topk',
    file: 'W4_topk.sql',
    template_hash: 'dccfc317035961a6310c31194e86a01c715f384ac8cbdc33fb00f35f27eefcee',
    logical_table: 'wh_probe_agg',
    qc_class: 'QC3',
    kind: 'rows',
    merge_ops: ['topk', 'raw_rows'],
    group_keys: [],
    params_schema: {},
    timeout_ms: 8000,
    max_rows: 1000,
    schema_version: 1,
    state: 'active',
    aggs: {},
    encoding: {},
  },
  {
    slug: 'W5_cold_agg',
    file: 'W5_cold_agg.sql',
    template_hash: '9a8e922d064539e3418fd77dbc17479fe6a9b69bafe0f182128c675ef1f95234',
    logical_table: 'facts_blocks',
    qc_class: 'COLD_AGG',
    kind: 'scalar',
    merge_ops: ['sum', 'count_col'],
    group_keys: [],
    params_schema: { dataset: 'text', day_from: 'date', day_to: 'date' },
    timeout_ms: 8000,
    max_rows: 1000,
    schema_version: 1,
    state: 'active',
    aggs: { s: { op: 'sum', col: 'value' }, c: { op: 'count_col', col: 'value' } },
    encoding: { s: 'text', c: 'number' },
  },
  // r129 (design_r128_joinplans §2.3): the join class — APPENDED at index 5
  // (append-only law; the index-addressed geo-plane pins address 0-4).
  // template_hash = sha256 of db/shard-templates/W6_colocated_join_agg.sql
  // (the body IS the contract); the `join` binding is the derivation
  // partition key + the key-binding check's manifest side.
  {
    slug: 'W6_colocated_join_agg',
    file: 'W6_colocated_join_agg.sql',
    template_hash: '7004f44de62a8e998ce1915348be0f0fc299ac1aae901966ae7080f7c2cc9576',
    logical_table: 'wh_probe_agg',
    qc_class: 'QC6',
    kind: 'rows',
    merge_ops: ['groupby', 'sum', 'count', 'count_col'],
    group_keys: ['region'],
    params_schema: {},
    timeout_ms: 8000,
    max_rows: 1000,
    schema_version: 1,
    state: 'active',
    aggs: { x: { op: 'sum', col: 'amount' }, c: { op: 'count', col: 'amount' }, n: { op: 'count' } },
    encoding: { x: 'text', c: 'number', n: 'number' },
    join: { dim: 'wh_probe_dim', left: 'region', right: 'region' },
  },
  // r133 (design_r132_w7_family §2.2): the tier2 join class — APPENDED at
  // index 6 (append-only law; the index-addressed pins address 0-5).
  // template_hash = sha256 of db/shard-templates/W7_dim_tier_join_agg.sql
  // (the body IS the contract — W6's body + the ONE delta line
  // `and d.tier = 2` on the ON clause, 482 bytes LF-only no-trailing-NL);
  // the `join` binding carries the additive variant:'tier2' discriminator —
  // the derivation partition key that keeps W6/W7 from ever co-deriving.
  {
    slug: 'W7_dim_tier_join_agg',
    file: 'W7_dim_tier_join_agg.sql',
    template_hash: 'e6d40cbe1d5da2587492c07076b97ec1e716deaf5cefa2b3098038bee98b79bb',
    logical_table: 'wh_probe_agg',
    qc_class: 'QC6',
    kind: 'rows',
    merge_ops: ['groupby', 'sum', 'count', 'count_col'],
    group_keys: ['region'],
    params_schema: {},
    timeout_ms: 8000,
    max_rows: 1000,
    schema_version: 1,
    state: 'active',
    aggs: { x: { op: 'sum', col: 'amount' }, c: { op: 'count', col: 'amount' }, n: { op: 'count' } },
    encoding: { x: 'text', c: 'number', n: 'number' },
    join: { dim: 'wh_probe_dim', left: 'region', right: 'region', variant: 'tier2' },
  },
  // r144 (agent-ctx/r140-w7-census.md §6): the tier2 GROUPED-AVG join class —
  // APPENDED at index 7 (append-only law; the index-addressed pins address
  // 0-6). template_hash = sha256 of db/shard-templates/W8_dim_tier_join_avg.sql
  // (the body IS the contract — W7's body with the ONE delta BYTE `x`→`s` on
  // the row_json wire key, 482 bytes LF-only no-trailing-NL); the `join`
  // binding carries the SAME variant:'tier2' discriminator — W7/W8 co-
  // derivation is now barred TWICE over (variant partition + the op-set:
  // W8 lacks `sum`, W7 lacks `avg_pair`). aggs/encoding ride the W3
  // avg-pair s/c convention (c = count_col — the count≡count_col
  // equivalence maps plan count(amount) onto it).
  {
    slug: 'W8_dim_tier_join_avg',
    file: 'W8_dim_tier_join_avg.sql',
    template_hash: 'bed23e35a457c534824e634e3f863742415d8077db36828bca2f630131968e86',
    logical_table: 'wh_probe_agg',
    qc_class: 'QC6',
    kind: 'rows',
    merge_ops: ['groupby', 'avg_pair', 'count', 'count_col'],
    group_keys: ['region'],
    params_schema: {},
    timeout_ms: 8000,
    max_rows: 1000,
    schema_version: 1,
    state: 'active',
    aggs: { s: { op: 'sum', col: 'amount' }, c: { op: 'count_col', col: 'amount' }, n: { op: 'count' } },
    encoding: { s: 'text', c: 'number', n: 'number' },
    join: { dim: 'wh_probe_dim', left: 'region', right: 'region', variant: 'tier2' },
  },
];

/** Manifest resolution by template hash (first match; the registry PK is the
 *  hash). Unknown/mistyped hash ⇒ null (the caller fails closed). */
export function manifestRowByHash(templateHash: string): EngineTemplateRow | null {
  if (typeof templateHash !== 'string') return null;
  for (const row of ENGINE_TEMPLATE_MANIFEST) {
    if (row.template_hash === templateHash) return row;
  }
  return null;
}

// ---------- plan-honesty + F14 subset algebra ----------

/**
 * Plan-honesty pre-check (design §6.3, plan-honesty §1 axis-5): returns the
 * plan-referenced hashes that do NOT resolve in the engine manifest. The
 * caller (wh_engine_core, at plan time) turns a non-empty result into a 4xx
 * malformed — a plan referencing an unmanifested template can never be
 * executed honestly, so it must die before any network I/O. (Lives as a pure
 * predicate here rather than a throwing assert: this module must not import
 * wh_engine_core — wh_engine_core imports THIS module.)
 */
export function planTemplatesMissingInManifest(hashes: readonly string[] | undefined): string[] {
  if (!Array.isArray(hashes)) return [];
  return hashes.filter((h) => manifestRowByHash(h) === null);
}

/** Structural view of the merge plan (WhMergePlan satisfies it) so this
 *  module needs no import from wh_merge. */
export interface HandshakePlanOpsView {
  groupKeys?: readonly unknown[];
  aggs: Record<string, { op: string; col?: string }>;
}

/**
 * Derive the plan's declared merge-op TOKENS (§6.3 enumeration) from the
 * merge plan: a grouped plan requires the `groupby` token; count(*) is
 * `count`, count(col) is `count_col` (NULL-blind partial), avg travels as
 * the `avg_pair` pair algebra (never the mean); sum/min/max pass through.
 */
export function derivePlanMergeOps(plan: HandshakePlanOpsView): string[] {
  const ops = new Set<string>();
  if (plan !== null && typeof plan === 'object' && Array.isArray(plan.groupKeys) && plan.groupKeys.length > 0) {
    ops.add('groupby');
  }
  const aggs = plan !== null && typeof plan === 'object' && plan.aggs !== null && typeof plan.aggs === 'object'
    ? plan.aggs
    : {};
  for (const pa of Object.values(aggs)) {
    if (pa === null || typeof pa !== 'object' || typeof pa.op !== 'string') continue;
    if (pa.op === 'count') ops.add(pa.col === undefined ? 'count' : 'count_col');
    else if (pa.op === 'avg') ops.add('avg_pair');
    else ops.add(pa.op);
  }
  return [...ops];
}

/**
 * [r42 errata F14] SUBSET rule: plan_ops ⊆ template.merge_ops — SUBSET, not
 * equality. v1's exact-equality mis-pin would falsely reject a topk-only
 * plan against W4's declared ["topk","raw_rows"]; this predicate is the
 * regression pin for that law.
 */
export function planOpsSubset(planOps: readonly string[], templateMergeOps: readonly string[]): boolean {
  if (!Array.isArray(planOps) || !Array.isArray(templateMergeOps)) return false;
  return planOps.every((op) => templateMergeOps.includes(op));
}

// ---------- r47 templateHashes request-plane derivation (wh-contract r47 errata) ----------

/** Minimal plan view deriveTemplateHashes needs (the same structural shape
 *  the handshake + F14 use — no import from wh_merge/wh_engine_core, which
 *  import THIS module). */
export interface DerivePlanView {
  table: string;
  groupKeys?: readonly unknown[];
  aggs: Record<string, { op: string; col?: string }>;
  /** r129 (design_r128_joinplans §2.3): the request's query.join descriptor
   *  (parsed shape {table,type,on,variant?}) when the plan is a join plan —
   *  a join plan derives ONLY manifest rows carrying the additive join
   *  binding, a non-join plan EXCLUDES them (the join-class partition).
   *  Typed `unknown` deliberately: the descriptor's own shape is parse-
   *  space, and derivation reads EXACTLY ONE field of it — r133
   *  (design_r132_w7_family §2.2) the `variant` discriminator (absent ↔
   *  absent, 'tier2' ↔ 'tier2'), which partitioned the join class once W6
   *  and W7 (identical merge_ops) coexisted. Every other field stays
   *  parse-space (key + dim binding are the plan block's job in
   *  wh_engine_core, against the MANIFEST side). */
  join?: unknown;
}

/**
 * ENGINE-DERIVED plan template hashes (wh-contract r47 errata): the
 * ENGINE_TEMPLATE_MANIFEST rows that could serve this plan —
 *   * logical_table === plan.table,
 *   * plan_ops ⊆ row.merge_ops (the F14 SUBSET rule, derivePlanMergeOps +
 *     planOpsSubset),
 *   * row.schema_version === tableSchemaVersion (VACUOUS when the directory
 *     version is unknown — undefined disables the filter),
 *   * state = 'active',
 *   * r129 JOIN-CLASS PARTITION (design_r128_joinplans §2.3, audit A A2):
 *     (row carries the manifest `join` binding) === (plan is a join plan,
 *     DerivePlanView.join present). Non-join plans EXCLUDE join-class rows
 *     and join plans derive ONLY them — otherwise W6 (merge_ops ⊇ W1's)
 *     would co-derive on every plain W1 plan (REDing the pinned
 *     ≤1-template census) and a join request lacking a count-col would
 *     derive W1+W6 with derived[0]=W1, 400ing a LEGAL join request.
 *   * r133 VARIANT PARTITION (design_r132_w7_family §2.2): WITHIN the join
 *     class, a plan derives ONLY rows whose manifest join.variant EQUALS
 *     the request's (absent↔absent = W6, 'tier2'↔'tier2' = W7). W6/W7
 *     merge_ops are IDENTICAL, so the variant — NOT merge-op disjointness —
 *     is the sole co-derivation discriminator (two join-class rows with
 *     the same op-set can never both derive; derived[0] stays
 *     deterministic). Zero matches after the partition → an empty
 *     derivation → the D2-owned plan_untemplated outcome (the dim binding
 *     is NOT a derivation input — the plan gate solely owns it).
 * The set is manifest-bounded BY CONSTRUCTION (plan-honesty can never
 * self-reject); the SAME set feeds plan-honesty, the §5.2 handshake, F14,
 * and "non-empty ⟺ the query could ride the wh_query RPC branch".
 *
 * v0 NARROWING (documented): the errata's params_schema-satisfiability
 * clause is EXECUTOR-TIME here (param compilation is design §6.1 executor
 * business — its failure is plan-time, not derivation-time); no request
 * field selects the RPC branch in v0, so "RPC-branch plan + EMPTY
 * derivation ⇒ plan-time 400" is forward-pinned and unreachable. Never
 * client-supplied: no request field exists or may be added (a client-
 * declared set is a lying-client degrade knob).
 */
export function deriveTemplateHashes(
  plan: DerivePlanView,
  tableSchemaVersion: number | undefined,
): string[] {
  if (plan === null || typeof plan !== 'object') return [];
  const planOps = derivePlanMergeOps(plan);
  // KIND MATCHING (r47 battery catch): the F14 SUBSET rule alone OVER-MATCHES
  // across kinds — a scalar sum/count plan ({sum,count}) subset-matches W1's
  // [groupby,sum,count] and would route a select-path query to a GROUPED RPC
  // template. The plan's shape (groupKeys present?) must equal the row's kind.
  const planKind = Array.isArray(plan.groupKeys) && plan.groupKeys.length > 0 ? 'rows' : 'scalar';
  // r129 JOIN-CLASS PARTITION (design_r128_joinplans §2.3): presence-only —
  // see the DerivePlanView.join doc + the header bullet above.
  const planIsJoin = plan.join !== undefined;
  // r133 VARIANT PARTITION (design_r132_w7_family §2.2): the ONE descriptor
  // field derivation reads — 'tier2' when the parsed request carried it,
  // undefined otherwise (absent ↔ absent). Any non-tier2 value never
  // reaches here (parse rejects it); garbage unit views normalize to
  // undefined (the W6 class), never to a match against W7.
  const planVariant = planIsJoin && plan.join !== null && typeof plan.join === 'object' &&
      (plan.join as { variant?: unknown }).variant === 'tier2'
    ? 'tier2'
    : undefined;
  const out: string[] = [];
  for (const row of ENGINE_TEMPLATE_MANIFEST) {
    if (row.state !== 'active') continue;
    if (row.logical_table !== plan.table) continue;
    if (row.kind !== planKind) continue;
    if (planIsJoin !== (row.join !== undefined)) continue;
    // r133: within the join class, the VARIANT decides — W6 (variant
    // absent) serves absent-variant plans only, W7 ('tier2') serves tier2
    // plans only; W6/W7 (identical merge_ops) can never co-derive.
    if (planIsJoin && (row.join?.variant ?? undefined) !== planVariant) continue;
    if (!planOpsSubset(planOps, row.merge_ops)) continue;
    if (tableSchemaVersion !== undefined && row.schema_version !== tableSchemaVersion) continue;
    out.push(row.template_hash);
  }
  return out;
}
