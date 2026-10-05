// =============================================================================
// _shared/wh_directory_reader.ts — directory reader (atomic RPC read + version probe)
// =============================================================================
// r40 (wiring review maxxing-r40-wiring-review): extracted from the r39
// warehouse-engine/index.ts readDirectory() so the row mapping, version
// coercion and the impure fetch edges are offline-testable with injected
// low-level fetchers (deploy.ts pattern: the entrypoint wires real
// supabase-js calls, the factory owns ALL the logic).
//
// r120 (OPT-1 atomic directory read): the fresh-path directory chain is ONE
// atomic jsonb payload from the single-statement language-sql RPC
// wh_directory_atomic_read (fm db/migrations/0026) — version + rows arrive
// consistent under ONE Postgres snapshot BY CONSTRUCTION. This SUPERSEDES
// the r40 pagination loop + P2-4 version double-probe (the pair only
// bounded the mixed-read flap window; the one-snapshot RPC makes it
// structural). probeDirectoryVersion (config GET) is KEPT UNCHANGED: the
// replay path + /health still need the standalone probe.
//
// Review fixes carried here:
//   * P0-2: the view exposes `pr.ref as project_ref` — there is NO `shard`
//     column. Rows are mapped through toWhDirectoryRow() (project_ref -> shard)
//     with loud failures on missing/mistyped required fields (view drift is an
//     internal error, never a silently-undefined shard). STILL APPLIES r120:
//     the atomic payload's rows ride the same mapping.
//   * P2-4 (SUPERSEDED r120): the directory version was probed BEFORE and
//     AFTER the row read; a change between probes retried the whole read ONCE
//     (concurrent catalog mutation otherwise stamped mixed rows with a wrong
//     version, §4.6). History: the single-statement RPC's one-snapshot body
//     removes the flap window the pair bounded — the double-probe is retired.
//   * P2-5: the config value is accepted only when `typeof === 'number'` —
//     Number(null)/Number(false)/Number('') all coerce to 0 and defeat the
//     guard (r38 SCALAR-encoding law protects the writer; this probe
//     re-validates the reader). STILL APPLIES r120: the RPC passes the RAW
//     jsonb scalar through (F1, no SQL cast) precisely so this law
//     re-validates it.
//   * P0-3 design note: the read is a FULL embed (all tables) because §4.6
//     snapshot mode attaches the full directory for the client to cache; the
//     per-table projection for query execution happens in the entrypoint
//     (one read serves both needs).
//
// F3 law (every arm of every reader here, incl. readDirectoryAtomic): throw
// PLAIN Errors ONLY — never WhEngineError. Raw DB/PostgREST text must never
// reach the wire: the entrypoint's WhEngineError branch relays e.message
// raw, whereas plain Errors hit the P3-4 scrub.
// =============================================================================

import type { WhDirectoryRow, WhGeoDirectoryRow } from './wh_engine_core.ts';

export const DIRECTORY_PAGE = 1000;

/** Low-level config probe for warehouse_directory_version (index.ts wires supabase-js). */
export type DirectoryVersionFetcher = () => Promise<{
  value: unknown;
  error: { message: string } | null;
} | null>;

/** r120 OPT-1: low-level atomic {version, rows} fetch (index.ts wires
 *  supabase-js client.rpc('wh_directory_atomic_read') — ONE POST; the
 *  single-statement language-sql RPC returns the whole payload under one
 *  Postgres snapshot). */
export type DirectoryAtomicFetcher = () => Promise<{ data: unknown; error: { message: string } | null }>;

/**
 * r120 F2: the loud fleet-shape bound on the directory embed — the successor
 * to the retired count:'exact' truncation guard. The atomic RPC cannot
 * over-return against a count (there is no pagination anymore), so the bound
 * is the fleet shape itself: a fetch above DIRECTORY_PAGE (1000) rows is a
 * law breakage thrown LOUDLY by readDirectoryAtomic — never silently
 * truncated, never a silent LIMIT inside the RPC.
 */
export const DIRECTORY_EMBED_BOUND = DIRECTORY_PAGE;

export interface DirectoryReader {
  probeDirectoryVersion(): Promise<number>;
  readDirectory(): Promise<{ rows: WhDirectoryRow[]; version: number }>;
}

function isPlainObject(v: unknown): v is Record<string, unknown> {
  return v !== null && typeof v === 'object' && !Array.isArray(v);
}

/**
 * r40 P2-5: the config row's jsonb SCALAR value is trusted only when it is
 * already a JS number (supabase-js parses jsonb scalars; ints arrive as
 * numbers). Number(null)/Number(false)/Number('') === 0 would forge version 0.
 */
export function coerceDirectoryVersion(raw: unknown): number {
  if (typeof raw !== 'number' || !Number.isInteger(raw) || raw < 0) {
    throw new Error(`directory version is not a non-negative integer: ${JSON.stringify(raw) ?? 'undefined'}`);
  }
  return raw;
}

/**
 * r40 P0-2: map one raw v_warehouse_directory row to WhDirectoryRow.
 * The view column is `project_ref` (pr.ref); the engine's row field is
 * `shard`. Missing required fields throw (internal) — a drifted view must
 * fail loudly, not produce `https://undefined.supabase.co` targets.
 */
export function toWhDirectoryRow(raw: unknown): WhDirectoryRow {
  if (!isPlainObject(raw)) throw new Error('directory row is not an object');
  const shard = raw.project_ref ?? raw.shard;
  if (typeof shard !== 'string' || shard.length === 0) {
    throw new Error(`directory row missing project_ref: ${JSON.stringify(raw).slice(0, 200)}`);
  }
  const schemaVersion = raw.schema_version;
  if (typeof schemaVersion !== 'number' || !Number.isInteger(schemaVersion)) {
    throw new Error(`directory row schema_version is not an integer: ${JSON.stringify(raw).slice(0, 200)}`);
  }
  const row: WhDirectoryRow = {
    shard,
    key_min: raw.key_min === null || raw.key_min === undefined ? null : String(raw.key_min),
    key_max: raw.key_max === null || raw.key_max === undefined ? null : String(raw.key_max),
    hash_slot: raw.hash_slot === null || raw.hash_slot === undefined ? null : Number(raw.hash_slot),
    state: String(raw.state ?? ''),
    platform_status: String(raw.platform_status ?? ''),
    schema_version: schemaVersion,
    last_health_at: String(raw.last_health_at ?? ''),
  };
  if (raw.hash_slot !== null && raw.hash_slot !== undefined && !Number.isInteger(row.hash_slot)) {
    throw new Error(`directory row hash_slot is not an integer: ${JSON.stringify(raw).slice(0, 200)}`);
  }
  // passthrough metadata (erratum §4.3): present in the view since 0013.
  if (typeof raw.logical_name === 'string') row.logical_name = raw.logical_name;
  if (raw.shard_key_type === 'none' || raw.shard_key_type === 'hash' || raw.shard_key_type === 'range' || raw.shard_key_type === 'time') {
    row.shard_key_type = raw.shard_key_type;
  }
  if (raw.shard_key_column === null || typeof raw.shard_key_column === 'string') row.shard_key_column = raw.shard_key_column;
  if (typeof raw.table_schema_version === 'number' && Number.isInteger(raw.table_schema_version)) {
    row.table_schema_version = raw.table_schema_version;
  }
  // r44 (§4.4 est_rows lane): the directory's row_estimate (0013:119/155)
  // rides the passthrough — the §5.2 handshake warnings carry it so the
  // gather can weigh degrade-vs-abort. Advisory: absent/mistyped => omitted
  // (the engine treats it as "no estimate known", never as 0 rows).
  if (typeof raw.row_estimate === 'number' && Number.isInteger(raw.row_estimate) && raw.row_estimate >= 0) {
    row.row_estimate = raw.row_estimate;
  }
  // r129 (design_r128_joinplans §2.2, audit A A3): the dim-relation
  // reference flag (0013:110 warehouse_tables / :144 warehouse_placements —
  // writer-stamped on the placement, never propagated from the table row)
  // rides the passthrough — the join colocation gate consumes it. Mirrors
  // the row_estimate arm's advisory style, mapped FAIL-CLOSED: boolean
  // true (or the text 'true' — PostgREST can deliver booleans as text
  // through view/jsonb paths) => true; boolean/'text' false => false;
  // ANYTHING else (absent/mistyped) => the field is OMITTED — the gate
  // reads is_reference !== true as NOT a reference placement (undefined
  // ≠ true, never guessed into colocatability).
  if (raw.is_reference === true || raw.is_reference === 'true') {
    row.is_reference = true;
  } else if (raw.is_reference === false || raw.is_reference === 'false') {
    row.is_reference = false;
  }
  return row;
}

/**
 * r120 OPT-1: atomic directory read — ONE jsonb payload {version, rows} from
 * the single-statement language-sql RPC wh_directory_atomic_read (fm
 * db/migrations/0026_wh_directory_atomic_read.sql). Module-level (NOT inside
 * the factory) mirroring coerceDirectoryVersion/toWhDirectoryRow: the
 * mapping+coercion logic stays offline-testable against a fake fetcher.
 *
 * F3 LAW: throws PLAIN Errors ONLY — never WhEngineError. Raw DB/PostgREST
 * text must never reach the wire: the entrypoint's WhEngineError branch
 * relays e.message raw, whereas plain Errors hit the P3-4 scrub.
 *
 * Snapshot semantics: version + rows arrive under ONE Postgres snapshot by
 * construction (language-sql single-statement body under READ COMMITTED) —
 * superseding the P2-4 before/after double-probe, which only bounded the
 * mixed-read flap window (marked superseded r120).
 *
 * F1: 'version' arrives as the RAW jsonb scalar from config (the RPC does
 * NOT cast) so the P2-5 coercion law re-validates it here:
 * coerceDirectoryVersion judges typeof number / integer / >= 0. A missing
 * config row surfaces as jsonb null => the explicit F7 arm below throws
 * loudly (never coerced to 0).
 *
 * F2: rows above DIRECTORY_EMBED_BOUND throw — the loud fleet-shape bound
 * successor to the retired truncation guard. Row mapping rides
 * toWhDirectoryRow — loud on view drift (P0-2 class).
 */
export async function readDirectoryAtomic(
  deps: { fetchAtomic: DirectoryAtomicFetcher },
): Promise<{ rows: WhDirectoryRow[]; version: number }> {
  const { data, error } = await deps.fetchAtomic();
  if (error) throw new Error(`atomic directory read failed: ${error.message}`);
  if (!isPlainObject(data)) {
    throw new Error(`atomic directory read returned a non-object payload: ${JSON.stringify(data)?.slice(0, 200)}`);
  }
  if (data.version === null) throw new Error('directory version probe failed: config row missing'); // F7
  const version = coerceDirectoryVersion(data.version); // F1: raw scalar judged by the SAME law as today
  if (!Array.isArray(data.rows)) {
    throw new Error(`atomic directory read returned non-array rows: ${JSON.stringify(data.rows)?.slice(0, 200)}`);
  }
  if (data.rows.length > DIRECTORY_EMBED_BOUND) {
    // F2: loud fleet-shape bound — the retired truncation guard's successor.
    throw new Error(`directory embed fetched ${data.rows.length} rows > bound ${DIRECTORY_EMBED_BOUND} — fleet-shape law breakage`);
  }
  return { version, rows: data.rows.map(toWhDirectoryRow) }; // loud on row drift (P0-2 class)
}

/**
 * Build the directory reader. fetchVersion/fetchAtomic are the ONLY impure
 * edges (injected by index.ts with real supabase-js calls; tests inject
 * call-counting fakes).
 *
 * r120 OPT-1: readDirectory delegates to readDirectoryAtomic — ONE rpc POST,
 * version+rows under one snapshot. The r40 signature took a paginated row
 * fetcher plus the version probe, and readDirectory ran the pagination loop
 * plus the P2-4 double-probe; the pagination fetcher is retired (replaced by
 * fetchAtomic) and the returned shape
 * { probeDirectoryVersion, readDirectory } is UNCHANGED for every caller.
 */
export function makeDirectoryReader(deps: {
  fetchVersion: DirectoryVersionFetcher;
  fetchAtomic: DirectoryAtomicFetcher;
}): DirectoryReader {
  // UNCHANGED r40 logic (the replay path + /health depend on the standalone probe).
  async function probeDirectoryVersion(): Promise<number> {
    const row = await deps.fetchVersion();
    if (row === null) throw new Error('directory version probe failed: config row missing');
    if (row.error) throw new Error(`directory version probe failed: ${row.error.message}`);
    return coerceDirectoryVersion(row.value);
  }

  // P2-4 HISTORY (superseded r120): readDirectory used to probe the version
  // BEFORE and AFTER a paginated row read and retry ONCE on a flap — the
  // probe pair only BOUNDED the mixed-read window under concurrent catalog
  // mutation (a second flap refused to stamp a mixed read). The
  // single-statement language-sql wh_directory_atomic_read RPC returns
  // version+rows under ONE Postgres snapshot (READ COMMITTED: one statement
  // = one snapshot), so consistency is structural: the double-probe AND the
  // paginated row loop are retired.
  const readDirectory = () => readDirectoryAtomic({ fetchAtomic: deps.fetchAtomic });

  return { probeDirectoryVersion, readDirectory };
}

// =============================================================================
// r47 geo read-plane reader (wh-contract r47 errata; impl plan §2)
// =============================================================================

/** Low-level v_geo_directory fetch (index.ts wires supabase-js; fixed limit(2). */
export type GeoDirectoryFetcher = () => Promise<{
  data: unknown[] | null;
  error: { message: string } | null;
}>;

/** Map one raw v_geo_directory row to WhGeoDirectoryRow. Loud on missing
 *  required scalars (a drifted view must fail loudly); coverage/lag_bytes
 *  pass through UNVALIDATED (the engine's gates own the shapes — the r47
 *  coverage law treats any unexpected jsonb shape as fail-closed ∅). */
export function toWhGeoDirectoryRow(raw: unknown): WhGeoDirectoryRow {
  if (!isPlainObject(raw)) throw new Error('geo directory row is not an object');
  if (typeof raw.project_ref !== 'string' || raw.project_ref.length === 0) {
    throw new Error(`geo directory row missing project_ref: ${JSON.stringify(raw).slice(0, 200)}`);
  }
  if (typeof raw.state !== 'string' || raw.state.length === 0) {
    throw new Error(`geo directory row missing state: ${JSON.stringify(raw).slice(0, 200)}`);
  }
  return {
    project_ref: raw.project_ref,
    region: typeof raw.region === 'string' ? raw.region : null,
    subscription: typeof raw.subscription === 'string' ? raw.subscription : null,
    state: raw.state,
    applied_lsn: typeof raw.applied_lsn === 'string' ? raw.applied_lsn : null,
    replay_ts: typeof raw.replay_ts === 'string' ? raw.replay_ts : null,
    lag_bytes: (raw.lag_bytes ?? null) as WhGeoDirectoryRow['lag_bytes'],
    coverage: raw.coverage ?? null,
    last_health_at: String(raw.last_health_at ?? ''),
    updated_at: String(raw.updated_at ?? ''),
  };
}

export interface GeoDirectoryReader {
  /** ≤1-row population read (the one_geo_replica_serving law). Throws on
   *  read failure AND on the >1-row law breakage (limit(2) tripwire — a
   *  dropped storage law must never be silently resolved by picking one). */
  readGeoDirectory(): Promise<WhGeoDirectoryRow[]>;
}

export function makeGeoDirectoryReader(deps: { fetchGeo: GeoDirectoryFetcher }): GeoDirectoryReader {
  async function readGeoDirectory(): Promise<WhGeoDirectoryRow[]> {
    const { data, error } = await deps.fetchGeo();
    if (error) throw new Error(`geo directory read failed: ${error.message}`);
    const rows = (data ?? []).map(toWhGeoDirectoryRow);
    if (rows.length > 1) {
      throw new Error(
        `geo directory returned ${rows.length} rows — the one_geo_replica_serving law is DROPPED (storage law breakage, never silently pick one)`,
      );
    }
    return rows;
  }
  return { readGeoDirectory };
}
