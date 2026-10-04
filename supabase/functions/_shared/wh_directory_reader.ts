// =============================================================================
// _shared/wh_directory_reader.ts — v_warehouse_directory reader + version probe
// =============================================================================
// r40 (wiring review maxxing-r40-wiring-review): extracted from the r39
// warehouse-engine/index.ts readDirectory() so the pagination loop, truncation
// guard, version double-probe and row mapping are offline-testable with
// injected low-level fetchers (deploy.ts pattern: the entrypoint wires real
// supabase-js calls, the factory owns ALL the logic).
//
// Review fixes carried here:
//   * P0-2: the view exposes `pr.ref as project_ref` — there is NO `shard`
//     column. Rows are mapped through toWhDirectoryRow() (project_ref -> shard)
//     with loud failures on missing/mistyped required fields (view drift is an
//     internal error, never a silently-undefined shard).
//   * P2-4: the directory version is probed BEFORE and AFTER the row read; a
//     change between probes retries the whole read ONCE (concurrent catalog
//     mutation otherwise stamps mixed rows with a wrong version, §4.6).
//   * P2-5: the config value is accepted only when `typeof === 'number'` —
//     Number(null)/Number(false)/Number('') all coerce to 0 and defeat the
//     guard (r38 SCALAR-encoding law protects the writer; this probe
//     re-validates the reader).
//   * P0-3 design note: the read is a FULL embed (all tables) because §4.6
//     snapshot mode attaches the full directory for the client to cache; the
//     per-table projection for query execution happens in the entrypoint
//     (one read serves both needs).
// =============================================================================

import type { WhDirectoryRow, WhGeoDirectoryRow } from './wh_engine_core.ts';

export const DIRECTORY_PAGE = 1000;

/** Low-level page fetch over v_warehouse_directory (index.ts wires supabase-js). */
export type DirectoryPageFetcher = (
  from: number,
  to: number,
) => Promise<{ data: unknown[] | null; count: number | null; error: { message: string } | null }>;

/** Low-level config probe for warehouse_directory_version (index.ts wires supabase-js). */
export type DirectoryVersionFetcher = () => Promise<{
  value: unknown;
  error: { message: string } | null;
} | null>;

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
  return row;
}

/**
 * Build the directory reader. fetchPage/fetchVersion are the ONLY impure
 * edges (injected by index.ts with real supabase-js calls; tests inject
 * call-counting fakes).
 */
export function makeDirectoryReader(deps: {
  fetchPage: DirectoryPageFetcher;
  fetchVersion: DirectoryVersionFetcher;
}): DirectoryReader {
  async function probeDirectoryVersion(): Promise<number> {
    const row = await deps.fetchVersion();
    if (row === null) throw new Error('directory version probe failed: config row missing');
    if (row.error) throw new Error(`directory version probe failed: ${row.error.message}`);
    return coerceDirectoryVersion(row.value);
  }

  async function readRowsOnce(): Promise<WhDirectoryRow[]> {
    const rows: WhDirectoryRow[] = [];
    let from = 0;
    for (;;) {
      const { data, count, error } = await deps.fetchPage(from, from + DIRECTORY_PAGE - 1);
      if (error) throw new Error(`directory read failed: ${error.message}`);
      const page = data ?? [];
      rows.push(...page.map(toWhDirectoryRow));
      if (count !== null) {
        if (rows.length > count) {
          throw new Error(`directory embed fetched ${rows.length} rows > count ${count} — refusing (truncation guard)`);
        }
        if (rows.length >= count) break;
      } else if (page.length < DIRECTORY_PAGE) break;
      from += DIRECTORY_PAGE;
    }
    return rows;
  }

  async function readDirectory(): Promise<{ rows: WhDirectoryRow[]; version: number }> {
    // P2-4: version probed BEFORE and AFTER the row read; the read is valid
    // only when the pair agrees (the rows were read under ONE version). One
    // retry on flap; a second flap refuses to stamp a mixed read.
    for (let attempt = 0; attempt < 2; attempt++) {
      const versionBefore = await probeDirectoryVersion();
      const rows = await readRowsOnce();
      const versionAfter = await probeDirectoryVersion();
      if (versionAfter === versionBefore) return { rows, version: versionBefore };
    }
    throw new Error('directory version flapped during read across two attempts — refusing to stamp a mixed read');
  }

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
