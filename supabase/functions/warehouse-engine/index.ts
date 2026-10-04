// =============================================================================
// warehouse-engine/index.ts — thin Deno.serve shell (r40 extraction)
// =============================================================================
// The handler lives in _shared/wh_entrypoint.ts (injected-deps, offline-
// testable — r40 wiring review: three P0s survived because the r39 monolith
// had zero direct coverage). This shell wires real deps only:
//   * readDirectory -> supabase-js client.rpc('wh_directory_atomic_read') —
//     ONE atomic rpc POST returning {version, rows} under one Postgres
//     snapshot (logic in _shared/wh_directory_reader.ts; r120 OPT-1: the r40
//     pagination + P2-4 double-probe read chain is retired for the
//     single-snapshot RPC)
//   * probeDirectoryVersion -> supabase-js config GET (UNCHANGED — kept for
//     the replay path + /health; no longer part of the fresh-path pre-fanout
//     chain, which reads the version atomically inside the rpc payload)
//   * fetcher -> the r69 RPC REAL FETCHER (makeRpcShardFetcher over the
//     shard service-key channel, _shared/wh_shard_channel.ts — the §6.1
//     wh_query POST; the select-shape URL stays the target identity). The
//     entrypoint-level hasRealFetcher gate STILL rejects /query 500 until
//     the flip — landing the channel did NOT move the gate.
//   * hasRealFetcher -> FLIP_hasRealFetcher (the single flip constant in
//     _shared/wh_entrypoint.ts; currently false — see it for the exact
//     flip conditions)
//   * handshake -> the §5.2 default handshake (makeWhHandshake over the real
//     fetch, contract §4.6 plane auth) with the SAME resolver feeding its
//     shardServiceKey (r69 D6 single source of truth) — unreachable while
//     the gate is false
//   * rpcMode -> the WH_REAL_FETCHER env-guard expression (the staged r60
//     flip lever's exact 'on' value; passed into the core via deps — F-N8
//     purity: the _shared modules never read env)
// Deployment shape (§4.1 pinned): path'd URL, verify_jwt=false, fn-level
// bearer auth inside the handler.
// =============================================================================

import { handleWhEngineRequest, rywGateEnabledFromEnv, FLIP_hasRealFetcher } from '../_shared/wh_entrypoint.ts';
import type { WhEngineDeps } from '../_shared/wh_entrypoint.ts';
import { makeDirectoryReader, makeGeoDirectoryReader } from '../_shared/wh_directory_reader.ts';
import { makeWhHandshake } from '../_shared/wh_handshake.ts';
import { makeRpcShardFetcher, makeShardKeyResolver, parseShardKeyEnv } from '../_shared/wh_shard_channel.ts';
import { fetchFenceConfig, ownProjectRefFromSupabaseUrl } from '../_shared/geo_write_fence.ts';
import type { WhFenceClient } from '../_shared/geo_write_fence.ts';
import { db } from '../_shared/whe_store.ts';

const deps: WhEngineDeps = (() => {
  // r69 §3.3 (D4/D5/D6): the shard SERVICE-KEY channel — parsed ONCE at
  // isolate boot, wired ONCE, feeding BOTH the handshake plane auth AND the
  // real fetcher (single source of truth). Purity: THIS shell owns the env
  // reads; the _shared channel module never touches Deno.env.
  const shardKeys = parseShardKeyEnv(Deno.env.get('WH_SHARD_KEYS'));
  const ownRef = ownProjectRefFromSupabaseUrl(Deno.env.get('SUPABASE_URL') ?? '');
  const ownKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY') ?? '';
  // r69 AM-8 boot defect-class log — once per isolate boot, env name +
  // defect class ONLY (never values). The fail-closed enforcement itself
  // stays per-call inside the resolver (OQ-7: once-per-boot reading pinned).
  if (ownRef === '') {
    console.error('warehouse-engine shard key channel defect: SUPABASE_URL unparseable — own-ref resolver arm disabled (ownRef empty)');
  }
  if (ownKey === '') {
    console.error('warehouse-engine shard key channel defect: SUPABASE_SERVICE_ROLE_KEY empty — own-ref key resolution fails closed (shard_key_missing)');
  }
  const resolveShardKey = makeShardKeyResolver({ ownRef, ownKey, remoteKeys: shardKeys });

  const client = db();
  // r120 OPT-1: the directory read is ONE atomic rpc POST — the
  // single-statement language-sql wh_directory_atomic_read RPC returns
  // {version, rows} under one Postgres snapshot (the r40 paginated-page
  // wiring + P2-4 double-probe are retired; the version-probe
  // wiring below is UNCHANGED — the replay path + /health still probe it).
  const atomicReader = makeDirectoryReader({
    fetchVersion: async () => {
      const { data, error } = await client
        .from('config')
        .select('value')
        .eq('key', 'warehouse_directory_version')
        .maybeSingle();
      if (error) return { value: null, error };
      return data === null ? null : { value: (data as { value: unknown }).value, error: null };
    },
    fetchAtomic: async () => {
      const { data, error } = await client.rpc('wh_directory_atomic_read');
      return { data, error };
    },
  });
  // r47 read-plane reader (wh-contract r47 errata): fixed limit(2) over
  // v_geo_directory — the ≤1-row population (one_geo_replica_serving); the
  // reader owns the law-breakage tripwire (>1 row throws). Unreachable until
  // FLIP_hasRealFetcher flips (the deploy gate keeps precedence).
  const geoReader = makeGeoDirectoryReader({
    fetchGeo: async () => {
      const { data, error } = await client
        .from('v_geo_directory')
        .select('*')
        .limit(2);
      return { data, error };
    },
  });
  return {
    probeDirectoryVersion: atomicReader.probeDirectoryVersion,
    readDirectory: atomicReader.readDirectory,
    // r69 §3.4: the RPC-primary real fetcher over the shard key channel
    // (D7/D8/D9 — never throws/rejects; the §6.1 wh_query POST on the rpc
    // arm, the loud shard_path_select_disabled refusal on the residual
    // select arm). The pre-r69 throwing stub is DELETED (wh_entrypoint_test
    // static flipped to the deletion pin, §5). Still never REACHED while
    // hasRealFetcher=false (the handler rejects /query before any fan-out).
    fetcher: makeRpcShardFetcher({ resolveKey: resolveShardKey }),
    // r47 read-plane deps: the engine's gate ladder (G1..G6) owns every
    // decision; each dep fails CLOSED (unreadable config => null; unknown
    // table => null is_reference; a throwing read degrades to the primary).
    readGeoDirectory: geoReader.readGeoDirectory,
    readGeoMode: async () => {
      const { data, error } = await client
        .from('config')
        .select('value')
        .eq('key', 'geo_mode')
        .maybeSingle();
      if (error || data === null) return null;
      const v = (data as { value: unknown }).value;
      return typeof v === 'string' ? v : null;
    },
    readTableReference: async (table: string) => {
      const { data, error } = await client
        .from('warehouse_tables')
        .select('is_reference')
        .eq('logical_name', table)
        .maybeSingle();
      if (error || data === null) return null;
      const v = (data as { is_reference: unknown }).is_reference;
      return typeof v === 'boolean' ? v : null;
    },
    // r44: the deploy gate's SINGLE flip site is FLIP_hasRealFetcher in
    // _shared/wh_entrypoint.ts (flip conditions listed there — real fetcher
    // frozen + PAT + probes #1/#2/#3 green per scatter §9). The wiring
    // passes the constant explicitly so the gate is visible here; it stays
    // false until the flip lands on a reviewed diff.
    hasRealFetcher: FLIP_hasRealFetcher,
    // r44 §5.2 handshake: DEFAULT implementation over the real platform
    // fetch with the engine→shard plane auth (contract §4.6 — apikey +
    // Authorization Bearer = shard service key). r69 (D6): the per-shard
    // service key now resolves through the SAME resolver instance the real
    // fetcher uses (the throwing stub is deleted — the service-key channel
    // landed with it). NEVER invoked while hasRealFetcher=false (the
    // handler rejects /query before any fan-out).
    handshake: makeWhHandshake({
      fetcher: (url, init) => fetch(url, init),
      shardServiceKey: resolveShardKey,
    }),
    // §4.6 snapshot HMAC key — a DEDICATED secret (set via the secrets API),
    // never WHE_BEARER_TOKEN (a client knowing the key could forge snapshots).
    // Absent => snapshot mode disabled (full embed only, nothing attached).
    ...(Deno.env.get('WH_SNAPSHOT_KEY') !== undefined ? { snapshotKey: Deno.env.get('WH_SNAPSHOT_KEY')! } : {}),
    // r49 B2 wave1: the WRITE-PLANE FENCE deps (geo_write_fence.ts). The
    // fence read is the ONE combined config read (G-W1) over the same client
    // — the per-request config read is already paid for on this plane (D16);
    // the override key joins it per R3/re-audit #3e. ownProjectRef = the
    // SUPABASE_URL subdomain (the co-hosted law); '' = unparseable ⇒ the R3
    // dispatch identity degrades to placements (never a self-dispatch).
    // The cast is the supabase-js-generic escape hatch (TS2589 instantiation
    // depth against the minimal structural WhFenceClient surface; the runtime
    // surface used is exactly from→select→in — the battery pins the chain).
    fetchFenceConfig: () => fetchFenceConfig(client as unknown as WhFenceClient),
    ownProjectRef: ownProjectRefFromSupabaseUrl(Deno.env.get('SUPABASE_URL') ?? ''),
    // r57 wh_ryw v1 flip lever (design_r53_wh_ryw_lsn_poll.md; PLAN r58
    // secondary "G6 stamped compare; flip stays gated"). THE LEVER LIVES
    // HERE: the WH_RYW_V1 env, read at boot — a deploy-time constant, never
    // a code change. Activation value: the exact string 'on' (parsed by
    // rywGateEnabledFromEnv — anything else, including absent, is OFF).
    //   * OFF (the default this repo ships): a min_lsn-pinned /query is a
    //     400 naming the lever (planner-honest, fail-closed); every request
    //     WITHOUT min_lsn is byte-identical to the pre-r57 behavior
    //     (additive-only law) — including read_plane:"replica" eventual-
    //     consistency reads, which the lever does NOT gate.
    //   * ON: the r47 wh-contract errata behavior exactly — the as-built G6
    //     stamped-compare ladder (wh_engine_core.ts resolveGeoPlane)
    //     adjudicates the pinned floor against the fm_geo_replica_stamp-
    //     fed applied_lsn (BigInt, never text; missing stamp fail-closed).
    // Flipping is a DEPLOY decision owned by the QUALITY GATES step — never
    // landed silently on a diff (mirror of FLIP_hasRealFetcher's doctrine).
    rywGateEnabled: rywGateEnabledFromEnv(Deno.env.get('WH_RYW_V1')),
    // r69 rpcMode (AM-4/F-N8): computed HERE from the SAME env-guard
    // expression the staged r60 flip patch's realFetcherFlipFromEnv
    // implements (WH_REAL_FETCHER, single activation value the exact string
    // 'on' — mirror of the WH_RYW_V1 lever above). The expression is inline
    // DELIBERATELY: the staged patch's wh_entrypoint.ts hunk adds the shared
    // parse fn at the flip window, and a duplicate would break its clean
    // application (only the patch's index.ts hunk is expected to conflict —
    // flip-time reconciliation collapses this line into the patch's gate OR).
    // Passed INTO the core via deps (purity law — the core never reads env);
    // with the env unset this is false ⇒ the unset path is byte-identical.
    rpcMode: Deno.env.get('WH_REAL_FETCHER') === 'on',
  };
})();

Deno.serve((req: Request) => handleWhEngineRequest(req, deps));
