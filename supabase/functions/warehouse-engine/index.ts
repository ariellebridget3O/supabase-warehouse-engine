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
//     the gate is false. r121 OPT-1b: this dep now serves the SAMPLED
//     inventory-audit backstop ONLY (1-in-16 by sha256(qid) bucket — the
//     per-query sweep is folded into wh_query's per-call eligibility;
//     manifest-side max_rows pre-refusal covers F15 with zero network).
//     Wiring UNCHANGED — no env lever on the sampler (K_SAMPLING is a
//     pinned constant in _shared/wh_engine_core.ts; _shared never reads env).
//   * rpcMode -> the WH_REAL_FETCHER env-guard expression (the staged r60
//     flip lever's exact 'on' value; passed into the core via deps — F-N8
//     purity: the _shared modules never read env)
//   * engineBuild -> the GENERATED build stamp (r124 A8,
//     design_r124_opt3_a8.md §2): _shared/engine_build.ts is gitignored and
//     written by `make stamp` (the HEAD sha7). The SHELL imports the
//     constant statically (a constant import cannot throw — the audit-B
//     mechanism) and threads it as deps.engineBuild (the hasRealFetcher DI
//     precedent — _shared itself never reads the file). Absent/empty ⇒ the
//     dep is not threaded: /health renders engine_build: null + ONE
//     fixed-string boot defect log below (echo law).
// Deployment shape (§4.1 pinned): path'd URL, verify_jwt=false, fn-level
// bearer auth inside the handler.
// =============================================================================

import { handleWhEngineRequest, rywGateEnabledFromEnv, FLIP_hasRealFetcher } from '../_shared/wh_entrypoint.ts';
import type { WhEngineDeps } from '../_shared/wh_entrypoint.ts';
// r124 A8: the GENERATED build stamp — gitignored, NEVER committed
// (committing = sha self-reference regress). `make stamp` writes it; the
// Makefile check/ci targets bootstrap it (a fresh clone's deno check fails
// on the missing import otherwise).
import { ENGINE_BUILD } from '../_shared/engine_build.ts';
import { makeDirectoryReader, makeGeoDirectoryReader } from '../_shared/wh_directory_reader.ts';
import { makeWhHandshake } from '../_shared/wh_handshake.ts';
import {
  makeOwnRefBypassRawFetch,
  makeProxiedRawFetch,
  makeRpcShardFetcher,
  makeShardKeyResolver,
  parseShardKeyEnv,
  parseWhProxyMapValue,
  raceWhProxyKvBoot,
  WH_PROXY_KV_BOOT_TIMEOUT_MS,
} from '../_shared/wh_shard_channel.ts';
import type { WhProxyRawFetch } from '../_shared/wh_shard_channel.ts';
import { fetchFenceConfig, ownProjectRefFromSupabaseUrl } from '../_shared/geo_write_fence.ts';
// r131 D2: the merged boot log — pure formatter + emit gate (the shell keeps
// the only console calls; F-N8 purity intact — the module reads no env).
import { formatWhBootLog, shouldEmitWhBootLog } from '../_shared/wh_bootlog.ts';
import type { WhBootDefectCode, WhBootLeverState, WhBootLogInput } from '../_shared/wh_bootlog.ts';
import type { WhFenceClient } from '../_shared/geo_write_fence.ts';
import { db } from '../_shared/whe_store.ts';

// r124 A8: TS — the generated constant's literal type (the file is the
// design's exact one-liner) is widened HERE so the empty-placeholder arms
// below type-check without touching the generator's output shape.
const engineBuild: string = ENGINE_BUILD;

// r123: the IIFE is now async (top-level await) — the WH_PROXY lever's
// boot-once KV read below needs ONE awaited PostgREST round-trip at isolate
// boot (⟫B4: boot-once like parseShardKeyEnv, not per-request like
// readGeoMode). The await is fail-closed: any KV fault logs ONE fixed string
// and boots with the lever INERT — Deno.serve below still starts. The read
// is TIMEOUT-RACED (raceWhProxyKvBoot, r123 fresh-eyes P3 fix): a HANGING
// PostgREST at boot degrades to the KV-absent state (lever inert + the
// fixed-string timeout defect log) after WH_PROXY_KV_BOOT_TIMEOUT_MS
// instead of wedging module evaluation forever.
// r131 D2 boot-log consolidation (design_d2_bootlog_consolidation_r129.md
// §2.1): the boot-defect COLLECTOR — the guard arms below push 1:1 codes IN
// BRANCH ORDER instead of firing 8 scattered prose console.error lines; the
// ONE merged {"event":"wh_boot",...} line emits ONCE after the IIFE, before
// Deno.serve (OQ-7: once per isolate boot — one collector, one emit).
// Module-scope so the post-IIFE emit block sees it. Echo law (AM-8): the
// codes are compile-time literals — env name + defect class ONLY, never
// values (no env value, ref, URL, key fragment, or KV content can reach the
// line). Severity mapping lives at the emit site (defects => console.error,
// defect-free armed boot => console.log).
const bootDefects: WhBootDefectCode[] = [];
let bootLever: WhBootLeverState = 'inert';

const deps: WhEngineDeps = await (async () => {
  // r69 §3.3 (D4/D5/D6): the shard SERVICE-KEY channel — parsed ONCE at
  // isolate boot, wired ONCE, feeding BOTH the handshake plane auth AND the
  // real fetcher (single source of truth). Purity: THIS shell owns the env
  // reads; the _shared channel module never touches Deno.env.
  const shardKeys = parseShardKeyEnv(Deno.env.get('WH_SHARD_KEYS'));
  const ownRef = ownProjectRefFromSupabaseUrl(Deno.env.get('SUPABASE_URL') ?? '');
  const ownKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY') ?? '';
  // r69 AM-8 boot defect-class log — once per isolate boot, env name +
  // defect class ONLY (never values). r131 D2: the branches now push 1:1
  // codes onto the bootDefects collector (the ONE merged wh_boot line emits
  // after the IIFE); the fail-closed enforcement itself stays per-call
  // inside the resolver (OQ-7: once-per-boot reading pinned).
  if (ownRef === '') {
    bootDefects.push('ownref_unparsed');
  }
  if (ownKey === '') {
    bootDefects.push('ownkey_empty');
  }
  // r124 A8 (design §2 ⟫B-1): the stamp's ONE boot defect — an EMPTY
  // generated constant (a hand-written placeholder that only satisfies the
  // import) means deploy provenance is unavailable. r131 D2: rides the
  // merged wh_boot line as the 'stamp_absent' code (echo law: never value
  // fragments, never a 500 on /health over it — the dep simply stays
  // unthreaded and /health renders engine_build: null).
  if (engineBuild === '') {
    bootDefects.push('stamp_absent');
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
  // r123 WH_PROXY rawFetch lever (design_r122_acct2_proxy.md §1.3/§1.4;
  // audits ⟫B4 boot-once KV + ⟫A9 browser_headers): the acct2 egress-pool
  // lever for the WH fanout plane. ONE boot-once KV read of
  // `wh_shard_proxy_map` via the DIRECT db() client chain — NEVER the shared
  // getConfig reader (it silently drops unknown keys). Activation requires ALL
  // THREE: WH_PROXY_FETCHER exactly 'on' (mirror of the WH_REAL_FETCHER /
  // WH_RYW_V1 activation expressions) + the DEDICATED WH_PROXY_TOKEN secret
  // (NEVER WHE_BEARER_TOKEN — the snapshotKey doctrine) + a well-formed
  // wh_shard_proxy_map value, re-validated ENGINE-SIDE by parseWhProxyMapValue
  // (defense in depth — the FM 0019/0027 validator mirrored: exactly {url,
  // refs}, NON-EMPTY refs, strict proxy-fn URL shape, 20-char refs, host-ref
  // ∈ refs, NO JWT-shaped string anywhere — the token NEVER rides config).
  // ANY miss ⇒ the lever is INERT: proxyRawFetch stays undefined, the
  // rawFetch dep below is the default platform fetch (byte-identical unset
  // path — mirror of the rpcMode expression's unset law), and ONE fixed
  // code pushes onto the bootDefects collector (the merged wh_boot line;
  // echo law: never value fragments, never the KV content).
  // OFF is the shipped default: with the env not 'on' this block reads NO
  // other env, performs NO KV round-trip, and boots exactly as pre-r123.
  // The activation expression is INLINE (mirror of the rpcMode expression —
  // the exact-string style the shell statics pin).
  let proxyRawFetch: WhProxyRawFetch | undefined;
  if (Deno.env.get('WH_PROXY_FETCHER') === 'on') {
    const proxyToken = Deno.env.get('WH_PROXY_TOKEN') ?? '';
    if (proxyToken === '') {
      bootDefects.push('proxy_token_unset');
    } else {
      try {
        // r123 fresh-eyes P3 fix: the boot KV read is TIMEOUT-RACED — a
        // hanging PostgREST yields the `null` sentinel after
        // WH_PROXY_KV_BOOT_TIMEOUT_MS (lever inert + the fixed-string
        // timeout defect log inside the callback) instead of wedging module
        // evaluation. A read that rejects still propagates UNCHANGED into
        // the catch's fixed-string log (raceWhProxyKvBoot swallows nothing).
        const raced = await raceWhProxyKvBoot(
          client
            .from('config')
            .select('value')
            .eq('key', 'wh_shard_proxy_map')
            .maybeSingle(),
          WH_PROXY_KV_BOOT_TIMEOUT_MS,
          () => {
            bootDefects.push('proxy_kv_timeout');
            return null;
          },
        );
        if (raced === null) {
          // the timeout code was already pushed inside the callback —
          // stays inert (the KV-absent state).
        } else {
          const { data, error } = raced;
          if (error !== null || data === null) {
            bootDefects.push('proxy_kv_absent');
          } else {
            // Validate ONCE (one boot, one defect log on any shape fault — the
            // validator owns the per-class fixed-string log, value fragments
            // NEVER echoed). null ⇒ inert: proxyRawFetch stays undefined.
            const proxyMap = parseWhProxyMapValue((data as { value: unknown }).value);
            // r123 P0 fix (the live-e2e RED closure): the lever arms ONLY as
            // the OWN-REF DIRECT carve-out wrapper (makeOwnRefBypassRawFetch)
            // and ONLY with a KNOWN own ref — the SUPABASE_URL subdomain
            // (ownProjectRefFromSupabaseUrl, the SAME authoritative source the
            // shard-key resolver uses above; the co-hosted law). The engine's
            // own shard is intra-project and must NEVER ride the cross-account
            // proxy: the v14 live e2e drew a relayed 401 "Invalid API key"
            // from the own host's public edge for the own-ref service
            // credential presented from the proxy egress, while the SAME
            // credential succeeds on the isolate-internal direct path. An
            // unparseable SUPABASE_URL keeps the lever INERT (fail-closed —
            // "own never proxies" is unprovable without the own-ref
            // identity); the bypass arm itself ALSO disables on '' (AM-8
            // mirror, belt-and-braces).
            if (proxyMap !== null && ownRef === '') {
              bootDefects.push('proxy_ownref_unparsed');
            } else if (proxyMap !== null) {
              proxyRawFetch = makeOwnRefBypassRawFetch({
                ownRef,
                proxiedRawFetch: makeProxiedRawFetch({ proxyUrl: proxyMap.url, proxyToken }),
              });
              // r131 D2: the armed log folds into the merged wh_boot line
              // (lever:'armed', class:'boot_armed' — the steady-state
              // emitter, emitted after the IIFE below).
              bootLever = 'armed';
            }
          }
        }
      } catch {
        bootDefects.push('proxy_kv_failed');
      }
    }
  }
  return {
    probeDirectoryVersion: atomicReader.probeDirectoryVersion,
    readDirectory: atomicReader.readDirectory,
    // r69 §3.4: the RPC-primary real fetcher over the shard key channel
    // (D7/D8/D9 — never throws/rejects; the §6.1 wh_query POST on the rpc
    // arm, the loud shard_path_select_disabled refusal on the residual
    // select arm). The pre-r69 throwing stub is DELETED (wh_entrypoint_test
    // static flipped to the deletion pin, §5). Still never REACHED while
    // hasRealFetcher=false (the handler rejects /query before any fan-out).
    // r123 WH_PROXY lever: the rawFetch dep rides the acct2 proxy ONLY when
    // the boot block above armed it (env 'on' + token + well-formed KV + a
    // known own ref) — and then as the OWN-REF DIRECT carve-out wrapper: the
    // engine's OWN host keeps the default platform fetch (the co-hosted law;
    // the r123 P0), remote shards ride the proxy. The spread adds NOTHING
    // otherwise, so the inert path is byte-identical to pre-r123 (default
    // platform fetch, the seam default in wh_shard_channel.ts). The
    // handshake plane above stays DIRECT either way.
    fetcher: makeRpcShardFetcher({
      resolveKey: resolveShardKey,
      ...(proxyRawFetch !== undefined ? { rawFetch: proxyRawFetch } : {}),
    }),
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
    // r124 A8 (design §2 ⟫B-1): the generated build stamp rides as a DI dep
    // (the hasRealFetcher precedent above — the constant import lives in
    // THIS shell; _shared only ever sees the injected value). Conditional
    // spread: with an empty constant the field is ABSENT (byte-identical
    // unset path; /health renders engine_build: null).
    ...(engineBuild !== '' ? { engineBuild } : {}),
  };
})();

// r131 D2: the ONE merged boot line (design §2.1/§2.3) — the 8 prose defect
// branches + the armed console.log above collapse into one JSON line
// (worst-case 1,140 B -> 239 B, −79.0%; armed steady-state 171 B -> 92 B).
// Severity mapping preserved: a defect-bearing boot emits via console.error
// (error class), the defect-free ARMED boot via console.log (info — the
// former :225 severity). A clean inert boot emits NOTHING (the byte-
// identical 0-line path). Still ONCE per isolate boot (OQ-7). D1 wrap
// point: the LOG_LEVEL shim (r128 findings D1), if it ever lands, wraps
// THIS call site — the armed/info-class line is its first gated customer;
// the error-class defects stay ungated (the triage floor).
const bootLog: WhBootLogInput = { lever: bootLever, defects: bootDefects };
if (shouldEmitWhBootLog(bootLog)) {
  const line = formatWhBootLog(bootLog);
  if (bootLog.defects.length > 0) console.error(line); // class boot_defect
  else console.log(line); // class boot_armed
}

Deno.serve((req: Request) => handleWhEngineRequest(req, deps));
