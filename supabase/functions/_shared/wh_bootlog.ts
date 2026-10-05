// =============================================================================
// _shared/wh_bootlog.ts — r131 D2 boot-defect log consolidation (design
// design_d2_bootlog_consolidation_r129.md §2.1/§2.3).
// =============================================================================
// ONE merged boot log line per isolate boot replaces the shell's 8 scattered
// fixed-string `console.error` defect branches + the armed `console.log`
// (warehouse-engine/index.ts former :97/:100/:108/:171/:188/:198/:219/:230
// + :225). Worst-case boot log: 1,140 B (8 prose lines + newlines) ->
// 239 B (this ONE line), −79.0% (measured, python UTF-8 over the exact
// literals — design §4); armed steady-state 171 B -> 92 B (−46.2%), the
// recurring win on every armed isolate boot.
//
// PURE MODULE (F-N8 purity law — index.ts:34 "the _shared modules never read
// env"): NO Deno.env, NO console, NO fetch. This module formats and gates;
// the SHELL keeps the only console calls (severity mapping: defects =>
// console.error, defect-free armed boot => console.log — today's severities
// preserved). It takes no env-derived values — only the resolved codes.
//
// The line (fixed key order, compact separators, ASCII-only):
//   {"event":"wh_boot","ts_source":"platform","class":"boot_defect","lever":"inert","defects":["ownref_unparsed","proxy_kv_absent"]}
//
//   event     always "wh_boot" — the ONE merged line's code.
//   ts_source always "platform" — NO self-emitted timestamp: the platform's
//             function_logs row already carries the authoritative timestamp
//             for console.* custom events; emitting an ISO ts would cost
//             ~25 B/line for data the platform duplicates. The literal
//             documents the contract in-band.
//   class     "boot_defect" | "boot_armed" — the coarse triage class:
//             defects dominate (any defect => boot_defect); the defect-free
//             ARMED boot is the only healthy state that still emits (the
//             folded :225 line). Logs-tool-filterable; console severity
//             follows it (error vs info).
//   lever     "inert" | "armed" — the folded :225 armed log (r128 D2:
//             "fold it in as lever:'armed'" — folding chosen: armed boots
//             are the steady-state emitter).
//   defects   <=8 fixed codes, pushed in index.ts branch order (= the
//             source order of the code table below = deterministic bytes).
//
// Code table — 1:1 with the 8 branches (no branch disappears; this table is
// the line's legend):
//   ownref_unparsed        SUPABASE_URL absent/unparseable => own-ref
//                          resolver arm disabled
//   ownkey_empty           SUPABASE_SERVICE_ROLE_KEY empty => shard key
//                          resolution fails closed (shard_key_missing)
//   stamp_absent           ENGINE_BUILD placeholder => deploy provenance
//                          unavailable (/health renders engine_build: null)
//   proxy_token_unset      WH_PROXY_FETCHER=on with the dedicated
//                          WH_PROXY_TOKEN secret unset
//   proxy_kv_timeout       wh_shard_proxy_map boot read exceeded the 10s
//                          race (raceWhProxyKvBoot sentinel)
//   proxy_kv_absent        wh_shard_proxy_map row absent or unreadable
//   proxy_ownref_unparsed  armed-attempt with ownRef='' — the own-ref
//                          DIRECT carve-out cannot be guaranteed (fail-
//                          closed inert)
//   proxy_kv_failed        KV read rejected (raceWhProxyKvBoot swallows
//                          nothing)
//
// No-echo law (r57/r49; AM-8 "env name + defect class ONLY (never values)"):
// every code is a compile-time literal in the union type — interpolation is
// impossible by construction. No env value, ref, URL, key fragment, or KV
// content can reach the line; the codes name the env and the defect class
// exactly as the retired prose strings did.
//
// D1 wrap point: if the LOG_LEVEL shim (findings_logs_ingest_diet_r128.md
// §3 D1) ever lands, its emit() wraps the ONE consolidated call site in
// index.ts — its first gated customer is the info-class armed line
// (boot_armed); the 8 error-class codes stay ungated (the triage floor).
// D2 deliberately does NOT build on D1 (design §2.2).
// =============================================================================

/** The 8 boot-defect codes — 1:1 with the index.ts guard branches
 *  (the code table above is the legend). */
export type WhBootDefectCode =
  | 'ownref_unparsed'        // SUPABASE_URL unparseable (own-ref arm disabled)
  | 'ownkey_empty'           // SUPABASE_SERVICE_ROLE_KEY empty (fail-closed)
  | 'stamp_absent'           // ENGINE_BUILD stamp absent (provenance null)
  | 'proxy_token_unset'      // WH_PROXY_TOKEN unset while the lever is 'on'
  | 'proxy_kv_timeout'       // KV boot read timed out (10s race sentinel)
  | 'proxy_kv_absent'        // wh_shard_proxy_map row absent/unreadable
  | 'proxy_ownref_unparsed'  // armed-attempt with ownRef '' (carve-out unguaranteeable)
  | 'proxy_kv_failed';       // KV read rejected (never swallowed)

/** The WH_PROXY lever state at boot (the folded :225 armed log). */
export type WhBootLeverState = 'inert' | 'armed';

export interface WhBootLogInput {
  lever: WhBootLeverState;
  defects: WhBootDefectCode[];
}

/** The ONE merged boot line — JSON.stringify with FIXED key order (event,
 *  ts_source, class, lever, defects) and compact separators. class =
 *  defects.length > 0 ? 'boot_defect' : 'boot_armed' (defects dominate);
 *  defects keep the caller's branch order — deterministic bytes. */
export function formatWhBootLog(input: WhBootLogInput): string {
  return JSON.stringify({
    event: 'wh_boot',
    ts_source: 'platform',
    class: input.defects.length > 0 ? 'boot_defect' : 'boot_armed',
    lever: input.lever,
    defects: input.defects,
  });
}

/** Emit gate: a clean INERT boot emits NOTHING (byte-identical to today's
 *  0-line path); any defect emits; an armed boot always emits (the folded
 *  :225 success line — the steady-state emitter). */
export function shouldEmitWhBootLog(input: WhBootLogInput): boolean {
  return input.defects.length > 0 || input.lever === 'armed';
}
