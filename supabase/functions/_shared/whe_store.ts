// =============================================================================
// _shared/whe_store.ts — the CONSUMER-STORE SEAM (r116 extraction)
// =============================================================================
// Provenance: extracted VERBATIM from the fleet-manager repo's
// supabase/functions/_shared/supabase-client.ts @ca4d280 (worklog lane
// supabase-free-tier-maxxing, task maxxing-r116-whe-legB). The engine repo was
// carved out of FM WITHOUT that file (401 LOC), but the engine shell
// (warehouse-engine/index.ts) consumes exactly ONE export from it: db(). This
// module is the consumer-store seam — the engine's ONLY production entry point
// to supabase-js — re-homed standalone on `npm:@supabase/supabase-js@2`.
//
// Deliberately NOT ported (the engine never calls them; grep-verified against
// the engine's import surface before extraction):
//   * the r19 config-read JWT clock-skew gate (readServiceJwtClaims /
//     configJwtSkewVerdict / configJwtSkewError / JWT_CLOCK_SKEW_SEC) — that
//     gate protects FM's getConfig() config-table contract, which the engine
//     does not use (the engine reads config via its own wired chains);
//   * getConfig / setConfig / configPatchValueError (FM's config schema);
//   * audit() (the engine writes no audit_log rows);
//   * bumpQuota() (the engine tracks no FM quota counters).
//
// Semantics preserved byte-for-byte from FM:
//   * env names: SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY (the platform
//     injects both automatically on Supabase edge functions);
//   * the exact missing-env throw message (pinned byte-exact in
//     whe_store_test.ts);
//   * client options: auth { persistSession: false, autoRefreshToken: false }
//     and global.headers['x-client-info'] = 'fleet-manager/edge-fn'. The
//     header string is FM's provenance identity, kept VERBATIM by the r116
//     contract (identical client options) — pinned in whe_store_test.ts;
//   * the lazy per-isolate singleton cache (isolates are recycled per call, so
//     this is effectively per-call; the cache precedes env re-reads).
//
// Testing contract: tests NEVER hit a live DB. whe_store_test.ts injects
// fakes for the builder surface, composes the engine's read chains against
// factory output WITHOUT ever awaiting a real builder (createClient performs
// no I/O — the first fetch byte only leaves when a PostgREST builder is
// awaited), and text-imports index.ts for static pins. The engine's request
// seam for tests remains WhEngineDeps (wh_entrypoint.ts) — no factory lives
// here on purpose: index.ts keeps its read chains (smallest surgical diff),
// so the only thing tests must replace is db() itself.
//
// Purity: this file imports NOTHING except npm:@supabase/supabase-js@2 (plus
// its type), reads no env beyond the two names above, and touches no other
// _shared module (pinned by whe_store_test.ts's import-set pin).
// =============================================================================

import { createClient, type SupabaseClient } from 'npm:@supabase/supabase-js@2';

let _client: SupabaseClient | null = null;

/**
 * Return the cached service-role Supabase client for the Fleet Manager's DB.
 * Throws if required env vars are missing.
 */
export function db(): SupabaseClient {
  if (_client) return _client;

  const url = Deno.env.get('SUPABASE_URL');
  const key = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY');
  if (!url || !key) {
    throw new Error(
      'SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set as edge function ' +
      'secrets. Run: supabase secrets set SUPABASE_URL=... SUPABASE_SERVICE_ROLE_KEY=...'
    );
  }

  _client = createClient(url, key, {
    auth: { persistSession: false, autoRefreshToken: false },
    // Edge functions have a 150s wall clock; keep our own timeouts lower.
    global: {
      headers: { 'x-client-info': 'fleet-manager/edge-fn' },
    },
  });
  return _client;
}
