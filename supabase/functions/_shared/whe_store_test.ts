// =============================================================================
// _shared/whe_store_test.ts — LETHAL offline pins for the consumer-store seam
// =============================================================================
// r116 (maxxing-r116-whe-legB) battery for whe_store.ts, the verbatim re-home
// of FM supabase-client.ts's db() @ca4d280. THREE evidence planes, all offline:
//
//   1. db() env semantics — the byte-exact FM throw when SUPABASE_URL or
//      SUPABASE_SERVICE_ROLE_KEY is missing, save/restore env per test (the
//      wh_entrypoint_test.ts withEnv pattern). These pins are registered
//      FIRST: the module singleton cache must still be cold (Deno runs
//      registrations in file order; --jobs parallelizes files, never the
//      tests inside one file — so the ordering holds under any invocation).
//   2. Factory-output shape — db() is called for real against a synthetic
//      env. This is NOT a network call: createClient only CONSTRUCTS (the
//      first fetch byte leaves when a PostgREST builder is awaited; no real
//      builder is awaited anywhere in this file). Pins: the builder surface
//      the engine's six read chains need, the FM-provenance x-client-info
//      option passing through verbatim, and the singleton cache identity.
//   3. The six read chains' EXACT PostgREST wire shapes — hand-computed:
//        eq(col, v)            => ?...&col=eq.<v>
//        in(col, [a,b,c])      => ?...&col=in.(a,b,c)   (href: %2C %28 %29)
//        limit(n)              => ?...&limit=n
//        rpc(fn)               => POST /rest/v1/rpc/<fn> body {} — r120 OPT-1:
//                                 chain #1 is the atomic {version, rows} read
//                                 (the paginated range(from,to) GET window is
//                                 RETIRED with the pagination loop)
//        select('*') / select('value') / select('key,value') render verbatim
//        count:'exact' — RETIRED r120 together with the pagination it guarded
//          (the loud DIRECTORY_EMBED_BOUND fleet-shape bound supersedes the
//          truncation guard; the old count:'exact' statics went with it)
//      Chains #1-#5 live in warehouse-engine/index.ts closures over `const
//      client = db()` (not importable without env — the shell wires deps at
//      boot and Deno.serve at the bottom), so the source is pinned statically
//      via a text-import (the wh_entrypoint_test.ts pattern) AND composed
//      dynamically on a real db() client; chain #6 (the fence read) IS
//      importable (geo_write_fence.fetchFenceConfig) and is exercised through
//      a recording fake whose thenable we control — zero network anywhere.
//
// Runs offline:  deno test --no-check -q --allow-env --allow-read supabase/functions/_shared/whe_store_test.ts
// =============================================================================

import { db } from './whe_store.ts';
import { fetchFenceConfig, FENCE_CONFIG_KEYS } from './geo_write_fence.ts';
import type { WhFenceClient, WhFenceQueryResult } from './geo_write_fence.ts';
import indexSrc from '../warehouse-engine/index.ts' with { type: 'text' };
import storeSrc from './whe_store.ts' with { type: 'text' };

// -----------------------------------------------------------------------------
// Helpers (self-contained, wh_entrypoint_test.ts house style)
// -----------------------------------------------------------------------------

let passed = 0;
let failed = 0;
const failures: string[] = [];

function eq(name: string, actual: unknown, expected: unknown): void {
  const a = JSON.stringify(actual);
  const e = JSON.stringify(expected);
  if (a === e) {
    passed++;
    console.log(`  ok  ${name}`);
  } else {
    failed++;
    failures.push(`${name}\n      expected: ${e}\n      actual:   ${a}`);
    console.error(`FAIL  ${name}\n      expected: ${e}\n      actual:   ${a}`);
  }
}

function ok(name: string, cond: boolean, detail = 'condition false'): void {
  if (cond) {
    passed++;
    console.log(`  ok  ${name}`);
  } else {
    failed++;
    failures.push(`${name}\n      ${detail}`);
    console.error(`FAIL  ${name}\n      ${detail}`);
  }
}

/** Env self-management: save / set-or-delete / restore-in-finally. */
async function withEnv(key: string, value: string | undefined, body: () => Promise<void>): Promise<void> {
  const saved = Deno.env.get(key);
  try {
    if (value === undefined) Deno.env.delete(key);
    else Deno.env.set(key, value);
    await body();
  } finally {
    if (saved === undefined) Deno.env.delete(key);
    else Deno.env.set(key, saved);
  }
}

const FAKE_URL = 'https://whe-store-test.supabase.co';
const FAKE_KEY = 'whe-store-test-service-role-key';

/** Hand-computed: FM supabase-client.ts @ca4d280 throws a two-part message
 *  concatenated with a single space ("...edge function " + "secrets. Run: ...").
 *  Pinned BYTE-EXACT — the message names both env vars and the fix command
 *  (grep-ability is the contract; rewording breaks operator runbooks). */
const FM_MISSING_ENV_MSG =
  'SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set as edge function ' +
  'secrets. Run: supabase secrets set SUPABASE_URL=... SUPABASE_SERVICE_ROLE_KEY=...';

function expectThrow(name: string, body: () => unknown): void {
  try {
    body();
    ok(name, false, 'db() did NOT throw');
  } catch (e) {
    eq(name, (e as Error).message, FM_MISSING_ENV_MSG);
  }
}

// -----------------------------------------------------------------------------
// 1 — db() env semantics (FM verbatim). Registered FIRST: these pins require
// the singleton cache to still be cold, i.e. no earlier db() success in this
// process. (The static import instantiated the module, but the cache only
// fills on a successful CALL — registrations below are the first calls.)
// -----------------------------------------------------------------------------

Deno.test('whe_store db(): SUPABASE_URL missing => byte-exact FM throw (cache-cold ordering load-bearing)', async () => {
  // Hand-computed: with the key present but the URL unset, FM's `if (!url || !key)`
  // fires the SAME message — it never distinguishes which var is missing.
  await withEnv('SUPABASE_URL', undefined, async () => {
    await withEnv('SUPABASE_SERVICE_ROLE_KEY', FAKE_KEY, async () => {
      expectThrow('missing SUPABASE_URL throws the FM message byte-exact', () => db());
    });
  });
});

Deno.test('whe_store db(): SUPABASE_SERVICE_ROLE_KEY missing => byte-exact FM throw', async () => {
  await withEnv('SUPABASE_URL', FAKE_URL, async () => {
    await withEnv('SUPABASE_SERVICE_ROLE_KEY', undefined, async () => {
      expectThrow('missing SUPABASE_SERVICE_ROLE_KEY throws the FM message byte-exact', () => db());
    });
  });
});

Deno.test('whe_store db(): both env vars missing => byte-exact FM throw (env names unchanged from FM)', async () => {
  await withEnv('SUPABASE_URL', undefined, async () => {
    await withEnv('SUPABASE_SERVICE_ROLE_KEY', undefined, async () => {
      expectThrow('both missing throws the FM message byte-exact', () => db());
    });
  });
});

// -----------------------------------------------------------------------------
// 2 — factory-output shape (createClient CONSTRUCTS only; no builder is
// awaited => no network). This registration performs the FIRST successful
// db() call in the process and thereby fills the singleton cache.
// -----------------------------------------------------------------------------

Deno.test('whe_store db(): factory output shape — builder surface + FM-provenance x-client-info (offline)', async () => {
  await withEnv('SUPABASE_URL', FAKE_URL, async () => {
    await withEnv('SUPABASE_SERVICE_ROLE_KEY', FAKE_KEY, async () => {
      const client = db();
      ok('db() returns an object with .from', typeof client.from === 'function', 'no .from');
      // The engine's read chains use exactly this builder surface (and
      // nothing else): select -> eq / in / limit, terminated by maybeSingle.
      // r120 OPT-1: the paginated .range() window is RETIRED (chain #1 is now
      // the atomic rpc POST, pinned below); chains #2-#6 stay .from() GETs.
      const b = client.from('v_warehouse_directory').select('*', { count: 'exact' });
      const bb = b as unknown as Record<string, unknown>;
      for (const m of ['eq', 'in', 'limit', 'maybeSingle']) {
        ok(`builder .${m}() exists on the select builder`, typeof bb[m] === 'function', `.${m} missing`);
      }
      // The .from() read chains compose as GETs — the ONE POST in the engine
      // is chain #1's rpc (pinned below; supabase-js rpc() is method=POST).
      eq('.from() read builder composes as a GET (the ONE POST is chain #1 rpc)', (b as unknown as { method: string }).method, 'GET');
      // Client options copied verbatim from FM: our x-client-info passes
      // through under its exact lowercase key (supabase-js 2.117.2 ALSO adds
      // its own capitalized X-Client-Info — a distinct object key, unpinned).
      eq(
        'x-client-info option passes through VERBATIM (FM provenance string)',
        (client as unknown as { headers: Record<string, string> }).headers['x-client-info'],
        'fleet-manager/edge-fn',
      );
    });
  });
});

Deno.test('whe_store db(): singleton cache — two calls return the SAME client instance (per-isolate law)', async () => {
  // Hand-computed: FM caches `_client` module-globally; the second call must
  // be a cache hit returning the identical object (===), never a re-create.
  await withEnv('SUPABASE_URL', FAKE_URL, async () => {
    await withEnv('SUPABASE_SERVICE_ROLE_KEY', FAKE_KEY, async () => {
      const a = db();
      const b = db();
      ok('db() is a memoized singleton (a === b)', a === b, 'two distinct clients');
    });
  });
});

// -----------------------------------------------------------------------------
// 3 — static pins over the shell source (text-import; never executed — the
// shell wires deps at boot and calls Deno.serve, so a real import would need
// env + a listener. wh_entrypoint_test.ts precedent).
// -----------------------------------------------------------------------------

Deno.test('index.ts: the r116 rewire — db() sourced from whe_store, supabase-client fully gone', () => {
  // Hand-computed: line 35 of the shell is now exactly this import; no other
  // line references the old FM module name anywhere in the shell source.
  ok(
    'index.ts imports db from ../_shared/whe_store.ts',
    indexSrc.includes("import { db } from '../_shared/whe_store.ts';"),
    'import line absent or reworded',
  );
  ok(
    'index.ts has ZERO supabase-client references left',
    !indexSrc.includes('supabase-client'),
    'stale FM module reference survived',
  );
  ok(
    'index.ts constructs the client exactly ONCE (const client = db();)',
    indexSrc.split('= db()').length - 1 === 1,
    'client constructed more than once (or the wiring moved)',
  );
});

Deno.test('index.ts chains #1+#2: atomic rpc POST + version probe — byte-exact PostgREST fragments', () => {
  // Hand-computed from the shell source (readDirectory wiring):
  //   #1 atomic: ONE POST /rest/v1/rpc/wh_directory_atomic_read body {} —
  //              version+rows under one Postgres snapshot (r120 OPT-1; the
  //              paginated page GET is retired)
  //   #2 probe:  GET config  select=value  key=eq.warehouse_directory_version  maybeSingle
  //              (UNCHANGED — the replay path + /health still probe)
  ok('#1 client.rpc(wh_directory_atomic_read) wired', indexSrc.includes("client.rpc('wh_directory_atomic_read')"), 'fragment absent');
  ok('#2 .from(config)', indexSrc.includes(".from('config')"), 'fragment absent');
  ok('#2 .select(value)', indexSrc.includes(".select('value')"), 'fragment absent');
  ok('#2 .eq(key, warehouse_directory_version)', indexSrc.includes(".eq('key', 'warehouse_directory_version')"), 'fragment absent');
  ok('#2 .maybeSingle() terminator', indexSrc.includes('.maybeSingle()'), 'fragment absent');
});

Deno.test('index.ts chains #3+#4+#5: geo directory limit(2), geo_mode, warehouse_tables reference', () => {
  // Hand-computed from the shell source (geoReader / readGeoMode /
  // readTableReference wiring):
  //   #3 geo dir: GET v_geo_directory  select=*  limit=2  (the ≤1-row population)
  //   #4 mode:    GET config  select=value  key=eq.geo_mode  maybeSingle
  //   #5 tref:    GET warehouse_tables  select=is_reference  logical_name=eq.<table>  maybeSingle
  ok('#3 .from(v_geo_directory)', indexSrc.includes(".from('v_geo_directory')"), 'fragment absent');
  ok('#3 .select(*)', indexSrc.includes(".select('*')"), 'fragment absent');
  ok('#3 .limit(2)', indexSrc.includes('.limit(2)'), 'fragment absent');
  ok('#4 .eq(key, geo_mode)', indexSrc.includes(".eq('key', 'geo_mode')"), 'fragment absent');
  ok('#5 .from(warehouse_tables)', indexSrc.includes(".from('warehouse_tables')"), 'fragment absent');
  ok('#5 .select(is_reference)', indexSrc.includes(".select('is_reference')"), 'fragment absent');
  ok('#5 .eq(logical_name, table)', indexSrc.includes(".eq('logical_name', table)"), 'fragment absent');
});

Deno.test('index.ts chain #6: the fence read rides the SAME db() client (single store instance)', () => {
  // Hand-computed: the fence dep receives the identical `client` the five
  // read chains close over (the escape-hatch cast to WhFenceClient) — the
  // r49 G-W1 combined read pays NO second client and NO second env read.
  ok(
    '#6 fetchFenceConfig(client as unknown as WhFenceClient)',
    indexSrc.includes('fetchFenceConfig(client as unknown as WhFenceClient)'),
    'fence not wired over the shared client',
  );
  ok('#6 the shared client is db()', indexSrc.includes('const client = db();'), 'client no longer sourced from db()');
});

// -----------------------------------------------------------------------------
// 4 — dynamic wire shapes: the six chains composed on a REAL db() client.
// Build-only (no await) => createClient/fetch never fire. Every expectation
// below is HAND-COMPUTED from the PostgREST query-rendering laws listed in
// the header (observed on supabase-js 2.117.2; a lib drift fails loudly,
// which is the point of the battery).
// -----------------------------------------------------------------------------

Deno.test('whe_store: chain #1 rpc + chains #2-#5 compose the hand-computed PostgREST shapes (offline, build-only)', async () => {
  await withEnv('SUPABASE_URL', FAKE_URL, async () => {
    await withEnv('SUPABASE_SERVICE_ROLE_KEY', FAKE_KEY, async () => {
      const client = db();
      const href = (b: unknown) => String((b as unknown as { url: URL }).url);

      // #1 atomic rpc: supabase-js rpc() composes the ONE POST — the URL is
      // /rest/v1/rpc/<fn>, the method is already POST at build time, and the
      // no-args call rides the empty-args body ({} — stringified at await time).
      const rpcB = client.rpc('wh_directory_atomic_read');
      eq(
        '#1 atomic rpc => POST /rest/v1/rpc/wh_directory_atomic_read body {}',
        [String((rpcB as unknown as { url: URL }).url), (rpcB as unknown as { method: string }).method, (rpcB as unknown as { body: Record<string, never> }).body],
        [`${FAKE_URL}/rest/v1/rpc/wh_directory_atomic_read`, 'POST', {}],
      );
      // #2 version probe: eq renders key=eq.<value>; maybeSingle adds NO query
      // param (it flips the Accept header at await time + the build-time flag
      // pinned right after).
      eq(
        '#2 version => ?select=value&key=eq.warehouse_directory_version',
        href(client.from('config').select('value').eq('key', 'warehouse_directory_version').maybeSingle()),
        `${FAKE_URL}/rest/v1/config?select=value&key=eq.warehouse_directory_version`,
      );
      eq(
        '#2 maybeSingle is a BUILD-TIME flag (isMaybeSingle=true)',
        (client.from('config').select('value').eq('key', 'warehouse_directory_version').maybeSingle() as unknown as { isMaybeSingle: boolean }).isMaybeSingle,
        true,
      );
      // #3 geo directory: the pinned fixed limit(2).
      eq(
        '#3 geo => ?select=*&limit=2',
        href(client.from('v_geo_directory').select('*').limit(2)),
        `${FAKE_URL}/rest/v1/v_geo_directory?select=*&limit=2`,
      );
      // #4 geo mode.
      eq(
        '#4 mode => ?select=value&key=eq.geo_mode',
        href(client.from('config').select('value').eq('key', 'geo_mode').maybeSingle()),
        `${FAKE_URL}/rest/v1/config?select=value&key=eq.geo_mode`,
      );
      // #5 table reference (table = 'orders' as the concrete stand-in for the
      // shell's `table` argument; eq renders logical_name=eq.orders).
      eq(
        '#5 tref(orders) => ?select=is_reference&logical_name=eq.orders',
        href(client.from('warehouse_tables').select('is_reference').eq('logical_name', 'orders').maybeSingle()),
        `${FAKE_URL}/rest/v1/warehouse_tables?select=is_reference&logical_name=eq.orders`,
      );
    });
  });
});

// -----------------------------------------------------------------------------
// 5 — the fence read (chain #6) through a recording fake: the ONE combined
// config read, awaited EXACTLY once (the fake's own thenable — still zero
// network), with the .in() membership list pinned in order.
// -----------------------------------------------------------------------------

interface FenceCallLog {
  from: string[];
  select: string[];
  in: [string, string[]][];
  awaited: number;
}

/** Recording fake of the WhFenceClient builder surface (geo_write_fence_test
 *  precedent): records every builder call, resolves via the controlled
 *  thenable. */
function recordingFenceClient(
  rows: { key: string; value: unknown }[] | null,
  log: FenceCallLog,
): WhFenceClient {
  const result: WhFenceQueryResult = { data: rows, error: null };
  const thenable: PromiseLike<WhFenceQueryResult> = {
    then<TResult1 = WhFenceQueryResult, TResult2 = never>(
      onFul?: ((value: WhFenceQueryResult) => TResult1 | PromiseLike<TResult1>) | null,
      onRej?: ((reason: unknown) => TResult2 | PromiseLike<TResult2>) | null,
    ): PromiseLike<TResult1 | TResult2> {
      log.awaited++;
      return Promise.resolve(result).then(onFul, onRej);
    },
  };
  return {
    from(table: string) {
      log.from.push(table);
      return {
        select(cols: string) {
          log.select.push(cols);
          return {
            in(col: string, values: readonly string[]) {
              log.in.push([col, [...values]]);
              return thenable;
            },
          };
        },
      };
    },
  };
}

Deno.test('whe_store: fence read — ONE round trip, .in(key, FENCE_CONFIG_KEYS) membership pinned (recording fake)', async () => {
  // Hand-computed call log for geo_write_fence.fetchFenceConfig — the engine
  // hands it the db() client (pinned statically above), so this log IS the
  // wire shape of chain #6:
  //   from('config') -> select('key,value') -> in('key', [the 3 fence keys])
  const log: FenceCallLog = { from: [], select: [], in: [], awaited: 0 };
  const out = await fetchFenceConfig(recordingFenceClient(
    [
      { key: 'geo_read_only', value: false },
      { key: 'geo_write_epoch', value: 7 },
      { key: 'geo_primary_override', value: 'otherref' },
    ],
    log,
  ));
  eq('exactly ONE awaited round trip (G-W1: one combined read)', log.awaited, 1);
  eq('one from() against config', log.from, ['config']);
  eq('one select() of key,value (no spaces — the exact fence read)', log.select, ['key,value']);
  eq(
    'one .in() — col "key" with the THREE fence keys in pinned order',
    log.in,
    [['key', ['geo_read_only', 'geo_write_epoch', 'geo_primary_override']]],
  );
  eq('FENCE_CONFIG_KEYS membership pin (the exact .in() list)', [...FENCE_CONFIG_KEYS], ['geo_read_only', 'geo_write_epoch', 'geo_primary_override']);
  eq('rows map through the same chain to raw values', out, {
    ok: true,
    values: { read_only: false, write_epoch: 7, primary_override: 'otherref' },
  });

  // The same fence chain composed on the REAL db() client (build-only — the
  // in-list renders with URL-encoded commas/parens in href). Hand-computed:
  // commas -> %2C, parens -> %28/%29.
  await withEnv('SUPABASE_URL', FAKE_URL, async () => {
    await withEnv('SUPABASE_SERVICE_ROLE_KEY', FAKE_KEY, async () => {
      const b = db().from('config').select('key,value').in('key', FENCE_CONFIG_KEYS);
      eq(
        '#6 fence on the real client => the exact in-list URL',
        String((b as unknown as { url: URL }).url),
        `${FAKE_URL}/rest/v1/config?select=key%2Cvalue&key=in.%28geo_read_only%2Cgeo_write_epoch%2Cgeo_primary_override%29`,
      );
    });
  });
});

// -----------------------------------------------------------------------------
// 6 — module purity self-pin: whe_store.ts imports NOTHING beyond
// npm:@supabase/supabase-js@2 (+ its type) and none of the FM helpers that
// were deliberately left behind (see the whe_store.ts docblock).
// -----------------------------------------------------------------------------

Deno.test('whe_store purity: the only module specifier is npm:@supabase/supabase-js@2; no FM helper signatures', () => {
  // Hand-computed: the file has exactly ONE from-import (the createClient +
  // SupabaseClient type line) and ZERO relative imports.
  const specs = [...storeSrc.matchAll(/from\s+'([^']+)'/g)].map((m) => m[1]);
  eq('module specifier set', specs, ['npm:@supabase/supabase-js@2']);
  ok('no relative imports at all', !storeSrc.includes("from './") && !storeSrc.includes("from '../"), 'a _shared dependency crept in');

  // Provenance markers survive (the extraction contract is auditable from the
  // file alone).
  ok('provenance: FM @ca4d280 named', storeSrc.includes('@ca4d280'), 'marker missing');
  ok('provenance: consumer-store seam named', storeSrc.includes('consumer-store seam'), 'marker missing');
  ok('provenance: supabase-client.ts origin named', storeSrc.includes('supabase-client.ts'), 'marker missing');
  ok('the seam export exists (export function db())', storeSrc.includes('export function db()'), 'db() missing');

  // Non-ported FM helpers: their CODE signatures must be absent (mentioning
  // them in the docblock's NOT-ported list is fine; defining them is not).
  for (const sig of [
    'export async function getConfig',
    'export async function setConfig',
    'export function configJwtSkewVerdict',
    'export function readServiceJwtClaims',
    'export function configPatchValueError',
    'export async function bumpQuota',
    'export async function audit',
    'const CONFIG_CACHE_TTL_MS',
    "rpc('bump_quota'",
    "from('audit_log')",
  ]) {
    ok(`non-ported helper absent: ${sig}`, !storeSrc.includes(sig), 'FM helper was ported against the r116 contract');
  }
});

Deno.test('whe_store auth options: persistSession/autoRefreshToken OFF — byte-exact source pin (r116 audit F-2)', () => {
  // Hand-computed expectation (audit probe 2026-10-04): flipping either flag
  // to true in whe_store.ts was NOT caught by any existing pin — the auth
  // options block was the only unpinned runtime behavior. The FM contract is
  // edge-fn semantics: NO session persistence, NO token auto-refresh (the
  // service-role key never expires; a persisted session would leak scope and
  // burn memory per isolate). These pins go RED on either flip, naming the
  // flag. Byte-exact fragments, not regex — mutation-lethal by construction.
  const expectations: Array<[string, string]> = [
    ['persistSession: false pinned (session scope never persisted)', 'persistSession: false'],
    ['autoRefreshToken: false pinned (service-role key never refreshes)', 'autoRefreshToken: false'],
    ['auth options block shape pinned (auth: { ... } carrier)', 'auth: { persistSession: false, autoRefreshToken: false }'],
  ];
  for (const [label, fragment] of expectations) {
    ok(label, storeSrc.includes(fragment), `fragment gone from whe_store.ts: ${fragment}`);
  }
});

// CLOSING GUARD — the island-harness idiom (wh_entrypoint_test.ts end-of-file
// meta-test). Without this, ok()/eq() failures accumulate into `failures[]`
// and are silently swallowed: every assertion in this file would be vacuous
// (r116 audit: the persistSession mutation probe went GREEN against a file
// whose pins "failed" invisibly — Leg B shipped the accumulator without the
// throw). Deno runs a file's registrations in order, so this executes last.
Deno.test('whe_store_test: assertion ledger — no silently-swallowed failures', () => {
  console.log(`\n${passed} passed, ${failed} failed`);
  if (failed > 0) {
    console.error('FAILURES:\n' + failures.map((f) => `  * ${f}`).join('\n'));
    throw new Error(`${failed} whe_store pin(s) failed`);
  }
});
