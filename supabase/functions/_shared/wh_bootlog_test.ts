// =============================================================================
// _shared/wh_bootlog_test.ts — r131 D2 boot-log consolidation battery
// (design_d2_bootlog_consolidation_r129.md §3.4).
// =============================================================================
// Lethal, hand-computed, offline. The merged wh_boot line's EXACT bytes get
// their lethal arm here — the byte-pinnable surface the prose literals never
// had (AM-11 doctrine: the pure formatter is pinned without stdout
// scraping). Every expected string below is HAND-COMPUTED and independently
// verified (python json.dumps, compact separators, ASCII-only):
//   worst-case 8-defect line = 239 B  (design §4 confirmed: 1,140 B -> 239 B,
//                                      −79.0%)
//   armed defect-free line   =  92 B  (design §4 said 93 — recount + python
//                                      confirm 92; −46.2% on 171 B)
//   single stamp_absent line = 107 B  (design §4 said 108 — see above)
// No env, no console, no fetch, no fs — the module under test is pure.
// =============================================================================

import { formatWhBootLog, shouldEmitWhBootLog } from './wh_bootlog.ts';
import type { WhBootDefectCode } from './wh_bootlog.ts';

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

/** The 8 defect codes in index.ts BRANCH ORDER (the merged line's
 *  deterministic worst-case payload — design §2.3 code table). */
const WORST_8_CODES: WhBootDefectCode[] = [
  'ownref_unparsed',
  'ownkey_empty',
  'stamp_absent',
  'proxy_token_unset',
  'proxy_kv_timeout',
  'proxy_kv_absent',
  'proxy_ownref_unparsed',
  'proxy_kv_failed',
];

// Hand-computed expected lines (byte counts verified; see the header note):
const WORST_8_LINE =
  '{"event":"wh_boot","ts_source":"platform","class":"boot_defect","lever":"inert","defects":["ownref_unparsed","ownkey_empty","stamp_absent","proxy_token_unset","proxy_kv_timeout","proxy_kv_absent","proxy_ownref_unparsed","proxy_kv_failed"]}'; // 239 B
const ARMED_LINE =
  '{"event":"wh_boot","ts_source":"platform","class":"boot_armed","lever":"armed","defects":[]}'; // 92 B
const SINGLE_LINE =
  '{"event":"wh_boot","ts_source":"platform","class":"boot_defect","lever":"inert","defects":["stamp_absent"]}'; // 107 B

const byteLen = (s: string): number => new TextEncoder().encode(s).length;

Deno.test('wh_bootlog: the ONE merged wh_boot line (r131 D2, design §3.4)', () => {
  // (a) worst-case 8-defect boot — the exact 239 B line (design §4's
  //     1,140 B -> 239 B, −79.0%).
  eq('worst-case 8-defect line is the hand-computed 239 B string (byte-exact)', formatWhBootLog({ lever: 'inert', defects: WORST_8_CODES }), WORST_8_LINE);
  eq('worst-case line is exactly 239 UTF-8 bytes', byteLen(formatWhBootLog({ lever: 'inert', defects: WORST_8_CODES })), 239);

  // (b) the armed defect-free boot — the folded :225 success line, the
  //     steady-state emitter (171 B -> 92 B, −46.2%).
  eq('armed defect-free line is the hand-computed 92 B string (byte-exact)', formatWhBootLog({ lever: 'armed', defects: [] }), ARMED_LINE);
  eq('armed line is exactly 92 UTF-8 bytes', byteLen(formatWhBootLog({ lever: 'armed', defects: [] })), 92);

  // (c) single-defect boot — the r124 fresh-clone/make-stamp case (114 B -> 107 B).
  eq('single stamp_absent line is the hand-computed 107 B string (byte-exact)', formatWhBootLog({ lever: 'inert', defects: ['stamp_absent'] }), SINGLE_LINE);
  eq('single-defect line is exactly 107 UTF-8 bytes', byteLen(formatWhBootLog({ lever: 'inert', defects: ['stamp_absent'] })), 107);

  // (d) key order + compactness: JSON.parse round-trip + the fixed head.
  eq('JSON.parse round-trips the worst-case line to the exact payload', JSON.parse(formatWhBootLog({ lever: 'inert', defects: WORST_8_CODES })), {
    event: 'wh_boot',
    ts_source: 'platform',
    class: 'boot_defect',
    lever: 'inert',
    defects: WORST_8_CODES,
  });
  eq('key order is FIXED (event, ts_source, class, lever, defects)', Object.keys(JSON.parse(formatWhBootLog({ lever: 'armed', defects: [] }))), ['event', 'ts_source', 'class', 'lever', 'defects']);
  ok('the line opens with the fixed event/ts_source head (compact separators — no spaces)', ARMED_LINE.startsWith('{"event":"wh_boot","ts_source":"platform"') && !ARMED_LINE.includes(', ') && !ARMED_LINE.includes(': '), 'head or compactness drifted');

  // (e) determinism + branch order.
  eq('deterministic: the same input renders byte-identical output', formatWhBootLog({ lever: 'inert', defects: WORST_8_CODES }), formatWhBootLog({ lever: 'inert', defects: [...WORST_8_CODES] }));
  eq('defects keep the caller branch order (never sorted/deduped)', JSON.parse(formatWhBootLog({ lever: 'inert', defects: ['proxy_kv_absent', 'ownref_unparsed'] })), {
    event: 'wh_boot',
    ts_source: 'platform',
    class: 'boot_defect',
    lever: 'inert',
    defects: ['proxy_kv_absent', 'ownref_unparsed'],
  });

  // (f) echo law: every union code is a fixed lowercase token — no
  //     interpolation carrier can ever ride the line (AM-8).
  ok(
    'echo law: all 8 codes match ^[a-z0-9_]{1,24}$ (compile-time literals, never value fragments)',
    WORST_8_CODES.length === 8 && WORST_8_CODES.every((c) => /^[a-z0-9_]{1,24}$/.test(c)),
    'a code grew outside the fixed-token shape (interpolation carrier risk)',
  );

  // (g) shouldEmitWhBootLog truth table — the clean inert boot emits
  //     NOTHING (byte-identical 0-line path); armed always emits.
  eq('gate: inert + no defects emits NOTHING (the 0-line path)', shouldEmitWhBootLog({ lever: 'inert', defects: [] }), false);
  eq('gate: armed + no defects emits (the folded :225 armed line)', shouldEmitWhBootLog({ lever: 'armed', defects: [] }), true);
  eq('gate: inert + a defect emits', shouldEmitWhBootLog({ lever: 'inert', defects: ['stamp_absent'] }), true);
  eq('gate: armed + defects emits (defects dominate => boot_defect/error severity)', shouldEmitWhBootLog({ lever: 'armed', defects: ['ownkey_empty'] }), true);

  // Ledger (the island-harness closing guard): no silently-swallowed
  // failures — this single block IS the design's "1 new block".
  console.log(`\nwh_bootlog: ${passed} passed, ${failed} failed`);
  if (failed > 0) {
    console.error('FAILURES:\n' + failures.map((f) => `  * ${f}`).join('\n'));
    throw new Error(`${failed} wh_bootlog pin(s) failed`);
  }
});
