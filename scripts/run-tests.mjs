#!/usr/bin/env node
// scripts/run-tests.mjs — `make test` entrypoint with a LOUD deno detector.
//
// Law: NEVER silent-skip. A missing deno must NEVER look like a green run
// (green-when-unverified is banned). If `deno` is on PATH, run the exact
// pinned battery command and propagate its exit code. If it is ABSENT,
// print a loud banner naming the exact install command and exit 1.
//
// The pinned battery (repo root cwd, verbatim from README.md / CI):
//   deno test --no-check -q --allow-env --allow-read
//
// Deno itself is required for the battery and for `deno check` — but NOT for
// `supabase functions deploy --use-api` (that bundles server-side; see
// DEPLOY.md).

import { spawn, spawnSync } from 'node:child_process';

const DENO_TEST_ARGS = ['test', '--no-check', '-q', '--allow-env', '--allow-read'];
const INSTALL_HINT = 'curl -fsSL https://deno.land/x/install/install.sh | sh';

function loudFailNoDeno() {
  console.error('='.repeat(78));
  console.error('ERROR: `deno` was not found on PATH — THE TEST BATTERY DID NOT RUN.');
  console.error('');
  console.error('This repo refuses to report a green result it did not earn:');
  console.error('no deno => no tests executed => exit 1 (never a silent skip).');
  console.error('');
  console.error('Install Deno 2.x, then re-run:');
  console.error(`    ${INSTALL_HINT}`);
  console.error('');
  console.error('Verify with: deno --version');
  console.error('Note: deno is required for `make test` and `make check`, but NOT for');
  console.error('`supabase functions deploy --use-api` (server-side bundling — DEPLOY.md).');
  console.error('='.repeat(78));
  process.exit(1);
}

// 1) Detect deno.
const probe = spawnSync('deno', ['--version'], { encoding: 'utf8' });
if (probe.error && probe.error.code === 'ENOENT') loudFailNoDeno();
if (probe.error) {
  console.error(`ERROR: probing \`deno --version\` failed: ${probe.error.message}`);
  process.exit(1);
}
if (probe.status !== 0) {
  console.error(`ERROR: \`deno --version\` exited ${probe.status} — deno is present but broken.`);
  console.error(`Reinstall Deno 2.x:  ${INSTALL_HINT}`);
  process.exit(1);
}

// 2) Run the pinned battery with inherited stdio, propagating the exit code.
const version = (probe.stdout || '').split('\n')[0].trim();
console.error(`run-tests: deno detected (${version})`);
console.error(`run-tests: running the offline battery from repo root: deno ${DENO_TEST_ARGS.join(' ')}`);
const child = spawn('deno', DENO_TEST_ARGS, { stdio: 'inherit' });
child.on('error', (err) => {
  console.error(`ERROR: failed to spawn deno: ${err.message}`);
  process.exit(1);
});
child.on('exit', (code, signal) => {
  if (signal) {
    console.error(`run-tests: battery terminated by signal ${signal}`);
    process.exit(1);
  }
  process.exit(code ?? 1);
});
