#!/usr/bin/env bash
# =============================================================================
# scripts/deploy_gate.sh — the r124 A8 DEPLOY GATE (design_r124_opt3_a8.md §2,
# audit B ⟫B-2).
# =============================================================================
# One implementation, two entry points: `make deploy-gate` (the checker) and
# `make deploy` (which re-runs the gate as a prerequisite — belt-and-braces,
# so the design's `make stamp && make deploy-gate && make deploy` leg runs it
# twice). The gate exists because `supabase functions deploy --use-api`
# bundles SERVER-side but uploads the WORKING TREE source: a wrong checkout,
# a dirty tree, or a stale stamp must never reach the edge.
#
# FIVE ordered checks, each failing with ONE fixed die() message (the fm
# deploy.sh fail-fast style is the precedent). Deliberately NOT mirrored from
# fm: the FLEET_TOKEN fallback (whe DEPLOY.md deliberately dropped fallbacks)
# and raw-bash ${VAR:?} leaks (the r118 T-3 lesson — the token guard lives
# HERE, in-script, never in a make recipe).
#
# Exit 0 prints the design-pinned PASS line:
#   branch main @ <sha7> == origin/main, clean, stamp ok
# =============================================================================

die() { echo "deploy-gate REFUSED: $1" >&2; exit 1; }

# (0) identity — everything below speaks in the HEAD sha7.
sha7="$(git rev-parse --short=7 HEAD 2>/dev/null)" || die "not a git worktree (git rev-parse failed) — run the gate from the repo checkout"

# (1) on-branch main — a detached HEAD dies here too (abbrev-ref prints
# 'HEAD' when detached; a feature branch is refused outright).
branch="$(git rev-parse --abbrev-ref HEAD 2>/dev/null)" || die "cannot resolve the current branch (git rev-parse --abbrev-ref failed)"
[ "$branch" = "main" ] || die "branch is '$branch', not main — deploys are main-only"

# (2) clean working tree — the deploy uploads the working tree, so any
# uncommitted byte would silently ride to the edge.
[ -z "$(git status --porcelain)" ] || die "working tree not clean (git status --porcelain non-empty) — commit or stash before deploying"

# (3) fetch origin main, then HEAD == origin/main. A failed fetch is a REFUSAL
# (never deploy on unverified refs — the deploy needs network anyway).
git fetch origin main --quiet || die "git fetch origin main failed — never deploy on unverified refs"
head_full="$(git rev-parse HEAD 2>/dev/null)" || die "cannot resolve HEAD"
origin_full="$(git rev-parse origin/main 2>/dev/null)" || die "cannot resolve origin/main after fetch"
[ "$head_full" = "$origin_full" ] || die "HEAD $(git rev-parse --short=7 HEAD) != origin/main $(git rev-parse --short=7 origin/main) — pull/rebase before deploying"

# (4) stamp fresh — the generated _shared/engine_build.ts must exist AND
# carry the EXACT current HEAD sha7 (catches the stale-generated-file class
# in the same round; the fix is always `make stamp`).
stamp_file="supabase/functions/_shared/engine_build.ts"
[ -f "$stamp_file" ] || die "engine_build stamp stale/absent — run make stamp"
[ "$(cat "$stamp_file")" = "export const ENGINE_BUILD = \"$sha7\";" ] || die "engine_build stamp stale/absent — run make stamp"

# (5) token non-empty — checked in-script (never ${:?} raw-bash, r118 T-3).
[ -n "${SUPABASE_ACCESS_TOKEN:-}" ] || die "SUPABASE_ACCESS_TOKEN is empty/unset — export the deploy token before running make deploy"

echo "branch main @ $sha7 == origin/main, clean, stamp ok"
