#!/usr/bin/env python3
"""r133_tier2_oracle.py -- OFFLINE tier-2 oracle PROJECTION generator (stdlib only).

Task: maxxing-r133-q7b-battery (work item 1). Implements the contract of
research/design_r132_w7_family.md §5 (the W7 oracle = a sha-gated PROJECTION
of the banked join oracle) and §6.4 (the E16_tier2 fixture schema). The
derivation is deterministic from the BANKED artifact
audit/r128_join_oracle/join_oracle.json — the corpus is NOT re-read, the
corpus shas are re-gated from the banked meta (discipline mirror of
scripts/r129_join_oracle.py: sha-gated, offline-gated BEFORE any emit).

Contract pins implemented here (design §5, all audit-B/census-triple-confirmed):
  - projection rule: tier(region) = (k % 3) + 1 where k = int(region[1:])
    (the banked dim spec meta.dim.tier_rule — verified BEFORE trusting rows);
    keep tier == 2  ⇔  k ≡ 1 (mod 3)  →  EXACTLY 17 rows, region-ascending
    (g01, g04, …, g49; g47 is tier-3 AND banked-excluded — tier-2 loses NO
    region to the g47 omission);
  - hand pins (independent of any run): Σx_t2 = 22,264,985 / Σc_t2 = 4,711 /
    Σn_t2 = 4,760; n_r = 280 ∀17 (⇒ Σn_t2 = 17 × 280 independent re-derivation);
  - per-band Σx_t2 / n pins (the E16 projection home, mapping recorded — the
    banked per_band is PER-REGION → PER-BAND, so the projection SUMS each
    band's x/n across the 17 tier-2 regions): A {n 2040, Σx 9,506,717} /
    B {n 1360, Σx 6,438,027} / C {n 1360, Σx 6,320,241};
  - set-inclusion: the 17 region keys ⊂ the 49 banked keys, per-region
    {x,c,n} byte-identical (a tier-predicate-drop mutant emits 49 rows →
    row-count AND set-difference RED);
  - closure: t1+t3 complement (banked − t2) = {Σx 42,256,498, Σc 8,867,
    Σn 8,960} and per-band A+B+C reassembles the t2 globals — ALL EXACT.

Emits (AFTER every gate passes — abort non-zero on ANY failure):
  (a) audit/r133_tier2_oracle/tier2_oracle.json — full projection + gates +
      shas (the /home/z/my-project audit artifact);
  (b) supabase/functions/_shared/wh_fixtures/E16_tier2.json — the battery
      fixture, E15_join.json formatting conventions (2-space indent, one-line
      row objects, one-line leaf objects, trailing newline).

Runs fully offline BEFORE any network (r124_ladder build_oracle() convention).
"""

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timezone

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Banked INPUT of record (r128/r129; OUTSIDE the kit — the canonical oracle
# artifact the projection derives from). Its sha256 IS the E16 meta.source.
DEFAULT_ORACLE = "/home/z/my-project/audit/r128_join_oracle/join_oracle.json"
DEFAULT_OUT_AUDIT = "/home/z/my-project/audit/r133_tier2_oracle/tier2_oracle.json"
DEFAULT_OUT_FIXTURE = os.path.join(
    REPO, "supabase", "functions", "_shared", "wh_fixtures", "E16_tier2.json")
# The banked E15 port (the 49-row join fixture) — provenance cross-gate.
DEFAULT_E15_FIXTURE = os.path.join(
    REPO, "supabase", "functions", "_shared", "wh_fixtures", "E15_join.json")
GENERATOR = "scripts/r133_tier2_oracle.py"

# Pinned corpus hashes (design_r132_w7_family §5: "corpus shas re-gated" —
# the projection re-asserts the BANKED corpus provenance from the artifact's
# meta; the full pinned hashes are the r129/audit-B constants).
PINNED_A4_SHA256 = "28407026b54ee7e1887457d1ba42afc32d830292acae7ae35dff6eed0410da4c"
PINNED_BANDC_SHA256 = "a022e3ee313304a02863911934aaf19c04a9874d0357e37cab4d12112deb819d"

# Banked dim spec (verified against the artifact's meta BEFORE trusting rows).
DIM_TABLE = "wh_probe_dim"
DIM_EXCLUDED = "g47"
TIER_RULE = "(k % 3) + 1 where k = region number"

# Banked 49-row globals (design_r128_joinplans §4; audit-B confirmed).
BANKED_GLOBALS = {"rows": 49, "sum_c": 13578, "sum_n": 13720, "sum_x": 64521483}
BANKED_G47 = {"x": 1374335, "c": 277, "n": 280}

# Design §5 hand pins for the tier-2 projection (independent of any run —
# asserted unconditionally; lethal-method law).
EXPECTED_T2_ROWS = 17
EXPECTED_T2_REGIONS = ["g%02d" % k for k in range(1, 50, 3)]  # k ≡ 1 (mod 3)
N_R_UNIFORM = 280
T2_GLOBALS = {"rows": 17, "sum_x": 22264985, "sum_c": 4711, "sum_n": 4760}
T2_PER_BAND = {  # {band: {n, sum_x}} — the E16 per_band projection home
    "A": {"n": 2040, "sum_x": 9506717},
    "B": {"n": 1360, "sum_x": 6438027},
    "C": {"n": 1360, "sum_x": 6320241},
}
# Closure residuals (banked − tier2 = tiers {1,3} combined; hand-derived from
# the §5 closure identities t1+t2+t3 = banked).
COMPLEMENT = {"sum_x": 64521483 - 22264985, "sum_c": 13578 - 4711,
              "sum_n": 13720 - 4760}


class OracleAbort(Exception):
    pass


def die(msg):
    raise OracleAbort(msg)


def sha256_file(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


def tier_of(region):
    """The banked dim spec: tier = (k % 3) + 1, k = region number."""
    return (int(region[1:]) % 3) + 1


def fmt_one_line(obj):
    """E15 house style for one-line leaf objects: `{ "a": 1, "b": 2 }`."""
    body = json.dumps(obj)
    return "{" + body[1:-1] + "}"


def render_e16(fixture):
    """Render the E16 fixture EXACTLY in E15_join.json's formatting
    conventions: 2-space indent; meta as one-key-per-line; rows as one-line
    objects; excluded/globals/per_band multi-line with one-line leaves;
    trailing newline. Returns the byte string (validated before writing)."""
    m = fixture["meta"]
    lines = []
    lines.append("{")
    lines.append('  "id": %s,' % json.dumps(fixture["id"]))
    lines.append('  "meta": {')
    lines.append('    "fixture": %s,' % json.dumps(m["fixture"], ensure_ascii=False))
    lines.append('    "source": %s,' % json.dumps(m["source"], ensure_ascii=False))
    lines.append('    "doc": %s' % json.dumps(m["doc"], ensure_ascii=False))
    lines.append('  },')
    lines.append('  "rows": [')
    for i, r in enumerate(fixture["rows"]):
        row = ('{ "region": %s, "x": %d, "c": %d, "n": %d }'
               % (json.dumps(r["region"]), r["x"], r["c"], r["n"]))
        lines.append("    " + row + ("," if i < len(fixture["rows"]) - 1 else ""))
    lines.append('  ],')
    lines.append('  "excluded": {')
    lines.append('    "note": %s' % json.dumps(fixture["excluded"]["note"], ensure_ascii=False))
    lines.append('  },')
    lines.append('  "globals": {')
    lines.append('    "rows": %d,' % fixture["globals"]["rows"])
    lines.append('    "sum_c": %d,' % fixture["globals"]["sum_c"])
    lines.append('    "sum_n": %d,' % fixture["globals"]["sum_n"])
    lines.append('    "sum_x": %d' % fixture["globals"]["sum_x"])
    lines.append('  },')
    lines.append('  "per_band": {')
    bands = list(fixture["per_band"].items())
    for i, (b, v) in enumerate(bands):
        leaf = '{ "n": %d, "sum_x": %d }' % (v["n"], v["sum_x"])
        lines.append('    "%s": %s%s' % (b, leaf, "," if i < len(bands) - 1 else ""))
    lines.append('  }')
    lines.append("}")
    return ("\n".join(lines) + "\n").encode("utf-8")


def main():
    ap = argparse.ArgumentParser(description="r133 offline tier-2 oracle projection generator")
    ap.add_argument("--oracle", default=DEFAULT_ORACLE,
                    help="the banked join_oracle.json of record (r128/r129)")
    ap.add_argument("--e15", default=DEFAULT_E15_FIXTURE,
                    help="the banked E15_join.json fixture (provenance cross-gate)")
    ap.add_argument("--out-audit", default=DEFAULT_OUT_AUDIT,
                    help="output tier2_oracle.json audit artifact path")
    ap.add_argument("--out-fixture", default=DEFAULT_OUT_FIXTURE,
                    help="output E16_tier2.json battery fixture path")
    args = ap.parse_args()

    checks = []

    def ok(name, detail=""):
        checks.append(f"PASS {name}" + (f" — {detail}" if detail else ""))
        print(f"  [PASS] {name}" + (f" — {detail}" if detail else ""))

    # ---- 1. Input integrity: sha256 + meta-field gates BEFORE trusting rows
    print("== [1] input integrity: sha256 + meta-field gates ==")
    if not os.path.isfile(args.oracle):
        die(f"banked oracle missing: {args.oracle}")
    oracle_sha = sha256_file(args.oracle)
    ok("join_oracle.json sha256 (recorded → E16 meta.source)", oracle_sha)
    with open(args.oracle) as f:
        oracle = json.load(f)

    meta = oracle.get("meta", {})
    dim = meta.get("dim", {})
    if dim.get("tier_rule") != TIER_RULE:
        die(f"meta.dim.tier_rule {dim.get('tier_rule')!r} != {TIER_RULE!r} "
            "(the projection rule is untrustable — refusing)")
    if dim.get("table") != DIM_TABLE:
        die(f"meta.dim.table {dim.get('table')!r} != {DIM_TABLE!r}")
    if dim.get("excluded_region") != DIM_EXCLUDED:
        die(f"meta.dim.excluded_region {dim.get('excluded_region')!r} != {DIM_EXCLUDED!r}")
    if not isinstance(dim.get("rows"), list) or len(dim["rows"]) != 49:
        die(f"meta.dim.rows expected 49 banked dim rows, got {len(dim.get('rows', []))}")
    ok("meta.dim spec", f"table={DIM_TABLE} excluded={DIM_EXCLUDED} "
                        f"rows=49 tier_rule={TIER_RULE!r}")
    corpus = meta.get("corpus", [])
    if [c.get("sha256") for c in corpus] != [PINNED_A4_SHA256, PINNED_BANDC_SHA256]:
        die(f"meta.corpus shas {[c.get('sha256') for c in corpus]} != the pinned "
            "28407026…da4c / a022e3ee…819d (banked corpus provenance re-gate FAILED)")
    ok("meta.corpus shas re-gated", "a4_dataset 28407026…da4c + band_c a022e3ee…819d "
                                    "(14k corpus unchanged — design §5)")

    # ---- 2. Banked-structure gates (rows/globals/per_band shape) ----------
    print("== [2] banked-structure gates ==")
    out_rows = oracle.get("output_rows", [])
    if len(out_rows) != BANKED_GLOBALS["rows"]:
        die(f"banked output_rows {len(out_rows)} != {BANKED_GLOBALS['rows']}")
    if oracle.get("globals") != BANKED_GLOBALS:
        die(f"banked globals {oracle.get('globals')} != {BANKED_GLOBALS}")
    regions = [r["region"] for r in out_rows]
    if regions != sorted(regions) or len(set(regions)) != len(regions):
        die("banked output_rows not strictly region-ascending / has dupes")
    if DIM_EXCLUDED in regions:
        die("banked output_rows contains g47 — not the banked artifact")
    ok("banked output_rows", f"49 regions, g47 ABSENT, region-ascending")
    banked_by_region = {r["region"]: r for r in out_rows}
    per_band = oracle.get("per_band", {})
    if sorted(per_band) != sorted(regions):
        die(f"banked per_band keys ({len(per_band)}) != the 49 output regions")
    ok("banked per_band", "per-REGION → per-band {A,B,C} structure, 49 keys "
                          "(the projection mapping adapts THIS structure — recorded below)")
    g47 = oracle.get("excluded", {}).get(DIM_EXCLUDED)
    if g47 != BANKED_G47:
        die(f"banked excluded g47 {g47} != {BANKED_G47} (not the banked artifact)")
    ok("banked excluded g47 mutant-catcher", str(BANKED_G47))

    # ---- 3. E15 provenance cross-gate (the 49-row fixture port) -----------
    print("== [3] E15 provenance cross-gate ==")
    if not os.path.isfile(args.e15):
        die(f"E15 fixture missing: {args.e15}")
    with open(args.e15) as f:
        e15 = json.load(f)
    if e15.get("id") != "E15_join" or e15.get("meta", {}).get("fixture") != "E15":
        die(f"E15 fixture id/fixture {e15.get('id')!r}/{e15.get('meta', {}).get('fixture')!r} "
            "do not match the banked port ('E15_join'/'E15')")
    if e15.get("globals") != BANKED_GLOBALS:
        die("E15 fixture globals do not match the banked constants")
    e15_rows = [{k: r[k] for k in ("region", "x", "c", "n")} for r in e15["rows"]]
    if e15_rows != out_rows:
        die("E15.rows != the banked output_rows (the port drifted)")
    ok("E15_join.json", "globals == banked AND rows == the 49 banked output_rows "
                        "(the banked port is current — set-inclusion ground truth)")

    # ---- 4. The tier-2 projection ------------------------------------------
    print("== [4] tier-2 projection (tier == 2, k ≡ 1 mod 3) ==")
    t2_rows = [dict(r) for r in out_rows if tier_of(r["region"]) == 2]
    t2_rows.sort(key=lambda r: r["region"])
    if [r["region"] for r in t2_rows] != EXPECTED_T2_REGIONS:
        die(f"tier-2 regions {[r['region'] for r in t2_rows]} != {EXPECTED_T2_REGIONS}")
    if len(t2_rows) != EXPECTED_T2_ROWS:
        die(f"tier-2 rows {len(t2_rows)} != {EXPECTED_T2_ROWS}")
    ok("tier-2 projection", f"EXACTLY {len(t2_rows)} rows, region-ascending: "
                            f"{t2_rows[0]['region']}…{t2_rows[-1]['region']} (k ≡ 1 mod 3)")
    if DIM_EXCLUDED in [r["region"] for r in t2_rows]:
        die("g47 leaked into the tier-2 projection (g47 is tier-3 — impossible)")
    ok("g47 losslessness", "tier-2 loses NO region to the g47 omission "
                           "(g47 is tier-3; the exclusion is already applied in the banked oracle)")

    # ---- 5. Hand-pin gates (design §5, asserted unconditionally) -----------
    print("== [5] hand-pin gates ==")
    t2_globals = {
        "rows": len(t2_rows),
        "sum_x": sum(r["x"] for r in t2_rows),
        "sum_c": sum(r["c"] for r in t2_rows),
        "sum_n": sum(r["n"] for r in t2_rows),
    }
    if t2_globals != T2_GLOBALS:
        die(f"tier-2 globals {t2_globals} != hand pins {T2_GLOBALS}")
    ok("tier-2 globals", f"rows={t2_globals['rows']} Σx={t2_globals['sum_x']:,} "
                         f"Σc={t2_globals['sum_c']} Σn={t2_globals['sum_n']}".replace(",", ","))
    bad_n = {r["region"]: r["n"] for r in t2_rows if r["n"] != N_R_UNIFORM}
    if bad_n:
        die(f"n_r != {N_R_UNIFORM} on {bad_n}")
    if t2_globals["sum_n"] != EXPECTED_T2_ROWS * N_R_UNIFORM:
        die(f"Σn_t2 {t2_globals['sum_n']} != 17 × 280 (uniformity re-derivation)")
    ok(f"n_r == {N_R_UNIFORM} ∀17", f"Σn_t2 = 17 × 280 = {t2_globals['sum_n']} "
                                    "(independent re-derivation of the banked value)")
    # Set-inclusion: the 17 keys ⊂ the 49 keys, per-region values byte-equal.
    t2_keys = {r["region"] for r in t2_rows}
    banked_keys = set(banked_by_region)
    if not t2_keys < banked_keys:
        die("set-inclusion FAILED: tier-2 keys not a strict subset of the 49 banked keys")
    for r in t2_rows:
        b = banked_by_region[r["region"]]
        if (r["x"], r["c"], r["n"]) != (b["x"], b["c"], b["n"]):
            die(f"tier-2 row {r['region']} {r} != banked row {b}")
    ok("set-inclusion", f"17 tier-2 region keys ⊂ 49 banked keys, per-region "
                        "{{x,c,n}} byte-identical (a predicate-drop mutant emits 49 → RED)")

    # Per-band projection. MAPPING RECORD: the banked per_band is per-REGION
    # → per-band; the E16 per_band is per-BAND aggregated over the 17 tier-2
    # regions: per_band[band] = {n: Σ_region n_region,band, sum_x: Σ_region x_region,band}.
    t2_per_band = {b: {"n": 0, "sum_x": 0} for b in ("A", "B", "C")}
    for r in t2_rows:
        pb = per_band[r["region"]]
        for b in ("A", "B", "C"):
            t2_per_band[b]["n"] += pb[b]["n"]
            t2_per_band[b]["sum_x"] += pb[b]["x"]
    if t2_per_band != T2_PER_BAND:
        die(f"per-band projection {t2_per_band} != hand pins {T2_PER_BAND}")
    ok("per-band projection (mapping recorded)", "banked per_region→per_band "
        "SUMMED over the 17 tier-2 regions → per-band {n, sum_x}: "
        f"A n={t2_per_band['A']['n']} Σx={t2_per_band['A']['sum_x']:,}; "
        f"B n={t2_per_band['B']['n']} Σx={t2_per_band['B']['sum_x']:,}; "
        f"C n={t2_per_band['C']['n']} Σx={t2_per_band['C']['sum_x']:,}".replace(",", ","))

    # ---- 6. Closure gates ---------------------------------------------------
    print("== [6] closure gates ==")
    if t2_per_band["A"]["sum_x"] + t2_per_band["B"]["sum_x"] + t2_per_band["C"]["sum_x"] != t2_globals["sum_x"]:
        die("closure FAILED: per-band Σx A+B+C != Σx_t2")
    if t2_per_band["A"]["n"] + t2_per_band["B"]["n"] + t2_per_band["C"]["n"] != t2_globals["sum_n"]:
        die("closure FAILED: per-band n A+B+C != Σn_t2")
    ok("per-band closure", f"A+B+C reassembles the tier-2 globals "
                           f"(Σx {t2_per_band['A']['sum_x']:,} + {t2_per_band['B']['sum_x']:,} + "
                           f"{t2_per_band['C']['sum_x']:,} = {t2_globals['sum_x']:,})".replace(",", ","))
    comp = {
        "sum_x": BANKED_GLOBALS["sum_x"] - t2_globals["sum_x"],
        "sum_c": BANKED_GLOBALS["sum_c"] - t2_globals["sum_c"],
        "sum_n": BANKED_GLOBALS["sum_n"] - t2_globals["sum_n"],
    }
    if comp != COMPLEMENT:
        die(f"complement closure {comp} != hand pins {COMPLEMENT}")
    ok("t1+t3 complement closure", f"banked − tier2 = {comp} (the §5 identity "
                                   "t1+t2+t3 = banked, ALL EXACT)")

    # ---- 7. Emit (a) tier2_oracle.json + (b) E16_tier2.json ----------------
    print("== [7] emit (gates ALL green — nothing written before this point) ==")
    now = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

    def fixture_obj():
        return {
            "id": "E16_tier2",
            "meta": {
                "fixture": "E16_tier2",
                "source": "audit/r128_join_oracle/join_oracle.json sha256 " + oracle_sha,
                "doc": "design_r132_w7_family.md §5 oracle (tier-2 sha-gated "
                       "PROJECTION of the banked join oracle — tier = (k % 3) + 1 == 2 ⇔ "
                       "k ≡ 1 mod 3) — rows = the 17 merged tier-2 output rows "
                       "{region, x, c, n} sorted region-ascending; x is the P0-1 scale-0 "
                       "fixed-point integer, c = count(amount) (null-blind), n = count(*); "
                       "n_r = 280 uniform ∀17; per_band = the {n, sum_x} projection home.",
            },
            "rows": [{"region": r["region"], "x": r["x"], "c": r["c"], "n": r["n"]}
                     for r in t2_rows],
            "excluded": {
                "note": "tier-2 loses NO region to the g47 omission (g47 is tier-3; "
                        "exclusion already applied in the banked oracle)",
            },
            "globals": {"rows": t2_globals["rows"], "sum_c": t2_globals["sum_c"],
                        "sum_n": t2_globals["sum_n"], "sum_x": t2_globals["sum_x"]},
            "per_band": {b: {"n": t2_per_band[b]["n"], "sum_x": t2_per_band[b]["sum_x"]}
                         for b in ("A", "B", "C")},
        }

    fx = fixture_obj()
    e16_bytes = render_e16(fx)
    # Self-check: the rendered fixture must re-parse to EXACTLY the intended
    # object (the hand renderer never silently diverges from the schema).
    if json.loads(e16_bytes.decode("utf-8")) != fx:
        die("render_e16 self-check FAILED: rendered bytes re-parse != intended object")
    ok("E16 render self-check", "rendered bytes re-parse EXACTLY to the §5 schema object")

    os.makedirs(os.path.dirname(args.out_audit), exist_ok=True)
    audit = {
        "meta": {
            "source": {"path": os.path.relpath(args.oracle, "/") if args.oracle.startswith("/") else args.oracle,
                       "sha256": oracle_sha},
            "doc": "design_r132_w7_family.md §5 — the W7 tier-2 oracle = sha-gated "
                   "PROJECTION of the banked r128 join oracle (deterministic; corpus "
                   "NOT re-read, corpus shas re-gated from the banked meta)",
            "tier_rule": TIER_RULE,
            "projection_rule": "keep output_rows rows with tier(region) == 2, "
                               "region-ascending; tier(region) = (k % 3) + 1, k = int(region[1:])",
            "per_band_mapping_recorded": "the banked per_band is per-REGION → per-band "
                                         "{A,B,C} {x,c,n}; the projection SUMS each band's "
                                         "x/n across the 17 tier-2 regions → per-band {n, sum_x}",
            "banked_globals": BANKED_GLOBALS,
            "generated_at": now,
            "generator": GENERATOR,
        },
        "rows": fx["rows"],
        "globals": {"rows": t2_globals["rows"], "sum_x": t2_globals["sum_x"],
                    "sum_c": t2_globals["sum_c"], "sum_n": t2_globals["sum_n"]},
        "per_band": fx["per_band"],
        "set_inclusion": {"tier2_regions": len(t2_rows), "banked_regions": len(banked_keys),
                          "strict_subset": True, "per_region_values_identical": True},
        "closure": {"per_band_reassembles_t2": True, "complement_t1_t3": comp},
        "excluded": {"note": fx["excluded"]["note"]},
        "emitted": {"fixture": args.out_fixture, "audit": args.out_audit},
        "gates": checks,
    }
    with open(args.out_audit, "w") as f:
        json.dump(audit, f, indent=2)
        f.write("\n")
    with open(args.out_fixture, "wb") as f:
        f.write(e16_bytes)
    ok("artifact written", f"{os.path.relpath(args.out_audit, '/')} sha256={sha256_file(args.out_audit)}")
    ok("fixture written", f"{os.path.relpath(args.out_fixture, REPO)} sha256={sha256_file(args.out_fixture)}")

    # ---- 8. Final numbers ---------------------------------------------------
    print("\n== FINAL NUMBERS ==")
    print("first 3 rows:")
    for row in fx["rows"][:3]:
        print(f"  {row['region']}  x={row['x']:>9}  c={row['c']}  n={row['n']}")
    print("last 3 rows:")
    for row in fx["rows"][-3:]:
        print(f"  {row['region']}  x={row['x']:>9}  c={row['c']}  n={row['n']}")
    print(f"globals: rows={t2_globals['rows']} Σx={t2_globals['sum_x']:,} "
          f"Σc={t2_globals['sum_c']} Σn={t2_globals['sum_n']}".replace(",", ","))
    print(f"per_band: " + " ".join(
        f"{b}{{n={v['n']}, Σx={v['sum_x']:,}}}".replace(",", ",")
        for b, v in fx["per_band"].items()))
    print(f"\nALL {len(checks)} CROSS-CHECKS GREEN — the tier-2 projection is canonical.")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except OracleAbort as e:
        print(f"FATAL: {e}", file=sys.stderr)
        sys.exit(1)
