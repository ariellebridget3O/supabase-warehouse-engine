#!/usr/bin/env python3
"""
lint_shard_templates.py — L3 template lint for the wh_query wave (r43).

Spec: research/design_wh_query_rpc.md §3.1 + r42 errata F10 (checkable grammar),
F2 (rows-kind sentinel law / jsonb_agg-wrap ban). Runs over db/shard-templates/
(body-of-record: <slug>.sql files + manifest.json) and verifies, per template:

  (a) file bytes -> sha256 == manifest.template_hash          (the §2.3 byte law)
  (b) body is ONE statement (dollar-quote-aware split on top-level semicolons)
  (c) placeables are ONLY $1 and $2
  (d) $1 grammar: whole `$1`, `$1::jsonb`, or `$1->>'k'` / `($1->>'k')` with an
      optional `::<type>` cast, where `k` is declared in params_schema.
      [IMPL-DECISION] The design's F10 grammar pins the arrow form as
      `($1->>'k')::<type>` (cast mandatory) — but the design's own pinned W1
      body uses a bare `$1->>'id_min'` inside its IS-NULL guards and pinned W5
      uses `($1->>'dataset')` with NO cast (text = text comparison). The hash
      law (§2.3) pins those bytes, so the lint accepts the arrow form with an
      OPTIONAL cast; the cast, when present, is still checked syntactically.
  (e) rows-kind bodies end `limit $2` and emit `_pre_trim` (F2; also enforced
      by the registry table CHECK — the lint re-proves it at authoring time).
      [r43-F19] BOTH anchors run on COMMENT-STRIPPED effective text (the
      audit-b trailing-`-- limit $2`-comment bypass matched the raw text), and
      rows-kind bodies must NOT contain row-aggregate wrappers
      (jsonb_agg/array_agg/string_agg — the audit-b aggregate-blob bypass hid
      a whole table in ONE row and defeated max_rows with truncated:false).
  (f) banned tokens (case-insensitive, scanned over STRING-BLANKED +
      comment-stripped text so literals cannot smuggle or false-trip):
      format(, nested EXECUTE, dblink, pg_sleep, query_to_xml, cursor_to_xml,
      pg_advisory* (session-state pollution, T5 — audit-b P2-4), set_config(
      (no GUC mutation in bodies — audit-b P2-5), insert into / update /
      delete from / copy / set / grant / create / alter
  (g) every table reference in from/join clauses is public.-qualified
      (regex-level, best effort — L1 is the proof, this lint is a claim)
  (h) manifest coherence: qc_class/kind/state enums, timeout_ms 1..30000,
      max_rows 1..1000000 (mirrors the registry CHECK, r43-F19), merge_ops
      subset of the §6.3 enumeration, schema_version >= 1, group_keys/
      params_schema shapes, logical_table identifier-safe (audit-b P2-6: a
      meta-character logical_table interpolated RAW into renderer GRANTs
      splits into attacker statements), aggs/encoding object shapes.

Exit code 0 iff every template passes every check; prints a per-template
PASS/FAIL table plus the recomputed sha256 of each body file.
"""

import hashlib
import json
import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
TEMPLATES_DIR_DEFAULT = REPO_ROOT / "db" / "shard-templates"

QC_CLASSES = {"QC2", "QC3", "QC4", "STDDEV", "PERCENTILE", "COLD_AGG", "QC6"}
KINDS = {"rows", "scalar"}
STATES = {"draft", "active", "frozen", "retired"}
MERGE_OPS = {
    "count", "count_col", "sum", "min", "max", "avg_pair",
    "groupby", "topk", "hll", "welford", "pct", "raw_rows",
}

# Banned tokens, case-insensitive (§3.1 L3 + F10). Word-boundary aware so
# e.g. `offset` does not trip `set` and `set_config` does not trip `set`.
BANNED_PATTERNS = [
    (r"format\s*\(", "format( (SQL-from-text executor)"),
    (r"\bexecute\b", "nested EXECUTE"),
    (r"\bdblink", "dblink (shard self-sufficiency, L3)"),
    (r"\bpg_sleep\s*\(", "pg_sleep"),
    (r"\bpg_advisory\w*\s*\(", "pg_advisory_* (locks outlive the request on pooled sessions, T5 — audit-b P2-4)"),
    (r"\bset_config\s*\(", "set_config( (no GUC mutation in bodies, T5 — audit-b P2-5)"),
    (r"query_to_xml\s*\(", "query_to_xml (XML row-shaping escapes the grammar, F10)"),
    (r"cursor_to_xml\s*\(", "cursor_to_xml (F10)"),
    (r"\binsert\s+into\b", "insert into (read-only bodies)"),
    (r"\bupdate\b", "update (read-only bodies)"),
    (r"\bdelete\s+from\b", "delete from (read-only bodies)"),
    (r"\bcopy\b", "copy (read-only bodies)"),
    (r"\bset\b", "set (no GUC/session mutation in bodies)"),
    (r"\bgrant\b", "grant (DDL in bodies)"),
    (r"\bcreate\b", "create (DDL in bodies)"),
    (r"\balter\b", "alter (DDL in bodies)"),
]

# [r43-F19] row-aggregate wrappers banned OUTRIGHT in rows-kind bodies — the
# per-row output shape (row_json, _pre_trim) is what makes the max_rows cap
# and the truncated gate real; any (jsonb_agg|array_agg|string_agg) in a
# rows-kind body is a blob smuggling the trim past the wrapper.
ROWS_AGG_BAN_RE = re.compile(r"\b(jsonb_agg|array_agg|string_agg)\s*\(", re.IGNORECASE)

IDENT_RE = re.compile(r"[a-z_][a-z0-9_]*\Z")

# $1 arrow form: optional open paren, $1->>'k', optional close paren, optional cast.
ARROW_RE = re.compile(r"\(?\$1->>'([A-Za-z_][A-Za-z0-9_]*)'\)?(::[A-Za-z_][A-Za-z0-9_]*)?")
WHOLE_JSONB_RE = re.compile(r"\$1::jsonb(?![A-Za-z0-9_])")
WHOLE_RE = re.compile(r"\$1(?![A-Za-z0-9_>:])")
DOLLAR_REFS_RE = re.compile(r"\$(\d+)")
LIMIT2_ANCHOR_RE = re.compile(r"limit\s+\$2\s*\Z")  # mirrors the registry CHECK's POSIX [[:space:]]
JSONB_AGG_RE = re.compile(r"jsonb_agg\s*\(")


def split_top_level(sql: str) -> list:
    """Split SQL on top-level semicolons; aware of '…', "…", --, /* */ (nested),
    and $tag$/$$ dollar-quoting. Returns non-empty statement strings."""
    stmts, buf = [], []
    i, n = 0, len(sql)
    while i < n:
        c = sql[i]
        if c == "'":
            j = i + 1
            while j < n:
                if sql[j] == "'":
                    if j + 1 < n and sql[j + 1] == "'":
                        j += 2
                        continue
                    break
                j += 1
            buf.append(sql[i:j + 1])
            i = j + 1
        elif c == '"':
            j = i + 1
            while j < n and sql[j] != '"':
                j += 1
            buf.append(sql[i:j + 1])
            i = j + 1
        elif c == "-" and i + 1 < n and sql[i + 1] == "-":
            j = sql.find("\n", i)
            j = n if j == -1 else j
            buf.append(sql[i:j])
            i = j
        elif c == "/" and i + 1 < n and sql[i + 1] == "*":
            depth, j = 1, i + 2
            while j < n and depth:
                if sql[j:j + 2] == "/*":
                    depth += 1
                    j += 2
                elif sql[j:j + 2] == "*/":
                    depth -= 1
                    j += 2
                else:
                    j += 1
            buf.append(sql[i:j])
            i = j
        elif c == "$":
            m = re.match(r"\$([A-Za-z_][A-Za-z0-9_]*)?\$", sql[i:])
            if m:
                tag = m.group(0)
                end = sql.find(tag, i + len(tag))
                if end == -1:
                    buf.append(sql[i:])
                    i = n
                else:
                    buf.append(sql[i:end + len(tag)])
                    i = end + len(tag)
            else:
                buf.append(c)
                i += 1
        elif c == ";":
            stmt = "".join(buf).strip()
            if stmt:
                stmts.append(stmt)
            buf = []
            i += 1
        else:
            buf.append(c)
            i += 1
    tail = "".join(buf).strip()
    if tail:
        stmts.append(tail)
    return stmts


def strip_comments_and_strings(sql: str) -> str:
    """Replace comments AND string-literal CONTENTS with spaces (structure and
    offsets preserved). Used for the banned-token scan (a literal like
    'please do not execute' must neither smuggle nor false-trip a token) and
    as the base for comment-stripped effective text. Dollar-quoted spans are
    kept verbatim (bodies have no dollar quotes, but be safe)."""
    out = []
    i, n = 0, len(sql)
    while i < n:
        c = sql[i]
        if c == "'":
            j = i + 1
            while j < n:
                if sql[j] == "'":
                    if j + 1 < n and sql[j + 1] == "'":
                        j += 2
                        continue
                    break
                j += 1
            out.append("'" + " " * max(0, j - i - 1) + ("'" if j < n else ""))
            i = j + 1 if j < n else n
        elif c == '"':
            j = i + 1
            while j < n and sql[j] != '"':
                j += 1
            out.append('"' + " " * max(0, j - i - 1) + ('"' if j < n else ""))
            i = j + 1 if j < n else n
        elif c == "-" and i + 1 < n and sql[i + 1] == "-":
            j = sql.find("\n", i)
            j = n if j == -1 else j
            out.append(" " * (j - i))
            i = j
        elif c == "/" and i + 1 < n and sql[i + 1] == "*":
            depth, j = 1, i + 2
            while j < n and depth:
                if sql[j:j + 2] == "/*":
                    depth += 1
                    j += 2
                elif sql[j:j + 2] == "*/":
                    depth -= 1
                    j += 2
                else:
                    j += 1
            out.append(" " * (j - i))
            i = j
        elif c == "$":
            m = re.match(r"\$([A-Za-z_][A-Za-z0-9_]*)?\$", sql[i:])
            if m:
                tag = m.group(0)
                end = sql.find(tag, i + len(tag))
                if end == -1:
                    out.append(sql[i:])
                    i = n
                else:
                    out.append(sql[i:end + len(tag)])
                    i = end + len(tag)
            else:
                out.append(c)
                i += 1
        else:
            out.append(c)
            i += 1
    return "".join(out)


def effective_text(sql: str) -> str:
    """Shape-scan text — MUST mirror the SQL-side wh_body_effective() exactly:
    STRING LITERALS blanked FIRST (the audit-a '/*'-in-string sandwich hid real
    code from comments-only stripping, and '--'-in-string false-rejected valid
    bodies; strings-first kills both), then block comments, then line comments.
    The tokenizer strip_comments_and_strings below implements exactly that
    order in one pass; this wrapper is its shape-scan alias."""
    return strip_comments_and_strings(sql)


def jsonb_agg_wrap_ban_hit(body: str) -> bool:
    """True iff a jsonb_agg( paren span contains a `limit $2` occurrence — the
    F2 wrap ban (a pre-aggregated blob hides the trim from the sentinel)."""
    hits = [m.start() for m in JSONB_AGG_RE.finditer(body)]
    if not hits:
        return False
    for start in hits:
        i = JSONB_AGG_RE.match(body, start).end()  # just past the open paren
        depth = 1
        while i < len(body) and depth:
            ch = body[i]
            if ch == "'":  # skip string literal
                j = i + 1
                while j < len(body):
                    if body[j] == "'":
                        if j + 1 < len(body) and body[j + 1] == "'":
                            j += 2
                            continue
                        break
                    j += 1
                i = j + 1
                continue
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            i += 1
        span = body[start:i]
        if re.search(r"limit\s+\$2", span):
            return True
    return False


def check_dollar1_grammar(body: str, schema_keys: set, findings: list) -> None:
    """Every $1 occurrence must be consumed by one of the allowed forms (d)."""
    for m in re.finditer(r"\$1(?![0-9])", body):
        seg = body[m.start():]
        arrow = ARROW_RE.match(seg)
        if arrow:
            key = arrow.group(1)
            if key not in schema_keys:
                findings.append(f"$1->>'{key}': key not declared in params_schema")
            continue
        if WHOLE_JSONB_RE.match(seg) or WHOLE_RE.match(seg):
            continue
        ctx = body[max(0, m.start() - 24):m.start() + 24].replace("\n", " ")
        findings.append(f"$1 occurrence outside the allowed grammar near: ...{ctx}...")


def check_table_qualification(body: str, findings: list) -> None:
    """Regex-level (best effort): from/join table references must be
    public.-qualified. Subqueries (`from (`) and `join lateral <fn>(` are not
    table references and are skipped; L1 is the proof, this is a claim."""
    for m in re.finditer(r"\b(from|join)\s+([A-Za-z_][A-Za-z0-9_]*)", body):
        ident = m.group(2)
        if ident.lower() == "lateral":  # join lateral <function>(...) — call, not table
            continue
        rest = body[m.end():]
        if ident == "public":
            if not re.match(r"\s*\.\s*[A-Za-z_]", rest):
                findings.append(f"bare `public` token after {m.group(1)} (expected public.<table>)")
            continue
        # unqualified identifier: a table reference unless it is a function call
        if re.match(r"\s*\(", rest):
            continue  # from/join <fn>(...) — function call (e.g. unpack_block(b))
        findings.append(
            f"unqualified table reference `{ident}` after {m.group(1)} — bodies must "
            f"schema-qualify tables as public.<name>"
        )


def lint_template(entry: dict, tdir: Path) -> dict:
    """Returns {slug, ok, checks: {check_name: (bool, detail)}, hash}."""
    res = {"slug": entry.get("slug", "?"), "ok": True, "checks": {}, "hash": None}
    findings = []

    def add(name, ok, detail):
        res["checks"][name] = (ok, detail)
        if not ok:
            findings.append(detail)

    # (a) byte law
    fpath = tdir / entry.get("file", "")
    raw = None
    if not fpath.is_file():
        add("a_hash", False, f"file missing: {entry.get('file')}")
    else:
        raw = fpath.read_bytes()
        try:
            raw.decode("utf-8")
            utf8_ok = True
        except UnicodeDecodeError:
            utf8_ok = False
        if b"\r" in raw:
            add("a_hash", False, "CR bytes present — body must be LF-only (§2.3 byte law)")
        elif raw.endswith(b"\n"):
            add("a_hash", False, "trailing newline present — body must end without LF (§2.3 byte law)")
        elif not utf8_ok:
            add("a_hash", False, "file is not valid UTF-8")
        else:
            h = hashlib.sha256(raw).hexdigest()
            res["hash"] = h
            if h != entry.get("template_hash"):
                add("a_hash", False,
                    f"sha256 {h} != manifest hash {entry.get('template_hash')}")
            else:
                add("a_hash", True, f"sha256 {h} == manifest")

    body = raw.decode("utf-8") if raw is not None and b"\r" not in (raw or b"") else ""

    # (b) single statement
    stmts = split_top_level(body) if body else []
    add("b_single_stmt", len(stmts) == 1,
        "1 statement" if len(stmts) == 1 else f"{len(stmts)} top-level statements")

    # (c) placeables only $1 / $2
    refs = set(DOLLAR_REFS_RE.findall(body))
    bad_refs = refs - {"1", "2"}
    add("c_placeables", not bad_refs,
        "placeables ⊆ {$1,$2}" if not bad_refs
        else f"illegal placeables: ${'$'.join(sorted(bad_refs))}")

    # (d) $1 grammar vs params_schema
    schema = entry.get("params_schema") or {}
    if not isinstance(schema, dict):
        add("d_dollar1", False, "params_schema is not an object")
        schema = {}
    grammar_findings = []
    check_dollar1_grammar(body, set(schema.keys()), grammar_findings)
    add("d_dollar1", not grammar_findings,
        "$1 grammar ok" if not grammar_findings else "; ".join(grammar_findings))

    # (e) rows-kind sentinel shape (F2 + r43-F19 + audit-a F-A/F-B): on the
    #     STRING-BLANKED + comment-stripped shape text (mirrors the SQL-side
    #     wh_body_effective: strings first, then 'gs' blocks, then lines) —
    #     ends `limit $2`, emits `_pre_trim` (+ alias), pins `row_json`, NO
    #     row-aggregate wrapper, and NO double quote (quoted identifiers
    #     cannot smuggle "jsonb_agg"( past the ban; data-`"` lives in strings
    #     and is already blanked).
    kind = entry.get("kind")
    eff = effective_text(body) if body else ""
    if kind == "rows":
        ends_ok = bool(LIMIT2_ANCHOR_RE.search(eff))
        pre_ok = "_pre_trim" in eff and re.search(r"as\s+_pre_trim\b", eff, re.IGNORECASE)
        rowj_ok = re.search(r"as\s+row_json\b", eff, re.IGNORECASE)
        agg_hit = ROWS_AGG_BAN_RE.search(eff)
        dq_hit = '"' in eff
        add("e_rows_shape", ends_ok and pre_ok and bool(rowj_ok) and not agg_hit and not dq_hit,
            "ends `limit $2` + per-row (row_json, _pre_trim) names, no row-agg wrapper, no `\"`"
            if (ends_ok and pre_ok and rowj_ok and not agg_hit and not dq_hit)
            else f"ends limit $2 (shape text): {ends_ok}; _pre_trim (+alias): {pre_ok}; "
                 f"row_json alias: {bool(rowj_ok)}; row-agg wrapper: {bool(agg_hit)}; "
                 f"double-quote: {dq_hit}")
    else:
        add("e_rows_shape", True, "scalar kind — no sentinel shape required")

    # (f) banned tokens + F2 jsonb_agg-wrap ban — scanned over the SAME
    #     shape text (strings blanked first: literals can neither smuggle nor
    #     false-trip).
    scan = eff
    banned_hits = []
    low = scan.lower()
    for pat, why in BANNED_PATTERNS:
        if re.search(pat, low):
            banned_hits.append(why)
    if kind == "rows" and jsonb_agg_wrap_ban_hit(scan):
        banned_hits.append("jsonb_agg( wraps limit $2 (F2 row-sniff blind spot)")
    add("f_banned", not banned_hits,
        "no banned tokens" if not banned_hits else "; ".join(sorted(set(banned_hits))))

    # (g) public.-qualified table refs (best effort)
    qual_findings = []
    check_table_qualification(body, qual_findings)
    add("g_qualified", not qual_findings,
        "table refs public.-qualified" if not qual_findings else "; ".join(qual_findings))

    # (h) manifest coherence
    mh = []
    if entry.get("qc_class") not in QC_CLASSES:
        mh.append(f"qc_class {entry.get('qc_class')!r} not in {sorted(QC_CLASSES)}")
    if kind not in KINDS:
        mh.append(f"kind {kind!r} not in {sorted(KINDS)}")
    if entry.get("state") not in STATES:
        mh.append(f"state {entry.get('state')!r} not in {sorted(STATES)}")
    lt = entry.get("logical_table")
    if not isinstance(lt, str) or not IDENT_RE.fullmatch(lt):
        # audit-b P2-6: the renderer interpolates logical_table RAW into
        # `grant select on public.<lt> to wh_executor;` — an identifier-unsafe
        # value splits that statement into attacker SQL. The registry column
        # is likewise rendered into engine-facing envelopes.
        mh.append(f"logical_table {lt!r} must match [a-z_][a-z0-9_]*")
    to = entry.get("timeout_ms")
    if not isinstance(to, int) or isinstance(to, bool) or not (1 <= to <= 30000):
        mh.append(f"timeout_ms {to!r} must be int in [1,30000] (0/-1 would DISABLE the timeout, F11)")
    mr = entry.get("max_rows")
    if not isinstance(mr, int) or isinstance(mr, bool) or not (1 <= mr <= 1000000):
        mh.append(f"max_rows {mr!r} must be int in [1,1000000] (mirrors the registry CHECK, r43-F19)")
    sv = entry.get("schema_version")
    if not isinstance(sv, int) or isinstance(sv, bool) or sv < 1:
        mh.append(f"schema_version {sv!r} must be int >= 1")
    ops = entry.get("merge_ops")
    if not isinstance(ops, list) or any(o not in MERGE_OPS for o in ops):
        mh.append(f"merge_ops {ops!r} must be a list ⊆ {sorted(MERGE_OPS)}")
    gk = entry.get("group_keys")
    if not isinstance(gk, list):
        mh.append("group_keys must be a list")
    for jkey, jshape in (("aggs", "object of {alias:{op,col}}"), ("encoding", "object of {alias:'text'|'number'}")):
        jv = entry.get(jkey)
        if not isinstance(jv, dict):
            mh.append(f"{jkey} must be an object ({jshape})")
        elif jkey == "aggs":
            for alias, spec in jv.items():
                if not (isinstance(spec, dict) and isinstance(spec.get("op"), str) and
                        ("col" not in spec or isinstance(spec.get("col"), str))):
                    mh.append(f"aggs.{alias} must be {{op, col?}}")
        else:
            for alias, enc in jv.items():
                if enc not in ("text", "number"):
                    mh.append(f"encoding.{alias} must be 'text'|'number' (got {enc!r})")
    th = entry.get("template_hash", "")
    if not re.fullmatch(r"[0-9a-f]{64}", th):
        mh.append("template_hash must be 64-char lowercase hex")
    add("h_manifest", not mh, "manifest coherent" if not mh else "; ".join(mh))

    res["ok"] = all(ok for ok, _ in res["checks"].values())
    res["findings"] = findings
    return res


def main() -> int:
    tdir = Path(sys.argv[1]) if len(sys.argv) > 1 else TEMPLATES_DIR_DEFAULT
    mpath = tdir / "manifest.json"
    if not mpath.is_file():
        print(f"FAIL: manifest missing: {mpath}", file=sys.stderr)
        return 2
    try:
        manifest = json.loads(mpath.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        print(f"FAIL: manifest.json is not valid JSON: {e}", file=sys.stderr)
        return 2
    if not isinstance(manifest, list) or not manifest:
        print("FAIL: manifest.json must be a non-empty array", file=sys.stderr)
        return 2

    results = [lint_template(e, tdir) for e in manifest]

    name_w = max(len(r["slug"]) for r in results) + 2
    print("=" * (name_w + 66))
    print(f"{'template':<{name_w}} {'hash':<10} {'stmt':<5} {'shape':<6} {'banned':<7} {'result'}")
    print("-" * (name_w + 66))
    for r in results:
        h_ok = r["checks"].get("a_hash", (False, ""))[0]
        s_ok = r["checks"].get("b_single_stmt", (False, ""))[0]
        e_ok = r["checks"].get("e_rows_shape", (False, ""))[0]
        f_ok = r["checks"].get("f_banned", (False, ""))[0]
        print(f"{r['slug']:<{name_w}} {('ok' if h_ok else 'BAD'):<10} "
              f"{('1' if s_ok else 'N'):<5} {('ok' if e_ok else 'BAD'):<6} "
              f"{('ok' if f_ok else 'BAD'):<7} {'PASS' if r['ok'] else 'FAIL'}")
        if not r["ok"]:
            for f in r["findings"]:
                print(f"{'':<{name_w}}   ! {f}")
    print("-" * (name_w + 66))
    print("recomputed sha256 per body file (§2.3 byte law):")
    for r in results:
        print(f"  {r['slug']:<{name_w}} {r['hash'] or '<n/a>'}")
    npass = sum(1 for r in results if r["ok"])
    print(f"{npass}/{len(results)} templates PASS")
    return 0 if npass == len(results) else 1


if __name__ == "__main__":
    sys.exit(main())
