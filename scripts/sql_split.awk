# =============================================================================
# scripts/sql_split.awk — split a Postgres SQL script into single statements.
# =============================================================================
# Bash-side twin of supabase/functions/_shared/management-api.ts
# ::splitSqlStatements (the P0-4 fix): tracks single/double-quoted string
# literals, `--` line comments, `/* */` block comments, and `$tag$`-style
# dollar-quoted strings (`$$`, `$body$`, `$tmpl$`, …). A `;` only terminates
# a statement when none of those contexts is open, so function bodies,
# cron.schedule() commands nested in a do-block, and SQL templates stored as
# string literals survive intact.
#
# stdin : SQL text (one file)
# stdout: statements separated by ASCII 0x1e (RS). Comments are stripped
#         (replaced by a single space so tokens never glue together); all
#         other whitespace, including newlines, is preserved.
#
# Pure POSIX awk (tested against mawk/gawk/BWK): no gawk extensions.
# =============================================================================
function emit() {
    sub(/^[ \t\r\n]+/, "", cur)
    sub(/[ \t\r\n]+$/, "", cur)
    if (length(cur) > 0) {
        printf "%s%c", cur, 30
    }
    cur = ""
}

{
    line = $0
    n = length(line)
    i = 1

    # Resume inside a multi-line /* ... */ block comment.
    if (inblock) {
        k = index(line, "*/")
        if (k == 0) next                      # whole line still commented
        inblock = 0
        i = k + 2
    }

    while (i <= n) {
        ch = substr(line, i, 1)
        nx = (i < n) ? substr(line, i + 1, 1) : ""

        # ---- inside a $tag$ ... $tag$ string: only the exact tag closes it --
        if (dollar != "") {
            if (ch == "$" && substr(line, i, length(dollar)) == dollar) {
                cur = cur dollar
                i += length(dollar)
                dollar = ""
            } else {
                cur = cur ch
                i++
            }
            continue
        }

        # ---- inside a '...' or "..." literal --------------------------------
        if (quote != "") {
            cur = cur ch
            if (ch == quote) {
                if (nx == quote) {            # SQL-standard doubled-quote escape
                    cur = cur nx
                    i += 2
                    continue
                }
                quote = ""
            }
            i++
            continue
        }

        # ---- open a dollar-quoted string: $ + [A-Za-z0-9_]* + $ -------------
        if (ch == "$") {
            j = i + 1
            while (j <= n && substr(line, j, 1) ~ /[A-Za-z0-9_]/) j++
            if (j <= n && substr(line, j, 1) == "$") {
                dollar = substr(line, i, j - i + 1)
                cur = cur dollar
                i = j + 1
                continue
            }
            # Not a tag (e.g. a $1 placeholder) — fall through, literal $.
        }

        # ---- line comment ----------------------------------------------------
        if (ch == "-" && nx == "-") {
            cur = cur " "                     # spacer: never glue tokens
            i = n + 1                         # skip rest of the line
            continue
        }

        # ---- block comment (may span lines) ----------------------------------
        if (ch == "/" && nx == "*") {
            cur = cur " "
            k = index(substr(line, i + 2), "*/")
            if (k == 0) {
                inblock = 1
                break                         # rest of this line is comment
            }
            i = i + 2 + k + 1                 # resume after the closing */
            continue
        }

        # ---- open a quoted literal -------------------------------------------
        if (ch == "'" || ch == "\"") {
            quote = ch
            cur = cur ch
            i++
            continue
        }

        # ---- statement terminator --------------------------------------------
        if (ch == ";") {
            emit()
            i++
            continue
        }

        cur = cur ch
        i++
    }

    # Preserve the line break as whitespace. Inside an open '…'/"…"/$tag$
    # literal the newline is literal content and must survive; between
    # tokens it is plain whitespace. (Only when inside a /* … */ block
    # comment is it dropped — comment content is discarded anyway.)
    if (!inblock) {
        cur = cur "\n"
    }
}

END {
    emit()                                    # trailing statement without ';'
}
