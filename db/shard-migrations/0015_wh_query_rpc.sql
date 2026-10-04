-- =============================================================================
-- 0015_wh_query_rpc.sql — the wh_query wave STATIC part (r43)
-- =============================================================================
-- Spec: research/design_wh_query_rpc.md (r42 audit-hardened, 483 ln) — §2.1/§2.2
-- (signature + registry), §4.2 (role/ACL plan), §2.3 (hash law), §2.4/F5/F9
-- (wave shape + ordering law), §3.1/F10 (L3 lint lives in
-- scripts/lint_shard_templates.py), §6.2/F7 (envelope), §7 (worked examples,
-- seeded by the companion wave generator scripts/render_wh_seed_wave.py).
--
-- APPLY TARGET: each SERVING SHARD via the Supabase Management SQL API
-- (POST /v1/projects/{ref}/database/query) — NOT the Fleet Manager project.
-- This file lives in db/shard-migrations/ (NOT db/migrations/) precisely
-- because the FM project's scripts/migrate.sh auto-globs db/migrations/*.sql
-- and would otherwise apply it to the FM project. Shard-wave application is
-- manifest-driven: db/shard-templates/ (bodies-of-record) +
-- scripts/render_wh_seed_wave.py (per-shard seed statements) carry the
-- per-template INSERTs and backing-table grants that complete the wave.
--
-- SPLITTABILITY CONTRACT: this file splits on TOP-LEVEL semicolons with
-- dollar-quote awareness ($tag$ … $tag$ spans — including $$ — plus '…',
-- "…", -- and /* */ literals; block comments nest). Each resulting statement
-- is one Management-API call, exactly the sql_split.awk semantics migrate.sh
-- uses for FM-project migrations. 26 statements. [r43 audit-a fixes: the
-- shape-scan helper blanks STRING LITERALS first (kills the '/*'-in-string
-- sandwich that hid real code from BOTH the CHECK and the lint mirror, and
-- the '--'-in-string false-reject); double quotes are BANNED in rows-kind
-- bodies (quoted-identifier "jsonb_agg"( bypassed the agg ban); NULL
-- _pre_trim joins NULL row_json in the WH500 guard; aggs/encoding get an
-- add-column-if-not-exists upgrade path; the L4 arm probes ADMIN OPTION
-- before altering roles; post-wave pool-recycle note for the role GUC.]
--
-- HASH LAW (§2.3): template_hash = sha256 of the EXACT seeded body bytes
-- (LF, no trailing newline). Bodies live ONLY in FM repo db/shard-templates/
-- (body-of-record); the seed wave INSERTs them with on conflict do nothing.
-- THIS migration additionally enforces the law at RUNTIME: execution step (3b)
-- re-derives sha256(body) and refuses to EXECUTE on mismatch -> WH403
-- wh_hash_mismatch (r42 errata F3), closing the handshake<->execute tamper
-- window (§5.2).
--
-- ORDERING LAW (r42 errata F9): the FM job's finalize may flip a shard to
-- serving ONLY after the template seed AND the wh_executor backing-table
-- grants (emitted by scripts/render_wh_seed_wave.py) are verified COMPLETE on
-- that shard. A mid-wave shard is naturally wh_query-ineligible: its template
-- inventory fails the engine's §5.2 handshake. This file carries the static
-- prerequisites (role, registry, executor function, ACLs); the seed wave
-- carries the per-template INSERTs and backing-table grants. Both terminate
-- their statement stream with `notify pgrst, 'reload schema';` so a
-- fresh/redeployed shard's schema cache is warm before the first RPC call
-- (F5: without it the first call 404s identically to correct anon-rejection,
-- which would make the WP-3 probe undecidable).
--
-- ERROR SURFACE (§5.3): WH400 wh_template_not_found (hash miss / state=draft),
-- WH401 wh_template_retired, WH402 wh_param_violation (hash shape, params
-- >8192 bytes after NULL normalization, limit non-integer/negative — the
-- ::int cast's 22P02/22003 are caught and re-raised), WH403 wh_hash_mismatch
-- (shard-integrity alarm — exclude the shard AND page; never retried
-- silently). Custom 5-char SQLSTATEs ride RAISE ... USING ERRCODE=.
--
-- [r43 audit-b fixes applied] (1) the L4 timeout layer is armed PLATFORM-SIDE:
-- `alter role service_role set statement_timeout='30s'` (a role GUC applies at
-- session start and re-arms PER STATEMENT — the in-function proconfig and the
-- step-(4) set_config mutate the GUC mid-statement and NEVER re-arm the timer;
-- empirically proven on PG 17.6: proconfig 500ms + pg_sleep(2) completes 2.0s,
-- role-GUC 200ms + pg_sleep(1) cancels 57014 at 0.2s). PostgREST's
-- db-statement-timeout env is the second belt on the FM deploy side. The
-- proconfig/set_config remain as documentation + defense-in-depth for any
-- session that pre-lowered the GUC. Design erratum r43-F18.
-- (2) the rows-kind CHECK is COMMENT-AWARE (wh_body_effective strips -- and
-- /* */ first) + CASE-INSENSITIVE on the limit anchor + bans row-aggregate
-- wrappers (jsonb_agg/array_agg/string_agg) outright in rows-kind bodies —
-- the audit-b trailing-comment and aggregate-blob bypasses both defeated the
-- v1 text-only regex with all authoring gates green. Design erratum r43-F19.
-- (3) scalar bodies are STRICT (zero/multi-row => WH500 wh_body_shape_violation
-- — 'exactly one row by construction' is now enforced, r43-F19); NULL row_json
-- in rows-kind also WH500. (4) v_cap math in bigint (int8 max_rows + int8
-- limit no longer 22003s at INT_MAX); max_rows bounded 1..1000000 by CHECK.
-- (5) the envelope stamps aggs + encoding from the registry (§6.2 concretized;
-- audit-b P1-3). (6) lint bans pg_advisory* + set_config( in bodies (T5-class
-- session pollution — audit-b P2-4/P2-5); manifest logical_table is
-- identifier-validated in lint AND renderer (P2-6).
--
-- IDEMPOTENCY: every statement is safe to re-apply (create-if-missing guard,
-- no-op revokes/grants, drop-if-exists before create policy, CREATE OR
-- REPLACE — never DROP+CREATE, which would reset ACLs and re-open the PUBLIC
-- EXECUTE hole per the 0012 header doctrine). The one intentional
-- create-or-replace hazard — re-apply after ownership moved to wh_executor —
-- is bridged by the membership bootstrap statement (2) below; see its comment.
--
-- IMPL-DECISION markers (full rationale in the r43 report): statements (2),
-- (6)/(9), and the executor-read policy in the registry-lockdown phase.
-- =============================================================================

-- ---------- (1) executor role: minimal SECURITY DEFINER owner (§4.2) ---------
-- NOLOGIN: no session may ever authenticate AS the executor. NOINHERIT: even a
-- role that is granted wh_executor gains nothing by inheritance — the
-- definer's privileges apply only through the function itself. L1 wall (§3.1):
-- SELECT-only per warehouse table, granted by the seed wave per template.
do $wh_executor_role$
begin
  if not exists (select 1 from pg_catalog.pg_roles where rolname = 'wh_executor') then
    create role wh_executor nologin noinherit;
  end if;
end
$wh_executor_role$;

-- ---------- (2) wave-identity membership bootstrap [IMPL-DECISION] -----------
-- After statement (8) the function is OWNED by wh_executor. PG requires the
-- caller of ALTER ... OWNER to be able to SET ROLE to the new owner, and the
-- caller of a later CREATE OR REPLACE / COMMENT ON (re-apply after the flip)
-- to hold the owner's privileges via INHERIT-able membership. migrate.sh
-- re-applies every migration file on every run, so the wave identity (postgres)
-- must hold both against wh_executor.
--
-- [r59 0015fix — live 42501 "must be able to SET ROLE wh_executor" (PG 17.6)]
-- The v1 guard `if not pg_has_role(current_user,'wh_executor','member')` was
-- an UNSOUND SET-ROLE proxy on PG16+: a non-superuser CREATEROLE that creates
-- a role is automatically granted the new role back WITH ADMIN TRUE, SET
-- FALSE, INHERIT FALSE — as if the bootstrap superuser had executed that
-- GRANT (PG16+ docs, Role Attributes) — i.e. a REAL pg_auth_members row that
-- confers admin but NOT SET ROLE. pg_has_role 'member' means "direct or
-- indirect membership ... without regard to what specific privileges may be
-- conferred" (PG16+ docs, pg_has_role), so it returned TRUE on that row, the
-- explicit grant was SKIPPED, and ALTER ... OWNER TO then failed 42501 (to
-- "give ownership of an existing object to another role, you must have the
-- ability to SET ROLE to that role" — GRANT/ALTER docs). The docs' own remedy
-- is the shape used here: "the CREATEROLE user can gain access to the created
-- role by simply granting that role back to themselves with the INHERIT
-- and/or SET options."
--
-- THE FIX: UNCONDITIONAL version-aware grant — idempotent on every vintage.
-- PG16+ GRANT semantics: re-granting an existing membership MODIFIES its
-- options in place (omitted options retain their current value) and multiple
-- membership records are allowed (PG16 release notes), so the bootstrap
-- admin-only row is never downgraded or lost and re-apply is a no-op. PG15
-- has no SET/INHERIT grant syntax and no CREATEROLE self-grant: plain
-- membership is SET-able by definition, CREATEROLE may grant membership in
-- any non-superuser role, and a matching re-grant replaces the record
-- silently. Feature probe = the pg_auth_members.set_option column (present
-- exactly when per-grant SET/INHERIT semantics exist), not a version string.
-- A wave identity holding no ADMIN OPTION on wh_executor (role created by
-- someone else) fails loudly HERE at statement (2), before any object exists,
-- instead of 42501-ing mid-wave at (8).
do $wh_executor_membership$
begin
  if exists (
    select 1 from pg_catalog.pg_attribute
     where attrelid = 'pg_catalog.pg_auth_members'::regclass
       and attname = 'set_option'
       and attnum > 0
       and not attisdropped
  ) then
    -- PG16+: SET TRUE is what ALTER ... OWNER TO (statement (8)) requires;
    -- INHERIT TRUE is what the re-apply ownership checks (the CREATE OR
    -- REPLACE above, the COMMENT ON below) require. ADMIN is deliberately
    -- omitted — the bootstrap row keeps its ADMIN TRUE (omitted = retained
    -- on an option-modifying re-grant).
    execute format('grant wh_executor to %I with set true, inherit true',
                   current_user);
  else
    -- pre-16: no per-grant options; any membership is SET-able.
    execute format('grant wh_executor to %I', current_user);
  end if;
end
$wh_executor_membership$;

-- ---------- (3) template registry (§2.2 verbatim + r43-F19 hardening) --------
-- Shape-scan helper FIRST (the registry CHECK below references it at CREATE
-- TABLE time — PG validates the function lookup when the constraint is
-- parsed, so it must pre-exist the table).
-- Shape-scan text = STRING LITERALS blanked FIRST (the audit-a '/*'-in-string
-- sandwich hid real code from the v1 helper, and '--'-in-string caused
-- false-rejects; strings-first kills BOTH), then block comments (the 'gs'
-- flag — 'n' means newline-SENSITIVE in PG ARE, the audit-a F-B flag bug;
-- dot must CROSS newlines for multi-line blocks), then line comments.
-- Ordering is load-bearing. IMMUTABLE so the CHECK and future indexes can
-- use it. The `"` ban in the CHECK means quoted identifiers cannot smuggle
-- "jsonb_agg"( past the aggregate ban (audit-a F-A) — rows-kind analytic
-- bodies target lowercase catalog tables and need no quoting.
create or replace function public.wh_body_effective(p_body text)
returns text language sql immutable as $wef$
  select regexp_replace(
           regexp_replace(
             regexp_replace(p_body, '''([^'']|'''')*''', ' ', 'g'),
             '/\*.*?\*/', ' ', 'gs'),
           '--[^\n]*', ' ', 'g')
$wef$;

-- Shard-local; rides THIS wave, NOT 0013. PK = the full 64-hex template_hash;
-- rows are immutable by convention (superseded via state, §5.1) and by the
-- step-(3b) runtime hash re-verification, which makes any in-place body edit
-- unexecutable (WH403) even if an operator bypasses the wave.
create table if not exists public.wh_query_templates (
  template_hash  text primary key,        -- sha256 hex of the EXACT body bytes as seeded (§2.3)
  logical_table  text not null,           -- 'orders' — matches catalog warehouse_tables.logical_name
  qc_class       text not null,           -- 'QC2' | 'QC3' | 'QC4' | 'STDDEV' | 'PERCENTILE' | 'COLD_AGG'
  kind           text not null check (kind in ('rows','scalar')),
  body           text not null,           -- ONE statement; $1 (params jsonb) + $2 (capped limit) ONLY; rows-kind shape
                                         -- pinned by the effective-text CHECK below [r42 errata F2 + r43 errata F19]
  params_schema  jsonb not null default '{}',   -- documented keys; lint-checked against body casts
  merge_ops      jsonb not null default '[]',   -- §6 enumeration; engine validates at consumption site
  group_keys     jsonb not null default '[]',   -- e.g. ["region"] for grouped partials
  aggs           jsonb not null default '{}',   -- §6.2 envelope stamp: {alias:{op,col}} — from the manifest at seed
  encoding       jsonb not null default '{}',   -- §6.2 envelope stamp: {alias:'text'|'number'} — from the manifest
  timeout_ms     int  not null default 8000 check (timeout_ms between 1 and 30000),
                                         -- wrapper: least(this, 30000); [r42 errata F11] 0/-1 would DISABLE the timeout
                                         -- (PG treats 0 = off), never clamp — the range CHECK is the guard
  max_rows       int  not null default 1000 check (max_rows between 1 and 1000000),
                                         -- hard cap; sentinel LIMIT max_rows+1 [r43-F19: bounded — 0/negative would
                                         -- dead-template; int8 max + int limit overflowed v_cap to 22003]
  schema_version int  not null default 1,       -- [r42 errata F17] equality with the catalog schema_version is the ENGINE's
                                         -- handshake check (§5.2b) — unforceable shard-side (no cross-project reads, §5.1)
  state          text not null default 'active'
                 check (state in ('draft','active','frozen','retired')),
  created_at     timestamptz not null default now(),
  -- [r42 errata F2 + r43 errata F19] rows-kind sentinel law, pinned where a
  -- seeder cannot bypass it — on COMMENT-STRIPPED effective text (wh_body_effective
  -- below; the audit-b bypasses — a trailing '-- limit $2' comment and a
  -- jsonb_agg blob with the real limit outside the aggregate — both defeated
  -- the v1 raw-text regex): the outermost statement must end `limit $2`
  -- (case-insensitive: uppercase LIMIT is valid SQL — the HASH LAW keeps
  -- authored bodies canonical lowercase, this CHECK is the runtime backstop),
  -- must emit the pre-trim window `_pre_trim`, and must NOT wrap rows in a
  -- row-aggregate (jsonb_agg/array_agg/string_agg) — per-row output shape
  -- (row_json, _pre_trim) is what makes the cap + truncated gate real; a
  -- pre-aggregated blob hides the trim from the wrapper (5.88MB single-row
  -- envelope repro, audit-b P1-2b).
  check (kind <> 'rows' or (
           wh_body_effective(body) ~* 'limit[[:space:]]+\$2[[:space:]]*$'
       and wh_body_effective(body) ~ '_pre_trim'
       and wh_body_effective(body) !~ '"'
       and wh_body_effective(body) !~* '(jsonb_agg|array_agg|string_agg)[[:space:]]*\('
  ))
);

-- [r43 audit-a F-H] upgrade path: vintages that applied the 115f468 letter of
-- this file hold the 12-column registry; add the F20 stamp columns idempotently.
alter table public.wh_query_templates add column if not exists aggs jsonb not null default '{}'::jsonb;
alter table public.wh_query_templates add column if not exists encoding jsonb not null default '{}'::jsonb;

-- ---------- (4-9) ACL plan (§4.2 verbatim + marked deltas) -------------------
-- The executor reads the registry for the step-(3) PK lookup...
grant usage on schema public to wh_executor;
grant select on public.wh_query_templates to wh_executor;

-- ...and owns the executor function. [IMPL-DECISION] PG requires the NEW owner
-- to hold CREATE on the function's schema for ALTER ... OWNER TO (PG15+
-- public-schema lockdown); granted here and revoked at (9) the moment the
-- ownership flip has landed, so the steady-state ACL plan stays exactly §4.2's
-- (USAGE + per-table SELECT only).
grant create on schema public to wh_executor;

-- §2.1 signature — CREATE OR REPLACE ONLY (0012 doctrine: DROP resets ACLs and
-- hands EXECUTE back to PUBLIC). Fixed definer search_path (bodies are
-- registry text, schema-qualified and lint-enforced; THIS function's own
-- statements are pg_catalog/public-safe). statement_timeout='10s' is the
-- caller-proof ENTRY value; the per-template set_config below may RAISE it —
-- statement_timeout is USERSET and the later assignment wins, bounded by the
-- absolute cap least(row.timeout_ms, 30000) (r42 errata F4).
create or replace function public.wh_query(
  p_template_hash text,                 -- 64-char lowercase hex sha256; exactly one registry row
  p_params        jsonb default '{}'::jsonb
) returns jsonb                        -- the §6.2 partial envelope
language plpgsql
security definer                       -- owner = wh_executor (§4); rationale table §4.1
set search_path = pg_catalog, public   -- definer hygiene: fixed search_path; bodies schema-qualify
set statement_timeout = '10s'          -- caller-proof ENTRY value; per-call set_config may RAISE it (USERSET, later
                                       -- assignment wins), bounded by the absolute cap least(row.timeout_ms,30000) [r42 errata F4]
as $fn$
declare
  -- §2.1 six-step execution flow; r42 errata F2/F3/F4/F12 applied.
  v_row        public.wh_query_templates%rowtype;
  v_params     jsonb;
  v_user_cap   int;
  v_cap        bigint;   -- [r43-F19] int8: max_rows(1e6) + int-limit cannot overflow 22003
  v_keep       int;
  v_kept       int := 0;
  v_pre_trim   bigint := 0;
  v_rows       jsonb := '[]'::jsonb;
  v_rec        record;
  v_partial    jsonb;
  v_kind       text;
  v_truncated  boolean;
  v_t0         timestamptz := clock_timestamp();
begin
  -- (1) hash-shape check: 64-char lowercase hex, else WH402 (§5.3).
  if p_template_hash is null or p_template_hash !~ '^[0-9a-f]{64}$' then
    raise exception 'wh_param_violation: p_template_hash must be 64-char lowercase hex sha256'
      using errcode = 'WH402',
            detail = 'received ' || coalesce(length(p_template_hash)::text, 'NULL')
                     || ' chars, expected 64';
  end if;

  -- (2) params normalization + 8KB gate. NULL -> '{}' BEFORE the gate (F12: a
  -- raw NULL makes the octet_length comparison itself NULL — not true — and
  -- the gate would pass unenforceably).
  v_params := coalesce(p_params, '{}'::jsonb);
  if octet_length(v_params::text) > 8192 then
    raise exception 'wh_param_violation: p_params exceeds the 8192-byte cap'
      using errcode = 'WH402', detail = octet_length(v_params::text)::text || ' bytes';
  end if;

  -- (3) PK lookup with the state filter (active+frozen executable; §5.1).
  --     retired is distinguished from missing/draft by a second targeted probe
  --     (WH401 vs WH400, §5.3); draft NEVER executes (WH400).
  select * into v_row
  from public.wh_query_templates
  where template_hash = p_template_hash
    and state in ('active', 'frozen');
  if not found then
    if exists (select 1 from public.wh_query_templates t
                where t.template_hash = p_template_hash
                  and t.state = 'retired') then
      raise exception 'wh_template_retired'
        using errcode = 'WH401', detail = 'template_hash prefix ' || left(p_template_hash, 16);
    end if;
    raise exception 'wh_template_not_found'
      using errcode = 'WH400', detail = 'template_hash prefix ' || left(p_template_hash, 16);
  end if;

  -- (3b) runtime hash re-verification BEFORE EXECUTE (F3): the seeded body
  -- must still hash to its PK, else the registry was tampered/corrupted after
  -- the §5.2 handshake — shard-integrity alarm, never retried silently.
  if encode(sha256(convert_to(v_row.body, 'UTF8')), 'hex') <> p_template_hash then
    raise exception 'wh_hash_mismatch: registry body does not hash to its template_hash PK'
      using errcode = 'WH403', detail = 'template_hash prefix ' || left(p_template_hash, 16);
  end if;

  -- (4) transaction-scoped GUCs — die with the request's transaction (T5: the
  -- REST plane rides the transaction pooler; a session-level SET would leak
  -- across pooled tenants). Timeout FIRST, read-only SECOND, both BEFORE the
  -- EXECUTE. statement_timeout is USERSET: this assignment WINS over the '10s'
  -- proconfig entry value and is bounded by the absolute 30000 cap (F4 — 0/-1
  -- would disable the timeout, which the registry CHECK forbids, F11).
  perform set_config('statement_timeout', least(v_row.timeout_ms, 30000)::text, true);
  perform set_config('transaction_read_only', 'on', true);

  -- (5) execute per registry kind.
  if v_row.kind = 'rows' then
    -- Sentinel cap (§2.1 step 5, F2): v_cap = least(coalesce(nullif(
    -- p_params->>'limit','')::int, row.max_rows), row.max_rows) + 1 — the
    -- caller's K can never exceed row.max_rows; +1 is the truncation sentinel.
    -- Non-integer limit (22P02, incl. 22003 overflow from the ::int cast) and
    -- negative limit re-raise WH402 (F12).
    begin
      v_user_cap := coalesce(nullif(v_params->>'limit', '')::int, v_row.max_rows);
    exception
      when invalid_text_representation or numeric_value_out_of_range then
        raise exception 'wh_param_violation: limit must be a non-negative integer'
          using errcode = 'WH402', detail = 'limit = ' || coalesce(v_params->>'limit', '<null>');
    end;
    if v_user_cap < 0 then
      -- NB: the detail parenthesizes (v_params->>'limit') — || and ->> share
      -- precedence and left-associate, so an unparenthesized
      -- 'limit = ' || v_params->>'limit' concatenates FIRST and then attempts
      -- a json parse of the result (22P02 masking the WH402 — caught live by
      -- the r43 selftest).
      raise exception 'wh_param_violation: limit must be a non-negative integer'
        using errcode = 'WH402', detail = 'limit = ' || (v_params->>'limit');
    end if;
    v_cap  := least(v_user_cap, v_row.max_rows)::bigint + 1;
    v_keep := v_cap - 1;
    for v_rec in execute v_row.body using v_params, v_cap loop
      if v_rec.row_json is null       -- [r43-F19] a NULL row_json would make
                                      -- v_rows || NULL a no-op and the envelope
                                      -- 'rows':null with rowCount>0 — a body-shape
                                      -- defect surfacing on data; WH500, never
                                      -- a silently-degraded partial.
         or v_rec._pre_trim is null then  -- [r43 audit-a F-C] the unguarded twin:
                                          -- 'truncated': null would flow past the
                                          -- engine's truncated_groupby gate (a
                                          -- boolean-consuming check).
        raise exception 'wh_body_shape_violation: rows-kind body emitted a NULL row_json/_pre_trim'
          using errcode = 'WH500', detail = 'template_hash prefix ' || left(p_template_hash, 16);
      end if;
      v_pre_trim := v_rec._pre_trim;    -- the body's own window count (constant per execution)
      if v_kept < v_keep then           -- trim rows[] to the cap, never past it
        v_rows := v_rows || v_rec.row_json;
        v_kept := v_kept + 1;
      end if;
    end loop;
    -- truncated is DERIVED from the body's pre-trim window, never from the
    -- returned row count alone (F2: an aggregate-wrapped body would hide the
    -- trim from a row-count sniff; the wrapper trusts _pre_trim instead).
    v_truncated := (v_pre_trim > v_kept);
    -- Envelope kind = merge-semantic class (§6.2 F7), NOT the registry kind:
    -- rows-kind declaring topk merges by the K-way-heap algebra; any other
    -- rows-kind is a grouped partial.
    v_kind := case when v_row.merge_ops @> '["topk"]'::jsonb then 'topk' else 'grouped' end;
    return jsonb_build_object(
      'v',                   1,
      'table',               v_row.logical_table,
      'schema_version',      v_row.schema_version,
      'qc_class',            v_row.qc_class,
      'kind',                v_kind,
      'groupKeys',           v_row.group_keys,
      'aggs',                v_row.aggs,
      'rows',                v_rows,
      'rowCount',            v_kept,
      'truncated',           v_truncated,
      'encoding',            v_row.encoding,
      'template_hash',       p_template_hash,
      'template_timeout_ms', v_row.timeout_ms,
      'latencyMs',           greatest(0, (extract(epoch from (clock_timestamp() - v_t0)) * 1000))::int
    );
  else
    -- scalar kind (F7): exactly one row — now ENFORCED (r43-F19: INTO STRICT;
    -- zero-row => no_data_found, multi-row => too_many_rows, both re-raised
    -- WH500 wh_body_shape_violation — a silent row-1-pick or 'partial':null
    -- would break the merge algebra's E2/E7 premises). The single-row object
    -- rides verbatim under `partial`.
    begin
      execute v_row.body into strict v_partial using v_params;
    exception
      when no_data_found or too_many_rows then
        raise exception 'wh_body_shape_violation: scalar body must emit exactly one row'
          using errcode = 'WH500', detail = 'template_hash prefix ' || left(p_template_hash, 16);
    end;
    return jsonb_build_object(
      'v',                   1,
      'table',               v_row.logical_table,
      'schema_version',      v_row.schema_version,
      'qc_class',            v_row.qc_class,
      'kind',                'scalar',
      'aggs',                v_row.aggs,
      'partial',             v_partial,
      'rowCount',            1,
      'truncated',           false,
      'encoding',            v_row.encoding,
      'template_hash',       p_template_hash,
      'template_timeout_ms', v_row.timeout_ms,
      'latencyMs',           greatest(0, (extract(epoch from (clock_timestamp() - v_t0)) * 1000))::int
    );
  end if;
end
$fn$;

alter function public.wh_query(text, jsonb) owner to wh_executor;

revoke create on schema public from wh_executor;

comment on function public.wh_query(text, jsonb) is
'wh_query escape hatch v1 (design_wh_query_rpc.md §2.1): (template_hash, params) -> §6.2 partial envelope. SECURITY DEFINER owned by wh_executor (SELECT-only; L1 wall). Body = registry text from wh_query_templates; template_hash = sha256 of the seeded body bytes, RE-VERIFIED before every EXECUTE (WH403, F3). L4 wall = service_role role-GUC statement_timeout 30s (re-arms per statement, r43-F18) + transaction-scoped transaction_read_only (T5); per-template timeout_ms is documented/WP-4-observed. rows-kind bodies carry the max_rows+1 sentinel on comment-stripped effective text (r43-F19); truncated derives from the body''s _pre_trim window, never from row count (F2). Scalar bodies are STRICT (WH500 on zero/multi-row, r43-F19). Custom SQLSTATEs: WH400/401/402/403/500 (§5.3 + r43-F19). EXECUTE: service_role only (0012 triple-revoke).';

-- ---------- (10-12) EXECUTE lockdown — 0012 doctrine verbatim ----------------
-- (a) revoke the Postgres default (EXECUTE -> PUBLIC at CREATE) ...
revoke execute on function public.wh_query(text, jsonb) from public;
-- (b) ... and the explicit Supabase-vintage grants (a role-targeted revoke
--     does not touch PUBLIC's ACL entry — BOTH are revoked, order-free).
revoke execute on function public.wh_query(text, jsonb) from anon, authenticated;
-- (c) service_role KEEPS EXECUTE: the engine identity is the sole caller
--     (contract §4.6); the verify gate below fails loudly if this lifeline
--     breaks (0012:29-33 precedent).
grant execute on function public.wh_query(text, jsonb) to service_role;

-- ---------- L4 arming: role-scoped statement_timeout (r43-F18) ----------------
-- THE load-bearing timeout wall. A role GUC applies AT SESSION START (login)
-- and arms the statement timer PER STATEMENT — unlike the function proconfig
-- and the step-(4) set_config, which mutate the GUC mid-statement and never
-- re-arm the timer (audit-b P1-1: proconfig 500ms + pg_sleep(2) completed at
-- 2.0s). CRITICAL PG SEMANTICS (audit-b follow-up, probed): a role GUC does
-- NOT re-apply on SET ROLE — only the LOGIN role's settings load at session
-- start. Supabase's PostgREST connects as the login role `authenticator` and
-- then SET ROLEs to anon/authenticated/service_role per request, so THE BELT
-- GOES ON authenticator (the login role), with service_role as the direct-
-- login belt. 30s = the design's absolute ceiling (F4); per-template values
-- stay documented in the registry and remain WP-4-observable (whether the
-- deployed vintage re-applies GUCs on SET ROLE decides which belt carries the
-- wall — probe arms in the r43 harness prove BOTH mechanisms scratch-side).
-- PostgREST's db-statement-timeout env is the second belt (FM deploy doc).
-- Whom it bounds: the WHOLE PostgREST plane on this shard (every REST request
-- rides authenticator — a whole-plane 30s policy, not wh_query-scoped; it also
-- OVERWRITES any platform-pre-armed value — audit-a F-F, accepted: 30s is
-- strictly looser than Supabase defaults and this wave owns the knob).
-- Pool recycle (audit-a F-D): role GUCs are read at session START — recycle
-- PostgREST's pool after applying (platform restart or terminating the
-- authenticator backends), else EXISTING pool connections stay un-armed until
-- natural recycle. Whether the wave identity holds ADMIN OPTION on
-- authenticator/service_role is a WP-4-class vintage item: the DO block
-- probes it and FAILS LOUDLY naming the missing privilege (audit-a F-E)
-- rather than silently skipping the arm.
do $l4_arm$
declare
  _r record;
  _can boolean;
begin
  for _r in select rolname from pg_catalog.pg_roles
            where rolname in ('authenticator', 'service_role') loop
    -- ADMIN OPTION probe (audit-a F-E): pg_has_role has no 'admin' mode —
    -- ADMIN OPTION lives in pg_auth_members.admin_option; superusers pass.
    select rolsuper or exists (
             select 1 from pg_catalog.pg_auth_members m
              where m.member = current_user::regrole
                and m.roleid = _r.rolname::regrole
                and m.admin_option)
      into _can
      from pg_catalog.pg_roles where rolname = current_user;
    if not coalesce(_can, false) then
      raise exception '0015 L4 arm: altering role % needs ADMIN OPTION (or superuser) — the statement_timeout wall cannot be armed on this vintage (WP-4-class)', _r.rolname;
    end if;
    execute format('alter role %I set statement_timeout = ''30s''', _r.rolname);
  end loop;
end
$l4_arm$;

-- ---------- (13-18) registry lockdown (§2.2 + 0013:354-359 doctrine) ---------
-- Inventory is service-plane only (the §5.2 handshake reads it with the
-- service key; service_role bypasses RLS — verify per vintage, OQ5-class).
revoke all on public.wh_query_templates from anon, authenticated;
grant select on public.wh_query_templates to service_role;
alter table public.wh_query_templates enable row level security;

-- [IMPL-DECISION — deviation from §2.2's literal "NO policies" letter, with
-- empirical proof in the r43 selftest] RLS-with-no-policies is deny-all for
-- EVERY non-bypass, non-owner role — INCLUDING wh_executor, the function's
-- SECURITY DEFINER owner, whose step-(3) PK lookup would then return ZERO rows
-- for every hash (every call would raise WH400; the function can never work).
-- The 0013 doctrine's PURPOSE — deny anon/authenticated/public and any leaked
-- grant, while service_role keeps its bypass — is fully preserved by a single
-- wh_executor-only SELECT policy: wh_executor is NOLOGIN (unreachable except
-- through wh_query itself, whose EXECUTE is service_role-only), stays
-- SELECT-only (no ownership => no writes, T2 intact), and every other role
-- still hits the deny-all default.
drop policy if exists wh_query_templates_executor_read on public.wh_query_templates;
create policy wh_query_templates_executor_read on public.wh_query_templates
  for select to wh_executor
  using (true);

-- ---------- (19) wave-terminating schema-cache reload (r42 errata F5) --------
-- The LAST schema-affecting statement of the wave: warms PostgREST's schema
-- cache so the first wh_query call cannot 404 "not found in schema cache"
-- (indistinguishable from correct anon-rejection — see F5/WP-3). The verify
-- gates below are pure catalog reads and change nothing.
notify pgrst, 'reload schema';

-- ---------- (20-22) verify gates — 0012 style: FAIL LOUDLY -------------------
-- (20) registry exists, RLS enabled, ACLs pinned (service_role lifeline up,
--      executor read in place, anon/authenticated stripped).
do $verify_wh_registry$
declare
  v_rls boolean;
  v_n   int;
begin
  select count(*) into v_n
    from pg_class c
    join pg_namespace n on n.oid = c.relnamespace
   where n.nspname = 'public' and c.relname = 'wh_query_templates'
     and c.relkind = 'r';
  if v_n = 0 then
    raise exception '0015 verify: public.wh_query_templates missing';
  end if;

  select c.relrowsecurity into v_rls
    from pg_class c
    join pg_namespace n on n.oid = c.relnamespace
   where n.nspname = 'public' and c.relname = 'wh_query_templates';
  if not v_rls then
    raise exception '0015 verify: RLS not enabled on public.wh_query_templates';
  end if;

  if not exists (
    select 1
      from pg_policy p
     where p.polname = 'wh_query_templates_executor_read'
       and p.polrelid = 'public.wh_query_templates'::regclass
       and p.polcmd = 'r'
       and 'wh_executor'::regrole = any (p.polroles::oid[])
  ) then
    raise exception '0015 verify: executor-read policy missing on public.wh_query_templates';
  end if;

  if not has_table_privilege('service_role', 'public.wh_query_templates', 'select') then
    raise exception '0015 verify: service_role SELECT lifeline on the registry missing';
  end if;

  -- [r43-F18] the L4 wall is armed: EVERY existing REST-plane login role
  -- (authenticator) and the direct-login engine identity (service_role)
  -- carries a role-level statement_timeout that arms per statement.
  if not exists (
    select 1
      from pg_catalog.pg_db_role_setting s
      join pg_catalog.pg_roles r on r.oid = s.setrole
     where s.setconfig @> array['statement_timeout=30s']
       and r.rolname in ('authenticator', 'service_role')
  ) then
    raise exception '0015 verify: no authenticator/service_role role-GUC statement_timeout=30s (L4 un-armed)';
  end if;
  if not has_table_privilege('wh_executor', 'public.wh_query_templates', 'select') then
    raise exception '0015 verify: wh_executor SELECT on the registry missing';
  end if;
  if has_table_privilege('anon', 'public.wh_query_templates', 'select')
     or has_table_privilege('authenticated', 'public.wh_query_templates', 'select') then
    raise exception '0015 verify: anon/authenticated retain privileges on the registry';
  end if;
end
$verify_wh_registry$;

-- (21) executor function exists, owned by wh_executor, proconfig pinned.
do $verify_wh_fn$
declare
  v_owner text;
  v_cfg   text[];
begin
  if to_regprocedure('public.wh_query(text,jsonb)') is null then
    raise exception '0015 verify: public.wh_query(text,jsonb) missing';
  end if;

  select p.proowner::regrole::text, p.proconfig
    into v_owner, v_cfg
    from pg_proc p
   where p.oid = to_regprocedure('public.wh_query(text,jsonb)');

  if v_owner <> 'wh_executor' then
    raise exception '0015 verify: wh_query owner is %, expected wh_executor', v_owner;
  end if;
  if v_cfg is null
     or not exists (select 1 from unnest(v_cfg) e where e = 'statement_timeout=10s') then
    raise exception '0015 verify: wh_query proconfig statement_timeout=10s missing';
  end if;
  if v_cfg is null
     or not exists (select 1 from unnest(v_cfg) e where e like 'search_path=%pg_catalog%') then
    raise exception '0015 verify: wh_query proconfig search_path missing';
  end if;
end
$verify_wh_fn$;

-- (22) EXECUTE ACLs: revoked from PUBLIC + anon/authenticated, granted to
--      service_role. Uses acldefault() so a NULL proacl (which means the
--      PUBLIC-by-default grant is still in force) is judged correctly.
do $verify_wh_acl$
declare
  v_oid  oid;
  v_pub  boolean;
  v_anon boolean;
  v_auth boolean;
  v_srv  boolean;
begin
  v_oid := to_regprocedure('public.wh_query(text,jsonb)');
  if v_oid is null then
    raise exception '0015 verify: public.wh_query(text,jsonb) missing';
  end if;

  select bool_or(a.privilege_type = 'EXECUTE' and a.grantee = 0),
         bool_or(a.privilege_type = 'EXECUTE' and r.rolname = 'anon'),
         bool_or(a.privilege_type = 'EXECUTE' and r.rolname = 'authenticated'),
         bool_or(a.privilege_type = 'EXECUTE' and r.rolname = 'service_role')
    into v_pub, v_anon, v_auth, v_srv
    from pg_proc p
    cross join lateral aclexplode(coalesce(p.proacl, acldefault('f', p.proowner))) a
    left join pg_roles r on r.oid = a.grantee
   where p.oid = v_oid;

  if coalesce(v_pub, false) then
    raise exception '0015 verify: wh_query EXECUTE still granted to PUBLIC (0012 hole open)';
  end if;
  if coalesce(v_anon, false) or coalesce(v_auth, false) then
    raise exception '0015 verify: wh_query EXECUTE still granted to anon/authenticated';
  end if;
  if not coalesce(v_srv, false) then
    raise exception '0015 verify: wh_query EXECUTE lifeline for service_role missing';
  end if;
end
$verify_wh_acl$;
