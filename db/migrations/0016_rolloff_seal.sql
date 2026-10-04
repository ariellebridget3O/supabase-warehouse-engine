-- =============================================================================
-- 0016_rolloff_seal.sql — v1 SEAL-ONLY roll-off, FM catalog side (r44)
-- =============================================================================
-- Spec: research/findings_wh_catalog_contract.md §2.3 + §5 build-order #5:
-- the ONE watermark constant `roll_off_threshold_pct = 0.80` "against ~450MB
-- usable => fire at ~360MB on-disk" — this REPLACES the three drifting
-- numbers in prior docs (patterns §8.4 "400MB", columnar §1.4 "15%
-- headroom", patterns §7.3 "400MB watermark"); the 15% figure survives ONLY
-- as the in-shard seal trigger (shard-side 0016 seal_headroom_below) and
-- must never again be read as a separate rotation threshold.
--
-- This migration ships the FM-side HALF of v1 seal-only roll-off:
--   * the watermark constants as config rows (jsonb-SCALAR encoding, 0013)
--   * v_warehouse_rolloff_candidates — the selection view (watermark +
--     §2.4 rotation guard: a project with ANY placement in
--     copying|draining|loading is excluded — the Outgoing-Path-Guard law;
--     EVERY consumer of the selection predicate carries the exclusion)
--   * fm_rolloff_finalize — the ONE endpoint that lands a completed in-shard
--     roll-off (seal_roll_off_day on the shard, applied by the PAT-gated
--     orchestration cron) into the catalog: cold_objects row AT 'promoted'
--     (the §2.3 exit criterion) + placement bytes_used reset. Directory
--     version bump rides the 0013 wh_cold_bump triggers AUTOMATICALLY —
--     this RPC never bumps manually.
-- The Storage-export half (exported state, R2) is v2 — 'sealed'/'exported'
-- states stay reserved for it (0013 state machine unchanged).
--
-- CONVENTIONS: config seeds = jsonb-SCALAR encoding (0013: read via
-- (value #>> '{}'), write via to_jsonb(scalar)); fm_rolloff_finalize is
-- SECURITY INVOKER service-plane (0014 loader precedent — service_role
-- invokes with its own bypass-RLS rights); every body ref schema-qualified;
-- lockdown = the 0012/0014 doctrine triple revoke (public + anon +
-- authenticated — default privileges grant EXECUTE to anon/authenticated
-- explicitly, so revoking public alone is NOT sufficient on a real project).
--
-- SPLITTABILITY CONTRACT: identical to 0013/0014 — top-level semicolons,
-- dollar-quote aware; each statement = one Management-API call. Idempotent
-- re-apply. 10 statements (r45 shape pass added the legacy-13-arg drop: a
-- create-or-replace with a CHANGED arg list creates an OVERLOAD, not a
-- replacement — the old unguarded body would stay callable and to_regproc
-- in the migrate.sh gates would missolve).
--
-- r45 SHAPE PASS (the 3 r44-audit-deferred P2s, one coherent signature
-- change): (a) p_from_blocks provenance param — fail-closed machine guard
-- (null or true => raise); (b) promoted day-range overlap guard — one
-- promoted row per (table, day), daterange && over the parsed object_path
-- bounds, serialized by a per-table advisory xact lock; (c) p_placement_id
-- disambiguation — refuse-to-guess when >1 serving placement shares the
-- (table, project). Signature: 13 -> 15 args (both new params appended,
-- poison-pill defaults: callers MUST surface p_from_blocks; p_placement_id
-- is optional-but-verified).
-- =============================================================================

-- ---------- (1) watermark constants (jsonb-SCALAR, 0013 encoding) -----------
insert into public.config (key, value, description) values
  ('roll_off_threshold_pct', to_jsonb(0.80::numeric),
   'THE roll-off watermark (contract §2.3): fire roll-off when placement.bytes_used >= pct * warehouse_usable_bytes => 0.80 * 450000000 = 360000000 on-disk. Replaces the three drifting numbers (patterns §8.4 400MB, columnar §1.4 15%-headroom, patterns §7.3 400MB); the 15% figure survives ONLY as the in-shard seal trigger (shard-side 0016), never a rotation threshold.')
  on conflict (key) do nothing;

insert into public.config (key, value, description) values
  ('warehouse_usable_bytes', to_jsonb(450000000::bigint),
   'Usable DB bytes per shard project (columnar §1.6: 500MB quota minus catalogs/WAL/temp/float => ~450MB conservative). Denominator of the roll-off watermark; NOT the 500MB hard wall.')
  on conflict (key) do nothing;

-- ---------- (2) the threshold in bytes (one derived read) -------------------
create or replace function public.warehouse_roll_off_threshold_bytes()
returns bigint
language plpgsql
stable
as $fn$
declare
  v_pct    numeric;
  v_usable bigint;
begin
  select (value #>> '{}')::numeric into v_pct
    from public.config where key = 'roll_off_threshold_pct';
  select (value #>> '{}')::bigint into v_usable
    from public.config where key = 'warehouse_usable_bytes';
  if v_pct is null or v_usable is null then
    raise exception 'roll-off watermark config missing (roll_off_threshold_pct / warehouse_usable_bytes)';
  end if;
  return (v_pct * v_usable)::bigint;
end
$fn$;

-- ---------- (3) selection view (watermark + rotation guard) -----------------
-- Predicate mirrors v_warehouse_directory (the normative serving predicate)
-- and ADDS: bytes_used >= threshold + the §2.4 guard. The guard is an
-- anti-join over the WHOLE project's placements — one loading/copying/
-- draining sibling placement excludes the project (rotate neither endpoint
-- of a reshard; orphaned-copy law).
create or replace view public.v_warehouse_rolloff_candidates as
select t.id as table_id, t.logical_name, t.schema_version as table_schema_version,
       p.id as placement_id, p.project_id, p.key_min, p.key_max, p.hash_slot,
       p.state, p.schema_version, p.row_estimate, p.bytes_used, p.last_health_at,
       pr.status as platform_status, pr.ref as project_ref
from public.warehouse_tables t
join public.warehouse_placements p on p.table_id = t.id
join public.projects pr on pr.id = p.project_id
where p.state = 'serving'
  and pr.status = 'ACTIVE_HEALTHY'
  and p.schema_version = t.schema_version
  and p.last_health_at > now() - '90 seconds'::interval
  and p.bytes_used >= public.warehouse_roll_off_threshold_bytes()
  and not exists (
    select 1 from public.warehouse_placements p2
     where p2.project_id = p.project_id
       and p2.state in ('copying', 'draining', 'loading')
  );

-- (a) the view must execute AS THE CALLER (0007/0013 precedent: views run
--     with their owner's privileges otherwise). LOAD-BEARING ON RE-RUN:
--     create or replace view RESETS reloptions (verified PG 17.6, 0013), so
--     this alter must stay adjacent and re-applied every run.
alter view public.v_warehouse_rolloff_candidates set (security_invoker = true);
-- (b) defense in depth: strip anon/authenticated grants outright.
revoke all on public.v_warehouse_rolloff_candidates from anon, authenticated;

-- ---------- (4) fm_rolloff_finalize — the ONE catalog-write endpoint --------
-- r45 shape pass: statement (4a) drops the r44 13-arg legacy identity BEFORE
-- the 15-arg create — create-or-replace with a changed arg list OVERLOADS,
-- leaving the old unguarded body callable (silent guard bypass).
drop function if exists public.fm_rolloff_finalize(uuid, uuid, text, text, text,
    timestamptz, timestamptz, integer, integer, bigint, bigint, text, bigint);

create or replace function public.fm_rolloff_finalize(
    p_table_id       uuid,
    p_project_id     uuid,
    p_object_path    text,
    p_key_min        text,
    p_key_max        text,
    p_ts_min         timestamptz,
    p_ts_max         timestamptz,
    p_dev_min        integer,
    p_dev_max        integer,
    p_bytes          bigint,
    p_row_count      bigint,
    p_checksum       text,
    p_remaining_bytes bigint default null,
    p_placement_id   uuid default null,
    p_from_blocks    boolean default null
) returns jsonb
language plpgsql
as $fn$
declare
  v_table      public.warehouse_tables%rowtype;
  v_placement  public.warehouse_placements%rowtype;
  v_superseded boolean;
  v_cold_id    uuid;
  v_serving_ids uuid[];
  v_n_serving  bigint;
  v_range      daterange;
  v_unparseable bigint;
  v_overlap_found boolean;
  v_existing_project uuid;
begin
  -- r45 shape pass (a): from_blocks provenance — fail CLOSED on null (a
  -- caller that does not surface the flag is not verified to have checked
  -- it; same doctrine as the r44 P1 NULL-checksum fix) and machine-reject
  -- true (block-recomputed checksums have no source truth behind them).
  if p_from_blocks is null or p_from_blocks then
    raise exception 'fm_rolloff_finalize: from_blocks provenance flag must be surfaced and false (relay seal_roll_off_day''s honest response flag; only status=rolled_off responses are finalizable); got %', coalesce(p_from_blocks::text, '<null>');
  end if;
  -- Validations (raise on violation — the caller is the FM orchestration
  -- cron, a bug there must fail loudly, never land a half-recorded object).
  -- NULL-through trap (r44 audit P1): !~ is NULL-through — a NULL checksum
  -- would PASS this guard and land a 'promoted' row with the §1.3 "only
  -- honest verifier" silently absent. Fail closed on null explicitly.
  if p_checksum is null or p_checksum !~ '^[0-9a-f]{32}$' then
    raise exception 'fm_rolloff_finalize: checksum % is not md5-of-sorted-PK shape', coalesce(p_checksum, '<null>');
  end if;
  if p_object_path is null or length(p_object_path) > 200
     or p_object_path !~ '^public\.facts_blocks\[[0-9]{4}-[0-9]{2}-[0-9]{2},[0-9]{4}-[0-9]{2}-[0-9]{2}\)$' then
    raise exception 'fm_rolloff_finalize: object_path % violates the pinned packed_blocks format', coalesce(p_object_path, '<null>');
  end if;
  -- r44 audit P2-4: the regex admits zero-width ([d,d)) and REVERSED
  -- ([d2,d1)) day ranges — parse the pair and demand strictly-ascending
  -- half-open bounds.
  if (substring(p_object_path from '^public\.facts_blocks\[([0-9]{4}-[0-9]{2}-[0-9]{2}),[0-9]{4}-[0-9]{2}-[0-9]{2}\)$')::date
      >= (substring(p_object_path from '^public\.facts_blocks\[[0-9]{4}-[0-9]{2}-[0-9]{2},([0-9]{4}-[0-9]{2}-[0-9]{2})\)$')::date)) then
    raise exception 'fm_rolloff_finalize: object_path day range must be half-open ascending ([d1,d2) with d1 < d2)';
  end if;
  if p_key_min is not null and p_key_max is not null and p_key_min >= p_key_max then
    raise exception 'fm_rolloff_finalize: key bounds must be half-open ascending (key_min < key_max)';
  end if;
  if p_bytes is null or p_bytes < 0 or p_row_count is null or p_row_count < 0 then
    raise exception 'fm_rolloff_finalize: bytes/row_count must be non-negative';
  end if;
  -- r44 audit P2-3: a negative remaining-size would land bytes_used < 0
  -- (no CHECK on the column) and hold the roll-off selection hostage.
  if p_remaining_bytes is not null and p_remaining_bytes < 0 then
    raise exception 'fm_rolloff_finalize: remaining_bytes must be non-negative';
  end if;
  if (p_ts_min is null) <> (p_ts_max is null) or (p_ts_min is not null and p_ts_min > p_ts_max) then
    raise exception 'fm_rolloff_finalize: ts bounds both-null-or-both-present and ordered';
  end if;
  if (p_dev_min is null) <> (p_dev_max is null) or (p_dev_min is not null and p_dev_min > p_dev_max) then
    raise exception 'fm_rolloff_finalize: dev bounds both-null-or-both-present and ordered';
  end if;
  if (p_key_min is null) <> (p_key_max is null) then
    raise exception 'fm_rolloff_finalize: key bounds both-null-or-both-present';
  end if;

  select * into v_table from public.warehouse_tables where id = p_table_id;
  if not found then
    raise exception 'fm_rolloff_finalize: table % is not registered', p_table_id;
  end if;

  -- r45 shape pass (b): per-table advisory xact lock BEFORE the overlap
  -- check (check-then-insert serialization; lock ordering load-bearing).
  -- NOTE: hashtext('fleet_promote_slots') (0008/0009) lives in THIS SAME db
  -- — keys differ, bigint advisory locks are dboid-scoped, a collision would
  -- add contention only, never wrongness.
  perform pg_advisory_xact_lock(hashtextextended('fm_rolloff_finalize:' || p_table_id::text, 0));

  -- r45 shape pass (b) pre-check: promoted rows of this table whose
  -- object_path is unparseable/non-ascending are ALREADY directory
  -- corruption — fail loudly on them instead of silently letting the
  -- unbounded-daterange semantics (NULL bounds = full domain, probed r45)
  -- decide the overlap verdict by accident.
  select count(*) into v_unparseable
    from public.warehouse_cold_objects c
   where c.table_id = p_table_id
     and c.state = 'promoted'
     and c.object_path is distinct from p_object_path
     and ( c.object_path is null
        or c.object_path !~ '^public\.facts_blocks\[[0-9]{4}-[0-9]{2}-[0-9]{2},[0-9]{4}-[0-9]{2}-[0-9]{2}\)$'
        or (substring(c.object_path from '^public\.facts_blocks\[([0-9]{4}-[0-9]{2}-[0-9]{2})')::date
            >= substring(c.object_path from '^public\.facts_blocks\[[0-9]{4}-[0-9]{2}-[0-9]{2},([0-9]{4}-[0-9]{2}-[0-9]{2})\)$')::date) );
  if v_unparseable > 0 then
    raise exception 'fm_rolloff_finalize: % promoted row(s) of table % have an unparseable/empty day range — clean the directory before finalize', v_unparseable, p_table_id;
  end if;

  -- r45 shape pass (b): ONE promoted row per (table, day) — two different
  -- object_paths covering one day would double-count the day in the
  -- directory (r44 audit P2-e). Adjacent half-open ranges do NOT conflict
  -- (daterange && is false on touching [d1,d2)/[d2,d3) — probed r45).
  v_range := daterange(
    substring(p_object_path from '^public\.facts_blocks\[([0-9]{4}-[0-9]{2}-[0-9]{2})')::date,
    substring(p_object_path from '^public\.facts_blocks\[[0-9]{4}-[0-9]{2}-[0-9]{2},([0-9]{4}-[0-9]{2}-[0-9]{2})\)$')::date, '[)');
  select exists (
    select 1 from public.warehouse_cold_objects c
     where c.table_id = p_table_id
       and c.state = 'promoted'
       and c.object_path is distinct from p_object_path
       and daterange(
             substring(c.object_path from '^public\.facts_blocks\[([0-9]{4}-[0-9]{2}-[0-9]{2})')::date,
             substring(c.object_path from '^public\.facts_blocks\[[0-9]{4}-[0-9]{2}-[0-9]{2},([0-9]{4}-[0-9]{2}-[0-9]{2})\)$')::date,
             '[)') && v_range
  ) into v_overlap_found;
  if v_overlap_found then
    raise exception 'fm_rolloff_finalize: a promoted object of table % already covers a day in % — one promoted row per (table, day)', p_table_id, p_object_path;
  end if;

  -- r45 shape pass: supersede + cross-shard collision. The object_path
  -- carries NO shard qualifier — two shard projects sealing the same day
  -- range produce the SAME path string; re-landing it from a DIFFERENT
  -- project would silently overwrite the first shard's block record
  -- (r45 design audit P1). Same-project re-land stays legal (idempotent
  -- replay / re-seal of the same day).
  select c.project_id into v_existing_project
    from public.warehouse_cold_objects c
   where c.table_id = p_table_id and c.object_path = p_object_path;
  v_superseded := found;
  if v_superseded and v_existing_project is distinct from p_project_id then
    raise exception 'fm_rolloff_finalize: % is already cataloged from shard project % — cross-shard same-day re-land would silently supersede that shard''s blocks', p_object_path, v_existing_project;
  end if;

  -- r45 shape pass (c): refuse-to-guess placement resolution (r44 audit
  -- P2-f). Order is load-bearing: N=0 first (root-cause message), then
  -- mismatch, then ambiguity; an explicit resolution with a not-found
  -- belt closes the truth-table hole (N>=2 + matching id must reset
  -- THAT placement's bytes_used — never a silent no-op).
  select coalesce(array_agg(id order by id), '{}'::uuid[]), count(*)
    into v_serving_ids, v_n_serving
    from public.warehouse_placements
   where table_id = p_table_id and project_id = p_project_id and state = 'serving';
  if v_n_serving = 0 then
    raise exception 'fm_rolloff_finalize: no serving placement for table % on project %', p_table_id, p_project_id;
  end if;
  if p_placement_id is not null and not (p_placement_id = any(v_serving_ids)) then
    raise exception 'fm_rolloff_finalize: p_placement_id % is not a serving placement for table % on project % (serving ids: %)', p_placement_id, p_table_id, p_project_id, v_serving_ids;
  end if;
  if v_n_serving > 1 and p_placement_id is null then
    raise exception 'fm_rolloff_finalize: ambiguous: % serving placements for table % on project % — pass p_placement_id (serving ids: %); remaining_bytes cannot be attributed while ambiguous', v_n_serving, p_table_id, p_project_id, v_serving_ids;
  end if;
  select * into v_placement from public.warehouse_placements
   where id = coalesce(p_placement_id, v_serving_ids[1]);
  if not found then
    raise exception 'fm_rolloff_finalize: internal — placement % vanished between resolution and select under the advisory lock', coalesce(p_placement_id::text, v_serving_ids[1]::text);
  end if;

  insert into public.warehouse_cold_objects
    (table_id, project_id, kind, codec, object_path, key_min, key_max,
     ts_min, ts_max, dev_min, dev_max, bytes, row_count, checksum,
     schema_version, state)
  values
    (p_table_id, p_project_id, 'packed_blocks', 'jsonb+lz4', p_object_path,
     p_key_min, p_key_max, p_ts_min, p_ts_max, p_dev_min, p_dev_max,
     p_bytes, p_row_count, p_checksum, v_table.schema_version, 'promoted')
  on conflict (table_id, object_path) do update set
    state = 'promoted',
    checksum = excluded.checksum,
    bytes = excluded.bytes,
    row_count = excluded.row_count,
    ts_min = excluded.ts_min,
    ts_max = excluded.ts_max,
    dev_min = excluded.dev_min,
    dev_max = excluded.dev_max,
    schema_version = excluded.schema_version,
    project_id = excluded.project_id
  returning id into v_cold_id;

  -- bytes_used reset = stats-only write: bytes_used is excluded from the
  -- placement bump trigger's UPDATE OF list (0013), so this never bumps.
  -- r45 shape pass (c) belt: a 0-row update here (placement drained by a
  -- concurrent flip between resolution and update) must be LOUD, never a
  -- silent no-op reset. NOTE: with a table split across 2+ spans on one
  -- project, the reset lands on the PINNED span only — sibling spans'
  -- bytes_used are never written (deterministic-single approximation the
  -- r44 audit accepted; a day straddling two spans under-counts the
  -- remaining bytes — orchestration passes per-span remaining).
  update public.warehouse_placements
     set bytes_used = coalesce(p_remaining_bytes, 0)
   where id = v_placement.id;
  if not found then
    raise exception 'fm_rolloff_finalize: placement % vanished before the bytes_used reset (concurrent flip?)', v_placement.id;
  end if;

  -- NO manual bump: the 0013 wh_cold_bump_ins_del/upd triggers fired on the
  -- insert/state change above (a re-promote that writes the same state is
  -- suppressed by the WHEN-distinct clause — directory version stays put on
  -- no-op writes, r37 P3).
  return jsonb_build_object(
    'cold_object_id', v_cold_id,
    'placement_id', v_placement.id,
    'superseded', v_superseded,
    'directory_version', (select (value #>> '{}')::bigint
                            from public.config
                           where key = 'warehouse_directory_version'));
end
$fn$;

-- ---------- (5) lockdown (service-plane only; 0012/0014 doctrine) -----------
revoke all on function public.fm_rolloff_finalize(uuid, uuid, text, text, text,
    timestamptz, timestamptz, integer, integer, bigint, bigint, text, bigint,
    uuid, boolean)
    from public, anon, authenticated;

comment on function public.fm_rolloff_finalize(uuid, uuid, text, text, text,
    timestamptz, timestamptz, integer, integer, bigint, bigint, text, bigint,
    uuid, boolean) is
'Seal-only roll-off finalize (contract §2.3/§5#5): lands a completed in-shard seal_roll_off_day into the catalog — cold_objects row AT state=promoted (exit criterion) + placement bytes_used reset (stats-only). r45 shape pass: p_from_blocks is fail-closed provenance (null/true => raise — relay seal_roll_off_day''s honest flag, rolled_off responses only); one promoted row per (table, day) via daterange-overlap guard under a per-table advisory xact lock (unparseable promoted paths raise — directory corruption); cross-shard same-path re-land raises (path carries no shard qualifier); p_placement_id refuse-to-guess disambiguation when >1 serving placement shares (table, project). The directory bump rides the 0013 wh_cold_bump triggers; never bumps manually.';
