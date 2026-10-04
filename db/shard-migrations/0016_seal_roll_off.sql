-- =============================================================================
-- 0016_seal_roll_off.sql — v1 SEAL-ONLY roll-off, shard side (r44)
-- =============================================================================
-- Spec: research/findings_wh_catalog_contract.md §2.3 (roll-off state machine;
-- ONE watermark constant roll_off_threshold_pct=0.80 against ~450MB usable
-- => fire at ~360MB on-disk FM-side) + the §5 build-order row #5: "seal-only
-- first (facts_blocks in-shard — implement the columnar doc §1.4 DDL verbatim;
-- 0.80 threshold, 15% seal trigger in-shard only); checksum-before-DELETE
-- invariant asserted in the job itself, not reviewed afterwards." Exit
-- criterion (§5 #5): forced roll-off preserves the key-set checksum
-- end-to-end and the FM cold_objects row lands 'promoted' (FM side = FM
-- 0016 fm_rolloff_finalize; orchestration cron is PAT-gated).
--
-- APPLY TARGET: each SERVING SHARD via the Supabase Management SQL API
-- (POST /v1/projects/{ref}/database/query) — NOT the Fleet Manager project.
-- Lives in db/shard-migrations/ for the same reason 0015 does (migrate.sh
-- auto-globs db/migrations/*.sql for the FM project only).
--
-- VERBATIM LAW: the facts_blocks DDL + unpack_block below are the §1.4 code
-- fence (research/findings_wh_columnar_loading.md:87-108) transplanted
-- VERBATIM — "that DDL is the source of truth, do not re-derive". The ONLY
-- deviations are ledgered here (both no-ops on first apply; required by the
-- idempotent-re-apply doctrine every shard migration carries):
--   D1: `create table facts_blocks`       -> `create table if not exists facts_blocks`
--   D2: `create index on facts_blocks (dataset, day);`
--       -> `create index if not exists facts_blocks_dataset_day_idx on facts_blocks (dataset, day);`
--       (the fence's unnamed form auto-generates exactly this PG name —
--       <table>_<cols>_idx — so naming it changes nothing on disk; it makes
--       re-apply legal and the verify-gate name-pinnable.)
--
-- PAYLOAD ADAPTATION (r43 harness precedent, audit/r43_0015_harness/
-- selftest_0015.py:277-292): the verbatim unpack_block casts
-- (b.payload->>'k')::timestamptz[] etc. A jsonb ARRAY renders as [..] which
-- that cast REJECTS (22P02, probed r43). Writers of this table therefore
-- store each value column as PG ARRAY LITERAL TEXT (a typed array's ::text
-- render IS that literal: {"2026-01-05 00:00:00+00",...}). One EXTRA payload
-- key is carried: 'id' — the source facts_events identity PK array — which
-- unpack_block ignores. The day-level key-set checksum recipe =
-- md5-of-sorted-PK over that array (contract §1.3 cold_objects.checksum:
-- "duplicates are invisible to COUNT — the checksum is the only honest
-- verifier"). timestamptz literal round-trip assumes ISO DateStyle
-- (PostgREST sessions default) — pinned here as a v1 invariant.
-- KNOWN LIMITATION (contract-honest): the key-set checksum does NOT detect
-- content tamper (a mutated 'v' inside a block still checksums green) — the
-- §1.3 recipe deliberately pins the KEY SET; content fidelity is by
-- construction (one INSERT..SELECT transpose) and pinned by the round-trip
-- battery, not by runtime verify.
--
-- THE SEAL PATH (in-shard; the roll-off job = ONE call = ONE transaction):
--   seal_roll_off_day(p_day): pack (transpose, chunked at 1000 events/block,
--   supersede-replace existing blocks) -> re-verify the key-set checksum
--   FROM THE BLOCKS against the source (mismatch => RAISE => txn aborts =>
--   source rows survive: the delete-guard lives in the job, not in review)
--   -> DELETE the day's source rows. VACUUM cannot run inside a transaction:
--   it stays a follow-up orchestration step (PAT-gated; pg_repack available
--   for bloat, columnar §1.4). Peak = rows + blocks momentarily (~1.4x,
--   contract §3 R1).
--   Idempotent semantics: source empty + blocks present => 'already_done'
--   with checksum recomputed FROM BLOCKS and from_blocks:true — honest
--   reporting: that checksum was NOT compared against a source (there is
--   none); FM orchestration must never feed a from_blocks:true checksum to
--   fm_rolloff_finalize as if freshly verified.
--   In-shard single-writer serialization per (dataset,day) =
--   pg_advisory_xact_lock (xact-scoped, re-entrant): the critical section IS
--   this one statement, so the lock spans it exactly (SKILL §10 cross-writer
--   law's app-lock caveat applies only to PostgREST/Edge-spanned sections).
--
-- WATERMARK CONSTANTS (in-shard vs FM-side — contract §2.3, ONE doctrine):
--   15% free-headroom SEAL trigger: seal_headroom_below() = size > 425000000
--   (85% of 500MB) — seals partitions early in-shard; NEVER a rotation
--   threshold. The 0.80 roll-off decision is FM-side (FM 0016 config rows).
--
-- COLD-PATH GRANTS: wh_query cold templates aggregate over unpack_block()
-- (design_wh_query_rpc.md C1) — SELECT on facts_blocks + EXECUTE on
-- unpack_block granted to wh_executor AT BIRTH here (role may not exist on a
-- bare shard pre-0015: each grant is DO-guarded on undefined_object; the
-- seed wave may repeat them harmlessly).
--
-- OBJECT_PATH PIN (FM cold_objects.object_path for kind='packed_blocks'):
--   'public.facts_blocks[YYYY-MM-DD,YYYY-MM-DD+1)'  (half-open, per-day).
--
-- SPLITTABILITY CONTRACT: identical to 0015 — splits on top-level
-- semicolons with dollar-quote awareness; each statement = one
-- Management-API call. Idempotent re-apply. 20 statements.
-- =============================================================================

-- ---------- (1) facts_blocks — VERBATIM §1.4 fence (D1 ledgered) ------------
create table if not exists facts_blocks (
  block_id   bigint generated always as identity primary key,
  dataset    text  not null,
  day        date  not null,
  n_rows     int   not null,
  ts_min     timestamptz not null, ts_max timestamptz not null,
  dev_min    int4, dev_max int4,                 -- min/max pruning keys (block-level zone map)
  payload    jsonb not null,                     -- {"ts":[...],"d":[...],"m":[...],"v":[...]}
  sketch_hll bytea,                              -- shard-kit HLL over device_id
  stats_m2   bytea                               -- count, mean, M2 per metric (mergeable variance)
);
alter table facts_blocks alter column payload set compression lz4;
create index if not exists facts_blocks_dataset_day_idx on facts_blocks (dataset, day);      -- BRIN(ts_min) if scanned by time
-- pack: transpose sealed day-partition via pg_cron job; unpack via SQL table function
create or replace function unpack_block(b facts_blocks)
returns table (ts timestamptz, device_id int4, metric text, value float8)
language sql immutable as $$
  select unnest((b.payload->>'ts')::timestamptz[]),   -- PG10+ expands multiple SELECT-list
         unnest((b.payload->>'d')::int4[]),           -- SRFs in lockstep ⇒ row-aligned transpose
         unnest((b.payload->>'m')::text[]),
         unnest((b.payload->>'v')::float8[])
$$;

-- ---------- (2) canonical facts source v1 (the seal target) -----------------
-- §1.4 pattern: facts_<dataset> PARTITION BY RANGE(day) — v1 ships the
-- canonical instance (ts, device_id, metric, value) + identity PK (the
-- checksum recipe needs a PK; duplicates invisible to COUNT). The loader
-- (r42 v1 transports) and ingest paths land rows here; per-dataset variants
-- are v2 (generated per-table seal fns, static SQL — no dynamic identifiers).
create table if not exists facts_events (
  id        bigint generated always as identity,
  ts        timestamptz not null,
  device_id int4  not null,
  metric    text  not null,
  value     float8 not null,
  primary key (id, ts)
) partition by range (ts);
create index if not exists facts_events_brin on facts_events using brin (ts);

create or replace function seal_ensure_day_partition(p_day date)
returns void language plpgsql as $fn$
begin
  -- Dynamic partition DDL with NO caller-controlled identifiers: the name is
  -- derived from the typed date (to_char), the bounds are %L-quoted typed
  -- dates — no injection surface (SKILL _quote_ident law: static SQL or a
  -- typed-parameter format() are the only safe shapes).
  -- Bounds pinned to UTC explicitly: a BARE date bound is cast to
  -- timestamptz per the CREATING SESSION's TimeZone at DDL time — a non-UTC
  -- session would freeze a shifted partition boundary in stone.
  execute format(
    'create table if not exists %I partition of facts_events for values from (%L) to (%L)',
    'facts_events_' || to_char(p_day, 'YYYYMMDD'),
    (p_day::timestamp at time zone 'UTC'), ((p_day + 1)::timestamp at time zone 'UTC'));
end
$fn$;

-- ---------- (3) 15% headroom seal trigger (in-shard ONLY) -------------------
create or replace function seal_headroom_below(p_size_bytes bigint default null)
returns boolean language sql volatile as $fn$
  -- 425000000 = 85% of 500MB (decimal MB, the platform's quota unit): free
  -- headroom < 15% (columnar §1.4 sealing policy (iii)). The 0.80 roll-off
  -- decision lives FM-side (FM 0016 config) — this trigger NEVER doubles as
  -- a rotation threshold (contract §2.3 ONE-watermark doctrine).
  select coalesce(p_size_bytes, pg_database_size(current_database())) > 425000000
$fn$;

-- ---------- (4) the pack half (shared by seal + roll-off) -------------------
create or replace function seal_pack_blocks(p_day date)
returns jsonb language plpgsql as $fn$
declare
  v_count        bigint;
  v_checksum     text;
  v_tsmin        timestamptz;
  v_tsmax        timestamptz;
  v_devmin       int4;
  v_devmax       int4;
  v_blocks       bigint;
  v_nrows        bigint;
  v_payload_bytes bigint;
  v_blocks_present bigint;
begin
  perform pg_advisory_xact_lock(hashtextextended('seal:events:' || p_day::text, 0));

  select count(*),
         md5(coalesce(string_agg(id::text, ',' order by id), '')),
         min(ts), max(ts), min(device_id), max(device_id)
    into v_count, v_checksum, v_tsmin, v_tsmax, v_devmin, v_devmax
    from facts_events
   where ts >= (p_day::timestamp at time zone 'UTC')
     and ts < ((p_day + 1)::timestamp at time zone 'UTC');

  if v_count = 0 then
    -- Honest 'empty': a day already rolled off has blocks but no source —
    -- report blocks_present so the caller cannot mistake it for virgin state.
    select count(*) into v_blocks_present
      from facts_blocks where dataset = 'events' and day = p_day;
    return jsonb_build_object('status', 'empty', 'day', p_day,
                              'blocks_present', v_blocks_present);
  end if;

  -- Supersede: re-pack replaces any existing block set for the day (the FM
  -- cold_objects 'dropped' state rides the FM-side supersede path).
  delete from facts_blocks where dataset = 'events' and day = p_day;

  with src as (
    select id, ts, device_id, metric, value,
           ((row_number() over (order by id) - 1) / 1000) + 1 as chunk
      from facts_events
     where ts >= (p_day::timestamp at time zone 'UTC')
       and ts < ((p_day + 1)::timestamp at time zone 'UTC')
  ), packed as (
    select chunk,
           count(*)             as n_rows,
           min(ts)              as ts_min,
           max(ts)              as ts_max,
           min(device_id)       as dev_min,
           max(device_id)       as dev_max,
           jsonb_build_object(
             'ts', (array_agg(ts order by id))::text,
             'd',  (array_agg(device_id order by id))::text,
             'm',  (array_agg(metric order by id))::text,
             'v',  (array_agg(value order by id))::text,
             'id', (array_agg(id order by id))::text
           ) as payload
      from src
     group by chunk
  )
  insert into facts_blocks (dataset, day, n_rows, ts_min, ts_max, dev_min, dev_max, payload)
  select 'events', p_day, n_rows, ts_min, ts_max, dev_min, dev_max, payload
    from packed;

  select count(*), sum(n_rows), sum(octet_length(payload::text))
    into v_blocks, v_nrows, v_payload_bytes
    from facts_blocks
   where dataset = 'events' and day = p_day;

  return jsonb_build_object('status', 'packed', 'day', p_day,
    'blocks', v_blocks, 'n_rows', v_nrows, 'checksum', v_checksum,
    'ts_min', v_tsmin, 'ts_max', v_tsmax, 'dev_min', v_devmin, 'dev_max', v_devmax,
    'from_blocks', false, 'payload_bytes', v_payload_bytes);
end
$fn$;

-- ---------- (5) block-side checksum (the verify oracle) ---------------------
create or replace function seal_blocks_checksum(p_day date)
returns text language plpgsql stable as $fn$
declare
  v_no_id bigint;
  v_result text;
begin
  -- r44 audit P2-1: a block written without the 'id' payload key (a
  -- fence-faithful writer — the verbatim §1.4 fence has no 'id') would make
  -- the md5-of-sorted-PK silently checksum NOTHING (md5('')). Guard: every
  -- block of the day MUST carry 'id'.
  select count(*) into v_no_id
    from facts_blocks
   where dataset = 'events' and day = p_day and not (payload ? 'id');
  if v_no_id > 0 then
    raise exception 'seal_blocks_checksum: % block(s) for % lack the id payload key — checksum would be vacuous', v_no_id, p_day;
  end if;
  -- md5-of-sorted-PK recomputed FROM the blocks' 'id' payload arrays.
  -- Zero rows => NULL: callers must treat NULL as "no blocks", never as a
  -- checksum.
  select md5(coalesce(string_agg(x.k::text, ',' order by x.k), ''))
    into v_result
    from (
      select unnest((payload->>'id')::int8[]) as k
        from facts_blocks
       where dataset = 'events' and day = p_day
    ) x
   having exists (select 1 from facts_blocks where dataset = 'events' and day = p_day);
  return v_result;
end
$fn$;

-- ---------- (6) nightly seal (pack, NO delete) ------------------------------
create or replace function seal_pack_day(p_day date)
returns jsonb language plpgsql as $fn$
declare v_result jsonb;
begin
  v_result := seal_pack_blocks(p_day);
  return v_result;
end
$fn$;

-- ---------- (7) standalone verify (the reconciliation arm) ------------------
create or replace function seal_verify_day(p_day date, p_expected_checksum text)
returns integer language plpgsql as $fn$
declare
  v_blocks   bigint;
  v_checksum text;
begin
  select count(*) into v_blocks
    from facts_blocks where dataset = 'events' and day = p_day;
  if v_blocks = 0 then
    raise exception 'seal_verify_day: no blocks for %', p_day;
  end if;
  v_checksum := seal_blocks_checksum(p_day);
  if v_checksum is distinct from p_expected_checksum then
    -- No DELETE path is reachable from this function — raising here IS the
    -- guard (contract §2.3: checksum re-verified BEFORE any DELETE).
    raise exception 'seal_checksum_mismatch: expected %, blocks give %',
      p_expected_checksum, v_checksum;
  end if;
  return v_blocks::integer;
end
$fn$;

-- ---------- (8) THE roll-off job (pack -> verify -> DELETE, one txn) --------
create or replace function seal_roll_off_day(p_day date)
returns jsonb language plpgsql as $fn$
declare
  v_source    jsonb;
  v_pack      jsonb;
  v_blocksum  text;
  v_blocks    bigint;
  v_nrows     bigint;
  v_payload_bytes bigint;
  v_tsmin     timestamptz;
  v_tsmax     timestamptz;
  v_devmin    int4;
  v_devmax    int4;
begin
  perform pg_advisory_xact_lock(hashtextextended('seal:events:' || p_day::text, 0));

  select jsonb_build_object(
           'count', count(*),
           'checksum', md5(coalesce(string_agg(id::text, ',' order by id), '')),
           'ts_min', min(ts), 'ts_max', max(ts),
           'dev_min', min(device_id), 'dev_max', max(device_id))
    into v_source
    from facts_events
   where ts >= (p_day::timestamp at time zone 'UTC')
     and ts < ((p_day + 1)::timestamp at time zone 'UTC');

  select count(*), sum(n_rows) into v_blocks, v_nrows
    from facts_blocks
   where dataset = 'events' and day = p_day;

  if (v_source->>'count')::bigint = 0 then
    if v_blocks > 0 then
      -- Honest already-done: checksum recomputed FROM BLOCKS ONLY (no source
      -- truth exists). from_blocks:true = never feed to fm_rolloff_finalize
      -- as if freshly source-verified.
      return jsonb_build_object('status', 'already_done', 'day', p_day,
        'blocks', v_blocks, 'n_rows', v_nrows,
        'checksum', seal_blocks_checksum(p_day), 'from_blocks', true);
    end if;
    return jsonb_build_object('status', 'empty', 'day', p_day);
  end if;

  -- Pack (supersedes any existing block set; advisory xact lock re-entrant).
  v_pack := seal_pack_blocks(p_day);

  -- THE INVARIANT, asserted in the job itself (contract §2.3): the block
  -- side must reproduce the source key-set checksum BEFORE any DELETE.
  v_blocksum := seal_blocks_checksum(p_day);
  if v_blocksum is distinct from (v_source->>'checksum') then
    raise exception 'seal_checksum_mismatch: source %, blocks % (delete guarded — source rows survive)',
      (v_source->>'checksum'), v_blocksum;
  end if;

  -- THE DELETE IS BOUNDED TO THE VERIFIED KEY SET (r44 audit P0): the
  -- range-DELETE could silently eat rows a concurrent ingest committed
  -- between the pack snapshot and the delete (they were never packed, never
  -- verified — md5-of-sorted-PK over a snapshot cannot see them). Deleting
  -- by the packed id arrays makes the delete = the verified set EXACTLY.
  delete from facts_events e
   where e.ts >= (p_day::timestamp at time zone 'UTC')
     and e.ts < ((p_day + 1)::timestamp at time zone 'UTC')
     and e.id in (
       select unnest((b.payload->>'id')::int8[])
         from facts_blocks b
        where b.dataset = 'events' and b.day = p_day);

  -- Post-DELETE drift assert: any row left in the day window committed AFTER
  -- the pack snapshot (concurrent ingest). RAISE aborts THIS job (source
  -- rows restored by rollback); the late rows stay — the next roll-off
  -- re-packs and includes them. Silent loss is impossible by construction.
  declare
    v_left bigint;
    v_deleted bigint;
  begin
    get diagnostics v_deleted = row_count;
    if v_deleted <> v_nrows then
      raise exception 'seal_delete_drift: deleted % but verified % key(s) — concurrent ingest drift (job aborted, source survives)',
        v_deleted, v_nrows;
    end if;
    select count(*) into v_left
      from facts_events
     where ts >= (p_day::timestamp at time zone 'UTC')
       and ts < ((p_day + 1)::timestamp at time zone 'UTC');
    if v_left > 0 then
      raise exception 'seal_delete_drift: % unverified row(s) landed in the day window during roll-off (job aborted, source survives)',
        v_left;
    end if;
  end;

  -- Typed re-extraction: jsonb_build_object renders BIGINT args as JSON
  -- NUMBERS — never re-wrap values pulled with ->> (that yields JSON
  -- strings; the FM orchestration consumes typed numbers).
  v_blocks        := (v_pack->>'blocks')::bigint;
  v_nrows         := (v_pack->>'n_rows')::bigint;
  v_payload_bytes := coalesce((v_pack->>'payload_bytes')::bigint, 0);
  v_tsmin         := (v_source->>'ts_min')::timestamptz;
  v_tsmax         := (v_source->>'ts_max')::timestamptz;
  v_devmin        := (v_source->>'dev_min')::int4;
  v_devmax        := (v_source->>'dev_max')::int4;

  return jsonb_build_object('status', 'rolled_off', 'day', p_day,
    'blocks', v_blocks, 'n_rows', v_nrows,
    'checksum', v_blocksum,
    'ts_min', v_tsmin, 'ts_max', v_tsmax,
    'dev_min', v_devmin, 'dev_max', v_devmax,
    'from_blocks', false, 'payload_bytes', v_payload_bytes);
end
$fn$;

-- ---------- (9) cold-path grants at birth (0015's wave may repeat) ----------
do $fn$
begin
  grant select on facts_blocks to wh_executor;
exception
  when undefined_object then null;  -- bare shard pre-0015: no wh_executor yet
end $fn$;
do $fn$
begin
  grant execute on function public.unpack_block(b public.facts_blocks) to wh_executor;
exception
  when undefined_object then null;
end $fn$;

-- ---------- (10) verify-gates (structural; each one statement) --------------
do $fn$
begin
  if (select a.attcompression
        from pg_catalog.pg_attribute a
       where a.attrelid = 'facts_blocks'::regclass
         and a.attname = 'payload') is distinct from 'l' then
    raise exception '0016 verify: facts_blocks.payload compression is not lz4 (TOAST gate, columnar §1.7)';
  end if;
end $fn$;

do $fn$
declare v_vol "char";
begin
  select p.provolatile into v_vol
    from pg_catalog.pg_proc p
    join pg_catalog.pg_namespace n on n.oid = p.pronamespace
   where p.proname = 'unpack_block' and n.nspname = 'public';
  if v_vol is distinct from 'i' then
    raise exception '0016 verify: unpack_block missing or not immutable';
  end if;
end $fn$;

do $fn$
begin
  if not exists (select 1 from pg_catalog.pg_class c
                  where c.oid = 'facts_events'::regclass
                    and c.relkind = 'p') then
    raise exception '0016 verify: facts_events is not a partitioned table';
  end if;
end $fn$;

do $fn$
declare v_name text;
begin
  foreach v_name in array array[
    'seal_ensure_day_partition', 'seal_headroom_below', 'seal_pack_blocks',
    'seal_blocks_checksum', 'seal_pack_day', 'seal_verify_day', 'seal_roll_off_day']
  loop
    if not exists (select 1
                     from pg_catalog.pg_proc p
                     join pg_catalog.pg_namespace n on n.oid = p.pronamespace
                    where p.proname = v_name and n.nspname = 'public') then
      raise exception '0016 verify: seal function % missing', v_name;
    end if;
  end loop;
end $fn$;

do $fn$
begin
  if not exists (select 1 from pg_catalog.pg_class c
                  where c.relname = 'facts_blocks_dataset_day_idx'
                    and c.relkind = 'i') then
    raise exception '0016 verify: facts_blocks_dataset_day_idx missing (D2 ledgered name)';
  end if;
end $fn$;
