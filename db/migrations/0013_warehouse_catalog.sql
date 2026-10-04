-- =============================================================================
-- Fleet Manager — 0013_warehouse_catalog.sql
-- =============================================================================
-- Unified warehouse catalog + directory version counter, implementing
-- research/findings_wh_catalog_contract.md §1 (NORMATIVE, r37) verbatim.
-- Task ID: maxxing-r38-fm-0013.
--
-- WHAT AND WHY (r37 architecture law "availability is the scarce quota"):
-- the distributed warehouse v1 = engine host (this FM project) + 1-2 hot
-- shards. The catalog is the engine's ONLY routing truth:
--   * warehouse_tables       — logical table registry (queryable_templates
--                              replaces a SQL planner; a template absent
--                              there means the engine 4xxs at plan time).
--   * warehouse_placements   — (table, project, key-span) physical facts.
--                              org/region/ref/role are JOINed from projects,
--                              never duplicated. TWO partial unique indexes
--                              encode the cross-writer invariant (SKILL §10:
--                              storage constraints, not app locks) — branched
--                              on the DENORMALIZED is_reference column because
--                              index predicates cannot reference other tables
--                              (r37 review P0-1: a single serving-per-span
--                              index 23505-guarantees broadcast/reference
--                              tables, whose spans are all NULL/NULL).
--   * v_warehouse_directory  — the normative selection predicate spelled
--                              ONCE (r37 P1-4): serving|draining +
--                              ACTIVE_HEALTHY + schema_version match + fresh
--                              last_health_at. Every consumer (engine, loader
--                              gate, watchdog) reads THIS view; no engine-side
--                              WHERE drift. Warm/cold are intentionally NOT in
--                              the view (tier_warm derives from other states).
--   * warehouse_cold_objects — cold tier as first-class directory rows
--                              (packed_blocks v1 / parquet_file+iceberg_table
--                              v2); packed blocks MUST name their owning shard.
--   * load_jobs/load_partitions — loader ledger retargeted onto the catalog
--                              (table_name->table_id, shard_ref->placement_id
--                              FK CASCADE: purged placement takes its load
--                              history with it — union-of-writers resume law
--                              now enumerates through the directory's key).
--   * config.warehouse_directory_version + wh_bump_directory_version() —
--                              ONE global monotonic counter (per-row epochs
--                              ABOLISHED, r37 §1.5). TWO triggers per table:
--                              (a) after insert or delete — a WHEN clause
--                              cannot reference OLD on INSERT triggers (PG:
--                              "INSERT trigger's WHEN condition cannot
--                              reference OLD values", verified PG 17.6), so
--                              ins/del fire unconditionally; (b) after update
--                              OF the contract-affecting columns with WHEN
--                              (old.* is distinct from new.*) so no-op
--                              updates and stats-only refreshes never bump.
--
-- PG VERSION REQUIREMENT: `unique nulls not distinct` AND the
-- `security_invoker` reloption both need PG15+. New Supabase projects
-- verified PG 17.6 [worklog r37]. Pre-PG15 fallback for the natural key (do
-- NOT apply blindly — it changes the ON CONFLICT target): drop the inline
-- constraint, use a UNIQUE INDEX on (table_id, project_id,
-- coalesce(key_min,''), coalesce(key_max,''), coalesce(hash_slot,-1)).
--
-- FLIP-ORDERING NOTE (r37 P1-2, enforced by the indexes, not this file):
-- one_serving_per_span counts 'draining' as serving, so a reshard flip MUST
-- retire the source BEFORE promoting the target (or use the single-statement
-- CASE swap) — the naive target-first recipe trips 23505 mid-transaction by
-- design. Loud failure on a dirty directory is a FEATURE (0006 doctrine).
--
-- STALENESS THRESHOLD: the view pins '90 seconds'::interval = 2x the
-- watchdog stamp period per the contract. Config-knob generalization is
-- deferred until a second consumer needs it (YAGNI; the literal is the
-- contract's normative DDL).
--
-- SPEC ERRATA (r38 audit maxxing-r38-0013-audit; applied here AND in the
-- contract corrigendum — both defects are faithful copies of the r37 spec):
--   * P0-1: the contract's view SELECT (t.id table_id, ..., p.*) declares a
--     duplicate output column name (p.* expands to include p.table_id) — PG
--     rejects `column "table_id" specified more than once` at CREATE, so the
--     view was undeplorable as specced; p.* is expanded explicitly minus
--     p.table_id (t.id as table_id is join-equal and stays first).
--   * P0-2: the contract's single WHEN (old.* is distinct from new.*)
--     trigger is illegal on insert/delete (WHEN/OLD restriction above);
--     split into ins/del + upd triggers — bump semantics verified equivalent
--     (ins/del/change +1; stats-only UPDATE silent; no-op set state=state
--     suppressed; concurrent bumps serialize on the config row lock).
--
-- RE-RUN SAFETY (scripts/migrate.sh re-applies ALL files in order):
-- create table/index if not exists, create or replace view/function,
-- drop trigger if exists + create trigger, config seed on conflict do
-- nothing, alter table enable row level security (no-op when already on),
-- revoke (no-op when no grants remain). Pure DDL — never mutates rows.
-- The INITIAL creation of the two partial unique indexes fails loudly only
-- if a dirty directory pre-exists (impossible on a fresh catalog; if you
-- got here with duplicates, resolve them per 0009/0010 dedupe doctrine).
-- 41 statements.
-- =============================================================================

-- ---------- warehouse_tables (§1.1) -----------------------------------------
create table if not exists public.warehouse_tables (
    id               uuid primary key default gen_random_uuid(),
    logical_name     text not null unique,
    shard_key_type   text not null default 'none'
                     check (shard_key_type in ('none','hash','range','time')),
    -- 'tenant_id' | 'created_at'; NULL iff type='none'
    shard_key_column text,
    -- hash recipe, e.g. 'mod(hashtextextended(k),N)'; range/time: NULL
    -- (bounds live in placements). The loader records THE recipe that
    -- produced the fleet, not a per-job guess.
    shard_expr       text,
    -- {'colocate_with':['dims_*'],'columns':[...]}
    distribution_policy jsonb not null default '{}',
    -- NULL | group id; co-located tables share shard_expr + placements
    colocation_group text,
    -- broadcast-at-load to every serving shard
    is_reference     boolean not null default false,
    -- capability declaration, e.g.
    -- [{"class":"QC2","cold_ok":false,"aggs":["sum","count","min","max","avg"],
    --   "group_keys":["region"]}] — a template absent here means engine 4xx
    -- at plan time, never a silent mis-merge
    queryable_templates jsonb not null default '[]',
    -- bumped ONLY by the FM ddl-wave job (§2.5)
    schema_version   integer not null default 1,
    -- sum of placements; stats only, NEVER a version-bumping write
    row_estimate     bigint not null default 0,
    created_at       timestamptz not null default now(),
    check (shard_key_type = 'none' or shard_key_column is not null)
);
comment on table public.warehouse_tables is 'Logical warehouse tables (the only names clients see). Writers: FM ddl-wave + table-registration RPC; readers: engine (plan), loader (gate), watchdog.';
comment on column public.warehouse_tables.queryable_templates is 'Capability contract replacing a SQL planner (patterns §4.4 #1): unlisted query classes are rejected at plan time.';
comment on column public.warehouse_tables.shard_expr is 'Hash recipe copied into load_jobs so every loader run uses the fleet-creating recipe.';

-- ---------- warehouse_placements (§1.2) -------------------------------------
create table if not exists public.warehouse_placements (
    id            uuid primary key default gen_random_uuid(),
    table_id      uuid not null references public.warehouse_tables(id) on delete cascade,
    -- org/region/ref/role resolved via JOIN to projects — never copied here.
    -- Quota counters NOT duplicated (they live in quotas; Quota-Counter-Sync).
    project_id    uuid not null references public.projects(id) on delete cascade,
    -- half-open [key_min, key_max); canonical text; NULL/NULL = unbounded.
    -- Inclusive min / EXCLUSIVE max (E11 boundary rule rides this).
    key_min       text,
    key_max       text,
    -- hash shards: slot id in [0,N) from shard_expr
    hash_slot     integer,
    -- DENORMALIZED from warehouse_tables.is_reference, stamped by the FM
    -- writer at insert (FK-joined at write time, never client-supplied) so
    -- the partial indexes below can branch on it — index predicates cannot
    -- reference other tables (r37 P0-1).
    is_reference  boolean not null default false,
    -- platform pause/restore is NOT a state here — it lives in
    -- projects.status only
    state         text not null default 'loading'
                  check (state in ('serving','copying','draining','retired','loading','failed')),
    -- reshard/copy target -> source lineage (Vitess MoveTables). ON DELETE
    -- SET NULL: purging the source never cascades into the serving target.
    source_placement_id uuid references public.warehouse_placements(id) on delete set null,
    -- must equal warehouse_tables.schema_version to serve (directory gate)
    schema_version integer not null default 1,
    -- stats; watchdog-refreshed; NON-bumping (staleness rule scatter §9 #3)
    row_estimate  bigint not null default 0,
    -- stats; roll-off watermark input (§2.3)
    bytes_used    bigint not null default 0,
    -- watchdog stamp; staleness excludes from selection (view predicate)
    last_health_at timestamptz,
    created_at    timestamptz not null default now(),
    -- natural key; idempotent upsert anchor. NULLS NOT DISTINCT (PG15+) is
    -- REQUIRED: default NULLS DISTINCT lets two identical NULL/NULL rows
    -- both insert and ON CONFLICT never fires (loader double-insert, P1-5).
    unique nulls not distinct (table_id, project_id, key_min, key_max, hash_slot)
);
comment on table public.warehouse_placements is 'ONE placement table: (table, project, key-span). Physical-shard facts are JOINed, never duplicated (r37 unified catalog).';

-- THE cross-writer invariants at the DB layer (SKILL §10). TWO indexes —
-- one index cannot serve both shapes (P0-1).
create unique index if not exists one_serving_per_span
    on public.warehouse_placements (table_id, coalesce(key_min,''), coalesce(key_max,''), coalesce(hash_slot,-1))
    where not is_reference and state in ('serving','draining');
comment on index public.one_serving_per_span is 'NON-reference invariant: one serving/draining placement per (table, span). draining still SERVES reads until cutover.';

create unique index if not exists one_serving_ref_per_shard
    on public.warehouse_placements (table_id, project_id)
    where is_reference and state in ('serving','draining');
comment on index public.one_serving_ref_per_shard is 'REFERENCE/broadcast invariant: one serving copy PER SHARD (spans are all NULL/NULL — the other index would 23505 every legal second shard).';

create index if not exists placements_dir on public.warehouse_placements (table_id, state);

-- ---------- v_warehouse_directory (§1.2) — the normative predicate ----------
create or replace view public.v_warehouse_directory as
select t.id as table_id, t.logical_name, t.shard_key_type, t.shard_key_column, t.shard_expr,
       t.queryable_templates, t.schema_version as table_schema_version,
       p.id, p.project_id, p.key_min, p.key_max, p.hash_slot, p.is_reference, p.state,
       p.source_placement_id, p.schema_version, p.row_estimate, p.bytes_used, p.last_health_at, p.created_at,
       pr.status as platform_status, pr.ref as project_ref
from warehouse_tables t
join warehouse_placements p on p.table_id = t.id
join projects pr on pr.id = p.project_id
where p.state in ('serving','draining')
  and pr.status = 'ACTIVE_HEALTHY'
  and p.schema_version = t.schema_version
  and p.last_health_at > now() - '90 seconds'::interval;

-- (a) the view must execute AS THE CALLER (0007 precedent: views run with
--     their owner's privileges otherwise). LOAD-BEARING ON RE-RUN:
--     create or replace view RESETS reloptions (verified PG 17.6), so this
--     alter must stay adjacent and re-applied every run.
alter view public.v_warehouse_directory set (security_invoker = true);
-- (b) defense in depth: strip anon/authenticated grants outright.
revoke all on public.v_warehouse_directory from anon, authenticated;

-- ---------- warehouse_cold_objects (§1.3) -----------------------------------
create table if not exists public.warehouse_cold_objects (
    id          uuid primary key default gen_random_uuid(),
    table_id    uuid not null references public.warehouse_tables(id) on delete cascade,
    -- packed_blocks: project_id = the owning shard (blocks live IN that
    -- shard's 500MB; CHECK below makes it REQUIRED). parquet_file /
    -- iceberg_table: NULL (object lives in Storage, not a shard DB).
    project_id  uuid references public.projects(id) on delete set null,
    -- whose 1GB storage / 5GB egress pools the object bills
    storage_org_id uuid references public.orgs(id),
    kind        text not null check (kind in ('parquet_file','iceberg_table','packed_blocks')),
    -- 's3://<bucket>/<prefix>/part-*.parquet' | '<schema>.facts_blocks[#range]'
    -- | Iceberg REST-catalog coordinates (dual-catalog fact, columnar A8)
    object_path text,
    -- same half-open canonical bounds as placements
    key_min     text,
    key_max     text,
    ts_min      timestamptz,
    ts_max      timestamptz,
    -- zone maps (block-level min/max pruning keys)
    dev_min     int4,
    dev_max     int4,
    codec       text not null check (codec in ('parquet+zstd','jsonb+lz4','dict_rle_bytea')),
    bytes       bigint not null default 0,
    row_count   bigint not null default 0,
    -- md5-of-sorted-PK recipe carried through roll-off — the key-set
    -- checksum verified BEFORE DELETE is the same value recorded here
    checksum    text,
    schema_version integer not null default 1,
    -- sealed = in-shard pack done; exported = Parquet written, source rows
    -- not yet deleted; promoted = DELETE+VACUUM done (terminal); dropped =
    -- re-ingested/superseded
    state       text not null default 'sealed'
                check (state in ('sealed','exported','promoted','dropped')),
    created_at  timestamptz not null default now(),
    check (kind <> 'packed_blocks' or project_id is not null)
);
comment on table public.warehouse_cold_objects is 'Cold tier as first-class directory rows. packed_blocks -> owning shard REQUIRED; Storage-tier kinds may be project-less.';

create index if not exists cold_objects_dir on public.warehouse_cold_objects (table_id, state, key_min);
create unique index if not exists cold_objects_identity on public.warehouse_cold_objects (table_id, object_path);
comment on index public.cold_objects_identity is 'One directory row per object — re-export/supersede rides ON CONFLICT, never a second row (P2-10).';

-- ---------- loader ledger, retargeted (§1.4) --------------------------------
create table if not exists public.load_jobs (
    job_id        uuid primary key default gen_random_uuid(),
    -- was table_name text [CHANGE]: the ledger can never reference a logical
    -- table that isn't registered — schema-gate becomes an FK
    table_id      uuid not null references public.warehouse_tables(id),
    -- must equal the table's at handoff; migrate-first gate (§2.5)
    schema_version integer not null,
    -- recipe COPY (the fleet-creating recipe, not a per-job guess)
    shard_key_column text not null,
    shard_expr    text not null,
    num_shards    int not null,
    source_uri    text not null,
    row_count_est bigint,
    created_at    timestamptz default now(),
    finished_at   timestamptz
);
comment on table public.load_jobs is 'Loader job ledger. The loader is stateless — this table is the sole truth for resume (union-of-not-done predicate).';

create table if not exists public.load_partitions (
    job_id        uuid not null references public.load_jobs(job_id) on delete cascade,
    -- was shard_ref text [CHANGE]: welds the progress ledger to the directory
    placement_id  uuid not null references public.warehouse_placements(id) on delete cascade,
    partition_id  text not null,
    -- 'failed' rows stay in-state pending/loaded with last_error; terminal
    -- failed = attempts exhausted (simplified 5-state enum, §2.2)
    state         text not null default 'pending'
                  check (state in ('pending','loading','loaded','verified')),
    rows_sent     bigint default 0,
    attempts      int default 0,
    -- 10min lease: MUST exceed restore(~4min)+deploy window so a lease cannot
    -- expire mid-restore; stale lease = loader death
    lease_until   timestamptz,
    -- loader-side md5 of '\n'-joined sorted PKs (order-independent,
    -- duplicate-blind — the only honest verifier, columnar §3.4)
    checksum      text,
    -- verifier: same recipe via one SQL-API statement on the shard
    shard_checksum text,
    last_error    text,
    updated_at    timestamptz default now(),
    primary key (job_id, placement_id, partition_id)
);
comment on table public.load_partitions is 'Per-partition load ledger. Resume predicate: state=pending OR (state=loading AND lease_until<now()) — the union of not-done.';

-- ---------- directory version counter (§1.5) --------------------------------
-- ONE encoding, pinned at BOTH ends (r37 P1-3): jsonb SCALAR only.
--   seed : to_jsonb(1::bigint)          — jsonb scalar 1 (a string '1' or an
--                                          object wrapper breaks the read)
--   read : (value #>> '{}')::bigint     — scalar path yields text -> bigint
--   write: to_jsonb(<computed bigint>)  — the object variant is DROPPED: on
--                                          an object, #>> '{}' returns the
--                                          whole JSON text -> every bump
--                                          would throw (self-inconsistent)
insert into public.config (key, value, description) values
  ('warehouse_directory_version', to_jsonb(1::bigint),
   'Monotonic warehouse directory version; bumped by wh_bump_directory_version() on contract-affecting catalog writes only (stats refreshes never bump). Consumers cheap-probe this row to cache safely.')
  on conflict (key) do nothing;

create or replace function public.wh_bump_directory_version() returns trigger
language plpgsql as $$
begin
  update public.config set value = to_jsonb(((value #>> '{}')::bigint) + 1)
  where key = 'warehouse_directory_version';
  return null;
end $$;
comment on function public.wh_bump_directory_version() is 'AFTER row-level bump for contract-affecting catalog writes; reads/writes the config row in the pinned jsonb-SCALAR encoding.';

-- placements: ins/del ALWAYS bump (WHEN cannot reference OLD on INSERT
-- triggers — see header); upd bumps only when a contract-affecting column
-- (state, key bounds, slot, schema_version) ACTUALLY changed — stats columns
-- (row_estimate, bytes_used, last_health_at) are excluded from UPDATE OF and
-- WHEN distinct suppresses no-op writes, so watchdog refreshes never
-- invalidate client snapshots (r37 P3).
drop trigger if exists wh_dir_bump on public.warehouse_placements;
drop trigger if exists wh_dir_bump_ins_del on public.warehouse_placements;
drop trigger if exists wh_dir_bump_upd on public.warehouse_placements;
create trigger wh_dir_bump_ins_del
  after insert or delete
  on public.warehouse_placements
  for each row
  execute function public.wh_bump_directory_version();
create trigger wh_dir_bump_upd
  after update of state, key_min, key_max, hash_slot, schema_version
  on public.warehouse_placements
  for each row
  when (old.* is distinct from new.*)
  execute function public.wh_bump_directory_version();

-- cold objects: contract-affecting state transitions (exported/promoted/
-- dropped) bump too — second trigger, same function (§1.5).
drop trigger if exists wh_cold_bump on public.warehouse_cold_objects;
drop trigger if exists wh_cold_bump_ins_del on public.warehouse_cold_objects;
drop trigger if exists wh_cold_bump_upd on public.warehouse_cold_objects;
create trigger wh_cold_bump_ins_del
  after insert or delete
  on public.warehouse_cold_objects
  for each row
  execute function public.wh_bump_directory_version();
create trigger wh_cold_bump_upd
  after update of state
  on public.warehouse_cold_objects
  for each row
  when (old.* is distinct from new.*)
  execute function public.wh_bump_directory_version();

-- ---------- RLS (0001 doctrine: enable + no policies) -----------------------
alter table public.warehouse_tables      enable row level security;
alter table public.warehouse_placements  enable row level security;
alter table public.warehouse_cold_objects enable row level security;
alter table public.load_jobs             enable row level security;
alter table public.load_partitions       enable row level security;
-- No policies = no access via anon/authenticated keys. Service role bypasses.
