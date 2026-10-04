-- =============================================================================
-- 0014_loader_rpc.sql — fm_loader_bookkeep + fm_loader_finalize (r41)
-- =============================================================================
-- Contract: research/findings_wh_catalog_contract.md §1.4 (ledger + ONE
-- idempotent RPC law) + §2.2 (load-partition state machine + finalize handoff).
-- Ledger tables are 0013's load_jobs/load_partitions — THIS migration adds
-- only the server-side state machine. Laws implemented here, verbatim:
--   * ALL loader ledger writes go through ONE idempotent SQL-RPC endpoint
--     (Multi-Row Enrollment law: N separate writes leave cascade-lethal
--     partial state; server-side validated where the trust lives).
--   * The loader is STATELESS: load_partitions is sole truth; resume =
--     state='pending' OR (state='loading' AND lease_until < now()).
--   * Lease = 10 minutes (must exceed restore ~4min + deploy ~60s so a
--     lease cannot expire mid-restore — §0 availability law).
--   * 'failed' is NOT a state: failed rows stay pending/loaded with
--     last_error; terminal failure = attempts exhausted (loader's read).
--   * The loader NEVER writes the catalog directly — placement flips happen
--     ONLY at fm_loader_finalize (FM trust boundary); the 0013 bump trigger
--     fires there (state is in its UPDATE OF list), and the one_serving_per_
--     span partial unique index surfaces 23505 as a clean error.
--   * Migrate-first gate: load_jobs.schema_version must equal the table's
--     schema_version at lease AND finalize (§2.5; the loader never bumps).
--   * Overlap gate (P1-6, range/time tables only): before flipping to
--     serving, NO existing serving/draining span on the same table may
--     overlap [key_min, key_max) — exact-equality uniqueness does NOT catch
--     [a,m)+[f,z) coexisting (wrong-SUM class). Hash tables skip this: slot
--     equality is already covered by one_serving_per_span.
-- =============================================================================
-- NOTE ON APPLICATION ORDER: 0014 references only 0013 objects (load_jobs,
-- load_partitions, warehouse_placements, warehouse_tables, config). The
-- config seed + bump triggers live in 0013 and are NOT repeated here.

-- ---------- fm_loader_bookkeep (the ONE ledger-write endpoint) --------------
create or replace function public.fm_loader_bookkeep(
    p_action        text,
    p_job_id        uuid,
    p_placement_id  uuid default null,
    p_partition_id  text default null,
    p_rows          bigint default null,
    p_checksum      text default null,
    p_shard_checksum text default null,
    p_error         text default null,
    p_table_id      uuid default null,     -- ensure_job only
    p_schema_version integer default null,  -- ensure_job only
    p_shard_key_column text default null,   -- ensure_job only
    p_shard_expr    text default null,      -- ensure_job only
    p_num_shards    integer default null,   -- ensure_job only
    p_source_uri    text default null,      -- ensure_job only
    p_row_count_est bigint default null,    -- ensure_job only
    p_placement_ids uuid[] default null,    -- ensure_partitions only
    p_partition_ids text[] default null     -- ensure_partitions only
) returns jsonb
language plpgsql
as $fn$
declare
    v_row       public.load_partitions%rowtype;
    v_job       public.load_jobs%rowtype;
    v_table     public.warehouse_tables%rowtype;
    v_updated   integer;
    v_overlap   integer;
begin
    case p_action
    -- ================================================== ensure_job
    when 'ensure_job' then
        if p_job_id is null or p_table_id is null or p_schema_version is null
           or p_shard_key_column is null or p_shard_expr is null
           or p_num_shards is null or p_source_uri is null then
            raise exception 'ensure_job: missing required fields';
        end if;
        select * into v_table from public.warehouse_tables where id = p_table_id;
        if not found then
            raise exception 'ensure_job: table % is not registered', p_table_id;
        end if;
        -- migrate-first gate, early form (§2.5): refuse to open a job whose
        -- declared schema_version is already behind the table's.
        if p_schema_version <> v_table.schema_version then
            raise exception 'ensure_job: schema_version % <> table % (migrate-first gate)',
                p_schema_version, v_table.schema_version;
        end if;
        insert into public.load_jobs (job_id, table_id, schema_version, shard_key_column,
                                      shard_expr, num_shards, source_uri, row_count_est)
        values (p_job_id, p_table_id, p_schema_version, p_shard_key_column,
                p_shard_expr, p_num_shards, p_source_uri, p_row_count_est)
        on conflict (job_id) do update set job_id = excluded.job_id;  -- idempotent no-op touch
        select * into v_job from public.load_jobs where job_id = p_job_id;
        return jsonb_build_object('ok', true, 'action', 'ensure_job',
                                  'job_id', v_job.job_id,
                                  'table_id', v_job.table_id,
                                  'schema_version', v_job.schema_version);

    -- ================================================== ensure_partitions
    when 'ensure_partitions' then
        if p_job_id is null or p_placement_ids is null or p_partition_ids is null
           or array_length(p_placement_ids, 1) <> array_length(p_partition_ids, 1) then
            raise exception 'ensure_partitions: job_id and equal-length placement/partition arrays required';
        end if;
        -- Natural-key ON CONFLICT DO NOTHING; the placement FK + the join
        -- filter weld each row to a placement that ACTUALLY belongs to the
        -- job's table (a misrouted partition must fail loud, not register).
        insert into public.load_partitions (job_id, placement_id, partition_id)
        select p_job_id, pid, part
        from unnest(p_placement_ids, p_partition_ids) as u(pid, part)
        join public.warehouse_placements wp on wp.id = pid and wp.table_id = (
            select table_id from public.load_jobs where job_id = p_job_id)
        on conflict (job_id, placement_id, partition_id) do nothing;
        get diagnostics v_updated = row_count;
        return jsonb_build_object('ok', true, 'action', 'ensure_partitions',
                                  'inserted', v_updated);

    -- ================================================== lease
    when 'lease' then
        -- migrate-first gate (§2.5) before any work is done.
        select * into v_job from public.load_jobs where job_id = p_job_id;
        if not found then raise exception 'lease: job % not found', p_job_id; end if;
        select * into v_table from public.warehouse_tables where id = v_job.table_id;
        if v_job.schema_version <> v_table.schema_version then
            raise exception 'lease: job schema_version % <> table % (migrate-first gate)',
                v_job.schema_version, v_table.schema_version;
        end if;
        -- §2.2 transition: pending -> loading, or stale-loading reclaimed.
        -- A LIVE lease (loading AND lease_until >= now()) must never be
        -- stolen: 0 rows updated = "not mine this round" for the stateless
        -- loader (resume predicate owns this decision client-side too).
        update public.load_partitions
        set state = 'loading',
            lease_until = now() + interval '10 minutes',
            attempts = attempts + 1,
            updated_at = now()
        where job_id = p_job_id and placement_id = p_placement_id
          and partition_id = p_partition_id
          and (state = 'pending' or (state = 'loading' and lease_until < now()))
        returning * into v_row;
        if not found then
            -- Distinguish the two benign cases from a genuine conflict:
            select * into v_row from public.load_partitions
            where job_id = p_job_id and placement_id = p_placement_id
              and partition_id = p_partition_id;
            if not found then
                raise exception 'lease: partition row %/%/% does not exist (ensure_partitions first)',
                    p_job_id, p_placement_id, p_partition_id;
            end if;
            if v_row.state = 'loading' and v_row.lease_until >= now() then
                return jsonb_build_object('ok', false, 'reason', 'lease_held',
                                          'state', v_row.state, 'lease_until', v_row.lease_until);
            end if;
            return jsonb_build_object('ok', false, 'reason', 'not_leaseable',
                                      'state', v_row.state, 'attempts', v_row.attempts,
                                      'last_error', v_row.last_error);
        end if;
        return jsonb_build_object('ok', true, 'action', 'lease',
                                  'state', v_row.state, 'attempts', v_row.attempts,
                                  'lease_until', v_row.lease_until);

    -- ================================================== loaded
    when 'loaded' then
        -- loading -> loaded: carries rows_sent + the loader-side checksum;
        -- clears the lease (the work is done; a live lease here means the
        -- writer died between INSERT and bookkeep — resume re-leases).
        update public.load_partitions
        set state = 'loaded', rows_sent = p_rows, checksum = p_checksum,
            lease_until = null, updated_at = now()
        where job_id = p_job_id and placement_id = p_placement_id
          and partition_id = p_partition_id and state = 'loading'
        returning * into v_row;
        if not found then
            select * into v_row from public.load_partitions
            where job_id = p_job_id and placement_id = p_placement_id
              and partition_id = p_partition_id;
            if not found then raise exception 'loaded: partition row not found'; end if;
            return jsonb_build_object('ok', false, 'reason', 'not_loading',
                                      'state', v_row.state);
        end if;
        return jsonb_build_object('ok', true, 'action', 'loaded',
                                  'state', v_row.state, 'rows_sent', v_row.rows_sent);

    -- ================================================== verify
    when 'verify' then
        -- loaded -> verified: SERVER-side checksum comparison (the §3.4
        -- honest-verifier law — the shard's checksum must equal the
        -- loader-side one; a mismatch is a loud exception, never a merge).
        select * into v_row from public.load_partitions
        where job_id = p_job_id and placement_id = p_placement_id
          and partition_id = p_partition_id;
        if not found then raise exception 'verify: partition row not found'; end if;
        if v_row.state = 'verified' and v_row.shard_checksum = p_shard_checksum
           and v_row.shard_checksum = v_row.checksum then
            return jsonb_build_object('ok', true, 'action', 'verify',
                                      'state', v_row.state, 'idempotent', true);
        end if;
        if v_row.state <> 'loaded' then
            return jsonb_build_object('ok', false, 'reason', 'not_loaded',
                                      'state', v_row.state);
        end if;
        if p_shard_checksum is null or v_row.checksum is null
           or p_shard_checksum <> v_row.checksum then
            raise exception 'verify: checksum mismatch (shard % vs loader %) — retry once, then fail loudly',
                coalesce(p_shard_checksum, 'null'), coalesce(v_row.checksum, 'null');
        end if;
        update public.load_partitions
        set state = 'verified', shard_checksum = p_shard_checksum, updated_at = now()
        where job_id = p_job_id and placement_id = p_placement_id
          and partition_id = p_partition_id and state = 'loaded';
        return jsonb_build_object('ok', true, 'action', 'verify', 'state', 'verified');

    -- ================================================== fail
    when 'fail' then
        -- last_error recording; state UNCHANGED ('failed' is not a state —
        -- terminal failure is the loader's read of attempts + last_error).
        update public.load_partitions
        set last_error = coalesce(p_error, 'unspecified loader failure'), updated_at = now()
        where job_id = p_job_id and placement_id = p_placement_id
          and partition_id = p_partition_id;
        if not found then raise exception 'fail: partition row not found'; end if;
        return jsonb_build_object('ok', true, 'action', 'fail');

    -- ================================================== finalize
    when 'finalize' then
        -- §2.2 handoff: ALL partitions verified -> flip each job placement
        -- to serving (bump fires via the 0013 UPDATE trigger) + overlap gate
        -- (range/time only) + migrate-first gate (authoritative form) +
        -- finished_at stamp. Idempotent: re-finalize on a finished job with
        -- everything verified is a no-op returning the same shape.
        select * into v_job from public.load_jobs where job_id = p_job_id;
        if not found then raise exception 'finalize: job % not found', p_job_id; end if;
        select * into v_table from public.warehouse_tables where id = v_job.table_id;
        if v_job.schema_version <> v_table.schema_version then
            raise exception 'finalize: job schema_version % <> table % (migrate-first gate)',
                v_job.schema_version, v_table.schema_version;
        end if;
        if exists (
            select 1 from public.load_partitions
            where job_id = p_job_id and state <> 'verified'
        ) then
            raise exception 'finalize: job % has unverified partitions (all must be verified)',
                p_job_id;
        end if;
        -- Overlap gate (P1-6): range/time tables only. Compare each
        -- about-to-serve placement's span against OTHER serving/draining
        -- placements of the same table (excluding the ones this job flips —
        -- they are not serving YET).
        if v_table.shard_key_type in ('range', 'time') then
            -- r41 audit F2: NULL bounds = UNBOUNDED (0013:134) — an
            -- unbounded span overlaps EVERYTHING, so NULL must act as
            -- ±infinity here, NOT be excluded by IS NOT NULL guards (the
            -- guards inverted 0013 semantics and let a bounded finalize
            -- co-exist with a serving unbounded span — the exact wrong-SUM
            -- class P1-6 exists to prevent).
            select count(*) into v_overlap
            from public.load_partitions lp
            join public.warehouse_placements a on a.id = lp.placement_id
            join public.warehouse_placements b
              on b.table_id = a.table_id
             and b.id <> a.id
             and b.state in ('serving', 'draining')
             and (a.key_min is null or b.key_max is null or a.key_min < b.key_max)
             and (b.key_min is null or a.key_max is null or b.key_min < a.key_max)
            where lp.job_id = p_job_id;
            -- r41 audit F3: the gate must also check the job's placements
            -- against EACH OTHER — they are all 'loading' at gate time, so
            -- the cross-writer backstop (this gate IS one) has to catch a
            -- buggy span-carver shipping mutually overlapping spans. All of
            -- a job's placements share the job's table (ensure_partitions
            -- join-enforced), so the pairs are compared directly.
            if v_overlap = 0 then
                select count(*) into v_overlap
                from public.load_partitions lpa
                join public.load_partitions lpb on lpb.job_id = lpa.job_id
                join public.warehouse_placements a on a.id = lpa.placement_id
                join public.warehouse_placements b on b.id = lpb.placement_id
                where lpa.job_id = p_job_id
                  and a.id < b.id  -- each unordered pair exactly once
                  and (a.key_min is null or b.key_max is null or a.key_min < b.key_max)
                  and (b.key_min is null or a.key_max is null or b.key_min < a.key_max);
            end if;
            if v_overlap > 0 then
                raise exception 'finalize: % overlapping serving/draining span(s) on table % (P1-6 overlap gate)',
                    v_overlap, v_table.logical_name;
            end if;
        end if;
        -- The flip (state IS in the 0013 bump trigger's UPDATE OF list —
        -- every flip bumps the directory version). Placements are born
        -- state='loading' (0013 default) and finalize is the ONLY writer
        -- that makes them 'serving'. r41 audit F1: the flip predicate must
        -- be state='loading' EXACTLY — '<> serving' would resurrect a
        -- RETIRED placement on an idempotent finalize replay (retired is
        -- terminal, §2.1; the FK-cascade never deletes the verified ledger
        -- rows, so the all-verified gate passes and an emptied shard would
        -- re-enter the directory as wrong-SUM-of-zeros).
        update public.warehouse_placements wp
        set state = 'serving'
        from public.load_partitions lp
        where lp.job_id = p_job_id and lp.placement_id = wp.id
          and wp.state = 'loading';
        get diagnostics v_updated = row_count;
        -- finished_at once (idempotent: coalesce-style guard).
        update public.load_jobs set finished_at = now()
        where job_id = p_job_id and finished_at is null;
        return jsonb_build_object('ok', true, 'action', 'finalize',
                                  'placements_flipped', v_updated,
                                  'table', v_table.logical_name);

    else
        raise exception 'unknown action %', p_action;
    end case;
end
$fn$;

-- ---------- lockdown (service-plane only; loader carries service-role) ------
-- r41 audit F4: revoke from public alone is NOT sufficient on a real Supabase
-- project — default privileges grant EXECUTE to anon/authenticated explicitly
-- (kit doctrine: 0012/0007 pattern). Strip both; service_role keeps its grant.
revoke all on function public.fm_loader_bookkeep(text, uuid, uuid, text, bigint,
    text, text, text, uuid, integer, text, text, integer, text, bigint, uuid[], text[])
    from public, anon, authenticated;

comment on function public.fm_loader_bookkeep is
'§1.4/§2.2: the ONE idempotent loader ledger-write endpoint (actions: ensure_job, ensure_partitions, lease, loaded, verify, fail, finalize). The loader is stateless — load_partitions is sole truth; resume = pending OR (loading AND lease expired). Loader NEVER writes the catalog directly: finalize is the only placement writer (bump fires via the 0013 trigger).';
