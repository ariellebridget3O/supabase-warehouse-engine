-- =============================================================================
-- Fleet Manager — 0017_grants_fuse.sql
-- =============================================================================
-- Explicit Data-API grants for the engine's service-role REST plane — the
-- 10-30 GRANTS-FUSE rider (r177 R39). Task ID: maxxing-r177-r39-impl.
-- ADDITIVE-ONLY: 0013/0014/0016 bytes are untouched (byte-pinned by kit
-- audits); this file re-establishes the same ACL posture on fresh deploys.
--
-- WHAT AND WHY: Supabase changelog 45329 — on 2026-10-30 the Data-API
-- default-grants behavior is enforced on ALL projects: "tables without
-- explicit grants stop being reachable via the Data API" (PLAN.md:78 rider,
-- r169 sweep). 0013/0014/0016 ship NO explicit grants on the engine tree —
-- they relied on the pre-fuse Data-API default privileges for service_role
-- (the 0014:309-312 doctrine: "service_role keeps its grant" via default
-- privileges); the only explicit service_role SELECT in the kit is
-- shard-side (0015:509 wh_query_templates). Existing hosts are UNAFFECTED
-- (pre-fuse tables keep their current grants and stay reachable); FRESH kit
-- deploys after 2026-10-30 would 401/403 the engine's boot reads. This file
-- bundles the explicit grants so a fresh deploy converges to the same
-- service_role ACL posture the default privileges used to provide.
--
-- ROLE SCOPE — service_role ONLY. Registered consumers: the engine's db()
-- service-role supabase-js client (whe_store.ts) + service-key probe legs.
-- Surface census (CREATE site / engine REST read site):
--   * config                  — index.ts:141 (directory-version probe),
--                               index.ts:202 (wh_shard_proxy_map KV),
--                               index.ts:284 (geo_mode), geo_write_fence.ts:93
--                               (fence keys). fm base-schema surface.
--   * projects                — wh_entrypoint.ts:612 (freshness keeper
--                               id-resolve); base table of the directory
--                               view (0013:191). fm base-schema surface.
--   * v_geo_directory         — index.ts:160 (chain #3, limit(2)). fm
--                               0017_geo_control_plane view — same
--                               fleet-manager-family provisioning as the
--                               base schema (DEPLOY §2).
--   * v_warehouse_directory   — 0013:183 (the directory plane the engine's
--                               atomic directory read rides). security_invoker
--                               view ⇒ invoker-side base-table ACLs are also
--                               checked; covered by the grants below on
--                               warehouse_tables / warehouse_placements /
--                               projects.
--   * warehouse_tables        — index.ts:294 (is_reference gate); 0013:94.
--   * warehouse_placements    — 0013:128 (directory view base; §2/§3b flows).
--   * warehouse_cold_objects  — 0013:206 (catalog-union leg).
--   * load_jobs               — 0013:249 (catalog-union leg).
--   * load_partitions         — 0013:267 (catalog-union leg).
-- The ONE update grant: wh_entrypoint.ts:621-624 — the r138 F-1 freshness
-- keeper's stats-only last_health_at stamp (the kit's only REST write;
-- failure is log-only, but the fuse would silence it on every fresh deploy).
-- NO anon/authenticated grants — no registered consumers (OQ-4: granting is
-- the riskier direction). The 0001 RLS doctrine (enable + no policies) and
-- the 0013/0016 view revokes stay intact (service_role bypasses RLS).
--
-- ASSUMPTIONS (parent-adjudicated; recorded here AND in DEPLOY §2b):
--   * The enforcement is assumed to strip service_role DEFAULT grants on
--     fresh deploys — the safe direction: pre-fuse, every grant here is a
--     harmless no-op re-statement of the default privilege. Unverified
--     against a live post-2026-10-30 project.
--   * Whether ALTER TABLE (or a new view over a pre-existing table)
--     re-triggers exposure evaluation post-fuse is UNKNOWN (record-only).
--     Existing hosts keep their grants either way, so re-running this file
--     on one stays a no-op.
--
-- DELIBERATE EXCLUSIONS: shard-tree surfaces (facts_blocks/facts_events are
-- deliberately NOT Data-API-exposed — revoke doctrine; the wh_query RPC path
-- is fuse-immune via the explicit 0015:453 EXECUTE and 0016:409 grants);
-- wh_query_templates (already granted at 0015:509, shard tree — may not
-- exist on a multi-project engine host, so never granted here);
-- fm-owned wh_directory_atomic_read (RPC EXECUTE is a function privilege,
-- not a Data-API table grant — outside the fuse's table-grant class);
-- v_warehouse_rolloff_candidates (no registered REST consumer; anon/
-- authenticated stay revoked per 0016); §3b's wh_probe_agg / wh_probe_dim
-- (operator-created — NOT guaranteed to exist when this file runs; the §3b
-- recipe bundles its own grant lines post-fuse).
--
-- RE-RUN SAFETY: GRANT is an idempotent ACL upsert; has_table_privilege
-- probes are read-only SELECTs. Pure ACL/SELECT — never mutates rows.
-- Runs LAST (lexicographic 0017): every granted surface exists by then
-- (0013 creates the catalog tables + directory view; config/projects are
-- the fm base-schema prerequisite — scripts/migrate.sh header, DEPLOY §2;
-- v_geo_directory arrives with the same provisioning).
--
-- SPLITTABILITY CONTRACT: ONE statement per line — scripts/migrate.sh POSTs
-- one statement per Management-API call (scripts/sql_split.awk; trailing
-- comments are stripped by the splitter, never POSTed). No dollar-quote
-- bodies. 20 statements (10 grants + 10 closing verify probes).
-- =============================================================================

grant select on public.config to service_role;                    -- index.ts:141/:202/:284 + geo_write_fence.ts:93
grant select on public.projects to service_role;                  -- wh_entrypoint.ts:612 + 0013:191 view base
grant select on public.v_geo_directory to service_role;           -- index.ts:160 (chain #3)
grant select on public.v_warehouse_directory to service_role;     -- 0013:183 (the directory plane)
grant select on public.warehouse_tables to service_role;          -- index.ts:294 (chain #5) + 0013:94
grant select on public.warehouse_placements to service_role;      -- 0013:128 (directory view base)
grant update on public.warehouse_placements to service_role;      -- wh_entrypoint.ts:621-624 (freshness keeper stamp)
grant select on public.warehouse_cold_objects to service_role;    -- 0013:206 (catalog-union leg)
grant select on public.load_jobs to service_role;                 -- 0013:249 (catalog-union leg)
grant select on public.load_partitions to service_role;           -- 0013:267 (catalog-union leg)

-- ---------- closing verify probes (one per grant; true = healthy) -----------
select has_table_privilege('service_role','public.config','select');
select has_table_privilege('service_role','public.projects','select');
select has_table_privilege('service_role','public.v_geo_directory','select');
select has_table_privilege('service_role','public.v_warehouse_directory','select');
select has_table_privilege('service_role','public.warehouse_tables','select');
select has_table_privilege('service_role','public.warehouse_placements','select');
select has_table_privilege('service_role','public.warehouse_placements','update');
select has_table_privilege('service_role','public.warehouse_cold_objects','select');
select has_table_privilege('service_role','public.load_jobs','select');
select has_table_privilege('service_role','public.load_partitions','select');
