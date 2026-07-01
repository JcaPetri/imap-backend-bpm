-- =============================================================================
-- BPM V016 — Defs de plataforma (SYSTEM_TENANT) visibles cross-tenant
-- =============================================================================
-- Las 9 tablas relacionales de def (V015) son METADATA DE PLATAFORMA: los
-- processdefs/versions/flowelements/DMN SYSTEM_TENANT (00000000-…-01) los usan
-- TODOS los tenants (ej. proceso 'login' compartido). Verificado en prod: 70
-- processinstances del tenant 6060c8dd ejecutan defs SYSTEM_TENANT.
--
-- La policy rls_tenant_isolation de V015 (heredada de V006) solo permite
-- own-tenant (+ NULL + bypass) → bajo GUC=<tenant> los defs SYSTEM quedan
-- OCULTOS. El path HTTP a system funciona porque la policy de system SÍ incluye
-- SYSTEM_TENANT (`tenant_id = '00000000-…-01' OR = GUC OR bypass`). Para que el
-- read LOCAL (F5) sea correcto para todos los tenants, replicamos ese patrón.
--
-- SOLO las 9 tablas de DEF (templates read-mostly de plataforma). Las tablas
-- RUNTIME (processinstance/token/…) NO se tocan: siguen aisladas por tenant.
-- Idempotente. admindb.
-- =============================================================================
SET search_path TO bpm, public;

DO $$
DECLARE t text;
DECLARE tbls text[] := ARRAY[
    'bpm_pro_processdef_tbl','bpm_pro_processversion_tbl','bpm_pro_flowelement_tbl',
    'bpm_pro_sequenceflow_tbl','bpm_hum_taskform_tbl','bpm_dmn_decisiondef_tbl',
    'bpm_dmn_rule_tbl','bpm_pro_migrationplan_tbl','bpm_pro_migrationrule_tbl'];
BEGIN
    FOREACH t IN ARRAY tbls LOOP
        EXECUTE format('DROP POLICY IF EXISTS rls_tenant_isolation ON bpm.%I', t);
        EXECUTE format(
            'CREATE POLICY rls_tenant_isolation ON bpm.%I '
            || 'USING (tenant_id = ''00000000-0000-0000-0000-000000000001''::uuid '
            || '       OR tenant_id = nullif(current_setting(''app.current_tenant_id'', true), '''')::uuid '
            || '       OR current_setting(''app.bypass_rls'', true) = ''true'') '
            || 'WITH CHECK (tenant_id = ''00000000-0000-0000-0000-000000000001''::uuid '
            || '       OR tenant_id = nullif(current_setting(''app.current_tenant_id'', true), '''')::uuid '
            || '       OR current_setting(''app.bypass_rls'', true) = ''true'')', t);
    END LOOP;
    RAISE NOTICE '--- V016: policy SYSTEM_TENANT-visible aplicada a 9 tablas de def ---';
END $$;
