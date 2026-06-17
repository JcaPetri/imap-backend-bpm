-- =============================================================================
-- BPM V006 — RLS ENFORCE uniforme (sprint 3c.3)
-- =============================================================================
-- Activa RLS real en TODAS las tablas bpm con tenant_id, con policy uniforme
-- (espejo de inventory V027 + cláusula bypass que bpm necesita):
--   USING/WITH CHECK = (tenant_id IS NULL
--                       OR tenant_id = <GUC app.current_tenant_id>
--                       OR app.bypass_rls = 'true')
--
-- * tenant_id IS NULL → filas de plataforma (si las hubiera) visibles.
-- * tenant_id = GUC   → aislamiento por tenant operativo (lo setea EavTenantSession
--                       en cada tx: request via TenantContextFilter, async via worker).
-- * bypass_rls=true   → escape para JobScanService.claimDueJobs (scan cross-tenant
--                       de jobs vencidos). inventory NO lo necesita (no tiene scanner);
--                       bpm SÍ. Solo lo setea código de sistema controlado.
-- * nullif(...,'')::uuid → fail-closed sin error de cast si el GUC viene vacío.
--
-- INERTE bajo admindb (superuser bypassa RLS). Se ACTIVA al pasar runtime a bpm_app
-- (rolsuper=f) vía el switch de DB_USERNAME en prod (3c.3b). Flyway corre como rol
-- DDL (admindb via DB_MIGRATION_USER) → este ALTER/CREATE POLICY tiene privilegios.
--
-- Reactiva las tablas que V005 (WorkHub) y Paso14 (msg-start) habían dejado DISABLE.
-- =============================================================================

DO $$
DECLARE t text;
BEGIN
    FOR t IN SELECT tablename FROM pg_tables WHERE schemaname = 'bpm' LOOP
        IF EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_schema = 'bpm' AND table_name = t AND column_name = 'tenant_id') THEN
            EXECUTE format('ALTER TABLE bpm.%I ENABLE ROW LEVEL SECURITY', t);
            EXECUTE format('DROP POLICY IF EXISTS rls_tenant_isolation ON bpm.%I', t);
            EXECUTE format(
                'CREATE POLICY rls_tenant_isolation ON bpm.%I '
                || 'USING (tenant_id IS NULL '
                || '       OR tenant_id = nullif(current_setting(''app.current_tenant_id'', true), '''')::uuid '
                || '       OR current_setting(''app.bypass_rls'', true) = ''true'') '
                || 'WITH CHECK (tenant_id IS NULL '
                || '       OR tenant_id = nullif(current_setting(''app.current_tenant_id'', true), '''')::uuid '
                || '       OR current_setting(''app.bypass_rls'', true) = ''true'')', t);
        END IF;
    END LOOP;
END $$;

-- Verificación
DO $$
DECLARE v_n INT;
BEGIN
    SELECT COUNT(*) INTO v_n
      FROM pg_policy p JOIN pg_class c ON c.oid = p.polrelid
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = 'bpm' AND p.polname = 'rls_tenant_isolation';
    RAISE NOTICE '--- V006 RLS enforce — % tablas bpm con policy rls_tenant_isolation ---', v_n;
END $$;
