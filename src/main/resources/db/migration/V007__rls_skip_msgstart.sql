-- =============================================================================
-- BPM V007 — excluir bpm_pro_message_start_subscription_tbl de RLS (fix regresión V006)
-- =============================================================================
-- Paso14 había DESACTIVADO RLS en esta tabla a propósito: su sync
-- (ProcessDefinitionLoader.syncMessageStartSubscriptions) hace un UPSERT que
-- consulta/actualiza subscriptions de forma cross-tenant (al cargar un processversion),
-- y su aislación se garantiza por WHERE explícito (tenant_id) en los repos
-- (findActiveByTenantAndMessageCode, findForUpsert, deactivateOldVersions).
--
-- V006 (RLS enforce uniforme) la re-activó por error → el SELECT del UPSERT queda
-- RLS-filtrado, no ve la fila existente bajo otro tenant (ej. legacy SYSTEM) e
-- intenta INSERT → "duplicate key violates bpm_pro_msg_start_sub_unique" (no fatal,
-- load() lo captura, pero es ruido + regresión).
--
-- Fix: volver a DISABLE RLS SOLO en esta tabla (resto de bpm mantiene la policy de V006).
-- Corre como rol DDL (admindb via DB_MIGRATION_USER); DISABLE requiere owner/superuser.
-- =============================================================================

ALTER TABLE bpm.bpm_pro_message_start_subscription_tbl DISABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS rls_tenant_isolation ON bpm.bpm_pro_message_start_subscription_tbl;

DO $$
DECLARE v_rls boolean;
BEGIN
    SELECT relrowsecurity INTO v_rls FROM pg_class
     WHERE oid = 'bpm.bpm_pro_message_start_subscription_tbl'::regclass;
    RAISE NOTICE '--- V007 — RLS en msg_start_subscription enabled=% (esperado false) ---', v_rls;
END $$;
