-- V023 — Overlay de procesos por tenant (gemelo de acc_tenant_account del plan de cuentas).
--
-- El catalogo de processdefs es de PLATAFORMA (bajo SYSTEM_TENANT); cada tenant ELIGE cuales
-- usa y los PARAMETRIZA sin tocar el grafo (modelo overlay de 3 niveles, ver IMAP_BPM_PROCESS_CATALOG.md §1):
--   Nivel 1 - habilitar: enabled = true/false (el tenant adopta el proceso del catalogo).
--   Nivel 2 - parametrizar: config jsonb (umbrales, rol->cola, SLAs, flags, etc.) que el motor
--             inyecta como la variable 'config' al arrancar la instancia; gateways/DMN la leen.
--   Nivel 3 - forkear: NO usa esta tabla; el tenant crea su propio processdef (el loader
--             resuelve tenant->SYSTEM). Ultimo recurso (no hereda mejoras de plataforma).
--
-- Se referencia por processdef_code (clave de negocio estable), no por FK a un processdef_id
-- (el code puede resolver a un def de plataforma o a uno forkeado del tenant).
-- audit-7 + RLS + triggers como el resto de las tablas bpm (patron V015/V019).

CREATE TABLE IF NOT EXISTS bpm.bpm_pro_tenant_process_tbl (
    id                   UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    tenant_id            UUID NOT NULL,
    processdef_code      TEXT NOT NULL,
    enabled              BOOLEAN NOT NULL DEFAULT true,
    config               JSONB,
    -- nucleo audit-7:
    state_id             UUID NOT NULL,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by_id        UUID,
    updated_by_id        UUID,
    owned_by_id          UUID,
    CONSTRAINT uq_tenant_process UNIQUE (tenant_id, processdef_code)
);

CREATE INDEX IF NOT EXISTS idx_tenant_process_enabled
    ON bpm.bpm_pro_tenant_process_tbl(tenant_id, enabled);

-- RLS + nucleo fill + audit triggers (idem V019).
DO $tenproc$
BEGIN
    ALTER TABLE bpm.bpm_pro_tenant_process_tbl ENABLE ROW LEVEL SECURITY;
    DROP POLICY IF EXISTS rls_tenant_isolation ON bpm.bpm_pro_tenant_process_tbl;
    CREATE POLICY rls_tenant_isolation ON bpm.bpm_pro_tenant_process_tbl
        USING (tenant_id IS NULL
               OR tenant_id = nullif(current_setting('app.current_tenant_id', true), '')::uuid
               OR current_setting('app.bypass_rls', true) = 'true')
        WITH CHECK (tenant_id IS NULL
               OR tenant_id = nullif(current_setting('app.current_tenant_id', true), '')::uuid
               OR current_setting('app.bypass_rls', true) = 'true');
    CREATE OR REPLACE TRIGGER trg_fill_nucleo BEFORE INSERT OR UPDATE ON bpm.bpm_pro_tenant_process_tbl
        FOR EACH ROW EXECUTE FUNCTION bpm.fn_fill_nucleo();
    CREATE OR REPLACE TRIGGER trg_audit_bpm_pro_tenant_process_tbl AFTER INSERT OR UPDATE OR DELETE ON bpm.bpm_pro_tenant_process_tbl
        FOR EACH ROW EXECUTE FUNCTION bpm.fn_audit_trigger();
END $tenproc$;
