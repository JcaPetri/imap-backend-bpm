-- V020 — Suscripciones de event sub-process (Ola 6.1).
--
-- Un event_sub_process es un handler DORMANTE adjunto a una instancia, disparado por
-- un evento (MVP: signal) a nivel instancia. Al arrancar la instancia se registra una
-- suscripcion activa por cada event_sub_process del def; cuando llega el signal con el
-- trigger_code que matchea, se dispara el handler (calledProcessversionId):
--   • interrupting=true  → cancela los tokens vivos de la instancia y corre el handler;
--                          al completar el handler, la instancia completa.
--   • interrupting=false → corre el handler en paralelo (fire-and-forget); el flujo
--                          principal sigue. Puede dispararse varias veces (queda active).
--
-- Runtime state (como tokens/jobs/incidents): el motor la escribe directo (exencion §F.6).
-- audit-7 + RLS + triggers como el resto de las tablas bpm (patron V015/V019).

CREATE TABLE IF NOT EXISTS bpm.bpm_pro_event_subscription_tbl (
    id                   UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    tenant_id            UUID NOT NULL,
    processinstance_id   UUID NOT NULL,
    element_id           UUID,            -- flow_element (event_sub_process)
    element_code         TEXT,
    trigger_type         VARCHAR(20) NOT NULL,   -- signal (MVP) | message | error (futuro)
    trigger_code         VARCHAR(100) NOT NULL,
    handler_version_id   UUID NOT NULL,   -- processversion del cuerpo del handler
    interrupting         BOOLEAN NOT NULL DEFAULT true,
    lifecycle            VARCHAR(20) NOT NULL DEFAULT 'active'
        CHECK (lifecycle IN ('active', 'consumed')),
    -- nucleo audit-7:
    state_id             UUID NOT NULL,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by_id        UUID,
    updated_by_id        UUID,
    owned_by_id          UUID,
    CONSTRAINT fk_evtsub_instance FOREIGN KEY (processinstance_id)
        REFERENCES bpm.bpm_pro_processinstance_tbl(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_evtsub_dispatch
    ON bpm.bpm_pro_event_subscription_tbl(tenant_id, trigger_type, trigger_code, lifecycle);
CREATE INDEX IF NOT EXISTS idx_evtsub_instance
    ON bpm.bpm_pro_event_subscription_tbl(processinstance_id);

-- RLS + nucleo fill + audit triggers (idem V019).
DO $evtsub$
BEGIN
    ALTER TABLE bpm.bpm_pro_event_subscription_tbl ENABLE ROW LEVEL SECURITY;
    DROP POLICY IF EXISTS rls_tenant_isolation ON bpm.bpm_pro_event_subscription_tbl;
    CREATE POLICY rls_tenant_isolation ON bpm.bpm_pro_event_subscription_tbl
        USING (tenant_id IS NULL
               OR tenant_id = nullif(current_setting('app.current_tenant_id', true), '')::uuid
               OR current_setting('app.bypass_rls', true) = 'true')
        WITH CHECK (tenant_id IS NULL
               OR tenant_id = nullif(current_setting('app.current_tenant_id', true), '')::uuid
               OR current_setting('app.bypass_rls', true) = 'true');
    CREATE OR REPLACE TRIGGER trg_fill_nucleo BEFORE INSERT OR UPDATE ON bpm.bpm_pro_event_subscription_tbl
        FOR EACH ROW EXECUTE FUNCTION bpm.fn_fill_nucleo();
    CREATE OR REPLACE TRIGGER trg_audit_bpm_pro_event_subscription_tbl AFTER INSERT OR UPDATE OR DELETE ON bpm.bpm_pro_event_subscription_tbl
        FOR EACH ROW EXECUTE FUNCTION bpm.fn_audit_trigger();
END $evtsub$;
