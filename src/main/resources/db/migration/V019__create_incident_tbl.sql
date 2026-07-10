-- V019 — Incidentes de ejecución (Ola 4.2: incident management + retry-from-failure).
--
-- Cuando un service_task/job falla terminal (agota retries, sin boundary), en vez
-- de dejar el token 'failed' y la instancia colgada sin recuperacion, se registra
-- un INCIDENTE que ops puede inspeccionar y reintentar-desde-el-paso o resolver.
--
-- Runtime state (como tokens/jobs): el motor la escribe directo (exencion §F.6).
-- audit-7 + RLS + triggers como el resto de las tablas bpm (patron V015).

CREATE TABLE IF NOT EXISTS bpm.bpm_pro_incident_tbl (
    id                   UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    tenant_id            UUID NOT NULL,
    processinstance_id   UUID NOT NULL,
    token_id             UUID,
    element_id           UUID,            -- flow_element (service_task) que fallo
    element_code         TEXT,
    incident_type        VARCHAR(40) NOT NULL,   -- service_task_failure | job_failure
    error_code           VARCHAR(80),
    error_message        TEXT,
    lifecycle            VARCHAR(20) NOT NULL DEFAULT 'open'
        CHECK (lifecycle IN ('open', 'retrying', 'resolved')),
    resolved_at          TIMESTAMPTZ,
    -- nucleo audit-7:
    state_id             UUID NOT NULL,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by_id        UUID,
    updated_by_id        UUID,
    owned_by_id          UUID,
    CONSTRAINT fk_incident_instance FOREIGN KEY (processinstance_id)
        REFERENCES bpm.bpm_pro_processinstance_tbl(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_incident_tenant_lifecycle ON bpm.bpm_pro_incident_tbl(tenant_id, lifecycle);
CREATE INDEX IF NOT EXISTS idx_incident_instance ON bpm.bpm_pro_incident_tbl(processinstance_id);

-- RLS + nucleo fill + audit triggers (idem V015).
DO $incident$
BEGIN
    ALTER TABLE bpm.bpm_pro_incident_tbl ENABLE ROW LEVEL SECURITY;
    DROP POLICY IF EXISTS rls_tenant_isolation ON bpm.bpm_pro_incident_tbl;
    CREATE POLICY rls_tenant_isolation ON bpm.bpm_pro_incident_tbl
        USING (tenant_id IS NULL
               OR tenant_id = nullif(current_setting('app.current_tenant_id', true), '')::uuid
               OR current_setting('app.bypass_rls', true) = 'true')
        WITH CHECK (tenant_id IS NULL
               OR tenant_id = nullif(current_setting('app.current_tenant_id', true), '')::uuid
               OR current_setting('app.bypass_rls', true) = 'true');
    CREATE OR REPLACE TRIGGER trg_fill_nucleo BEFORE INSERT OR UPDATE ON bpm.bpm_pro_incident_tbl
        FOR EACH ROW EXECUTE FUNCTION bpm.fn_fill_nucleo();
    CREATE OR REPLACE TRIGGER trg_audit_bpm_pro_incident_tbl AFTER INSERT OR UPDATE OR DELETE ON bpm.bpm_pro_incident_tbl
        FOR EACH ROW EXECUTE FUNCTION bpm.fn_audit_trigger();
END $incident$;
