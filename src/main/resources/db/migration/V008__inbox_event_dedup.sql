-- =============================================================================
-- BPM V008 — Inbox de dedup de message-start (Consistencia Fase 0 #3-B / outbox)
-- =============================================================================
-- El outbox de los micros entrega at-least-once (reintenta hasta confirmar). Para que un
-- reintento NO arranque el proceso dos veces, /v1/bpm/messages/start acepta un eventId y
-- deduplica contra esta tabla (unique tenant_id+event_id): check-exists antes + record-after-start.
--
-- RLS uniforme (espejo de V006): el endpoint corre con contexto de tenant del request
-- (X-Tenant-Id → GUC), así que la policy aplica normal. bypass_rls disponible por consistencia.
-- =============================================================================

CREATE TABLE IF NOT EXISTS bpm.bpm_inbox_event_tbl (
    id                 UUID                     NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    tenant_id          UUID                     NOT NULL,
    event_id           UUID                     NOT NULL,
    message_code       VARCHAR(100)             NOT NULL,
    instances_started  INT                      NOT NULL DEFAULT 0,
    processed_at       TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_bpm_inbox_event UNIQUE (tenant_id, event_id)
);

CREATE INDEX IF NOT EXISTS ix_bpm_inbox_processed_at ON bpm.bpm_inbox_event_tbl (processed_at);

ALTER TABLE bpm.bpm_inbox_event_tbl ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS bpm_tenant_isolation ON bpm.bpm_inbox_event_tbl;
CREATE POLICY bpm_tenant_isolation ON bpm.bpm_inbox_event_tbl
    USING (tenant_id IS NULL
           OR tenant_id = nullif(current_setting('app.current_tenant_id', true), '')::uuid
           OR current_setting('app.bypass_rls', true) = 'true')
    WITH CHECK (tenant_id IS NULL
           OR tenant_id = nullif(current_setting('app.current_tenant_id', true), '')::uuid
           OR current_setting('app.bypass_rls', true) = 'true');

COMMENT ON TABLE bpm.bpm_inbox_event_tbl IS
    'Dedup de message-start para entrega at-least-once del outbox (Consistencia #3-B).';
