-- =============================================================================
-- BPM V009 — bpm_inbox_event_tbl SIN RLS (fix de V008)
-- =============================================================================
-- V008 le puso RLS por consistencia, pero el dedup se inserta en el controller FUERA de la tx
-- donde EavTenantSession setea el GUC → el WITH CHECK fallaba (GUC vacío) → 500 en /messages/start.
--
-- El inbox es una tabla de INFRA de dedup (igual que sale.outbox_event): se consulta SIEMPRE con
-- tenant_id explícito (existsByTenantIdAndEventId) y los event_id son UUIDs únicos globales → no hay
-- riesgo de fuga cross-tenant. Por eso va SIN RLS. Excepción consciente a S7, documentada.
-- =============================================================================

ALTER TABLE bpm.bpm_inbox_event_tbl DISABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS bpm_tenant_isolation ON bpm.bpm_inbox_event_tbl;

COMMENT ON TABLE bpm.bpm_inbox_event_tbl IS
    'Dedup de message-start (Consistencia #3-B). SIN RLS: tabla de infra, se consulta con tenant_id explícito.';
