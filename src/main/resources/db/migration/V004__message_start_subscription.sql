-- =============================================================================
-- IterBPM Paso 14 — bpm_pro_message_start_subscription_tbl
-- =============================================================================
-- Agrega capacidad al motor BPM de arrancar procesos por message correlation
-- (event-based start). Hoy correlateMessage solo despierta tokens waiting de
-- intermediate events — esta tabla permite que startEvents con
-- config.message.messageCode también disparen instances.
--
-- Patrón:
--   1. ProcessDefinitionLoader popula la tabla al cargar un processdef con
--      startEvents que tienen messageCode (UPSERT idempotente).
--   2. Cuando una version nueva del mismo processdef se carga, la subscription
--      vieja se marca is_active=false (V1 simple — solo current version dispara).
--   3. POST /v1/bpm/messages/start busca subscriptions activas matching el
--      messageCode + tenant, y arranca un instance por cada match (broadcast
--      a múltiples processdefs distintos suscritos al mismo evento).
--
-- Tabla NO particionada (volumen bajo — pocos message-start subscriptions por tenant).
-- RLS habilitado: WHERE tenant_id = current_setting('app.current_tenant_id', true).
-- =============================================================================

CREATE TABLE IF NOT EXISTS bpm.bpm_pro_message_start_subscription_tbl (
    id                       UUID            NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    tenant_id                UUID            NOT NULL,
    message_code             VARCHAR(100)    NOT NULL,
    processdef_id            UUID            NOT NULL,
    processversion_id        UUID            NOT NULL,
    start_flow_element_id    UUID            NOT NULL,
    is_active                BOOLEAN         NOT NULL DEFAULT true,

    -- Columnas estándar IMAP
    state_id            UUID                     NOT NULL,
    created_at          TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    created_by_id       UUID,
    updated_by_id       UUID,
    owned_by_id         UUID,
    timezone_id         UUID,
    table_history       TEXT,
    data_language_id    UUID,

    -- Una version-startEvent solo puede tener una subscription por message
    CONSTRAINT bpm_pro_msg_start_sub_unique
        UNIQUE (tenant_id, message_code, processversion_id, start_flow_element_id)
);

-- Lookup principal: dado un message entrante, buscar todas las subscriptions activas del tenant.
CREATE INDEX IF NOT EXISTS idx_bpm_msg_start_sub_lookup
    ON bpm.bpm_pro_message_start_subscription_tbl (tenant_id, message_code, is_active);

-- Para desactivar viejas al subir nueva version del mismo processdef
CREATE INDEX IF NOT EXISTS idx_bpm_msg_start_sub_processdef
    ON bpm.bpm_pro_message_start_subscription_tbl (tenant_id, message_code, processdef_id);

COMMENT ON TABLE bpm.bpm_pro_message_start_subscription_tbl IS
    'Subscriptions de startEvents BPMN tipo "message" para soportar event-based process start. Populada por ProcessDefinitionLoader al cargar un processdef con startEvents que tienen config.message.messageCode. Consultada por POST /v1/bpm/messages/start.';

-- ─── RLS ────────────────────────────────────────────────────────────────────
-- DESACTIVADO consistente con el resto del schema BPM (que aún no tiene un
-- TenantContextService aplicando SET LOCAL app.current_tenant_id en las queries
-- JPA). Cuando se haga el cleanup RLS BPM-wide en un iter dedicado, esta tabla
-- se reactiva con el mismo patrón que las tablas inventory/system.
-- Mientras tanto, la aislación tenant se garantiza por WHERE explícito en los
-- query methods del repository (findActiveByTenantAndMessageCode, etc.).
ALTER TABLE bpm.bpm_pro_message_start_subscription_tbl DISABLE ROW LEVEL SECURITY;
