-- =============================================================================
-- IterBPM Paso 15 — WorkHub: metadata de clasificación y priorización de tareas
-- =============================================================================
-- Soporta la priorización de la bandeja (WorkHub / frontend por procesos).
-- Diseño cerrado en: imap.enterprise/imap/docs/architecture/workhub-northstar.md
--
-- score_base  = SUM:     wg·Gravedad + wu·Urgencia + wt·Tendencia      (G/U/T 1–10)
--               PRODUCT: Gravedad · Urgencia · Tendencia               (estilo RPN/FMEA)
-- prioridad%  = score_base / score_max · 100        (normaliza: SUM 30 y PRODUCT 1000 → 100%)
-- semáforo    = umbrales fijos sobre prioridad% (rojo ≥ high, amarillo ≥ medium, verde resto)
--              + presión_SLA y override DMN se calculan en runtime (NO se persisten acá).
--
-- TABLAS FÍSICAS (sub-prefijo 'whb' = workhub):
--   1. bpm_whb_tenantconfig_tbl     — política por tenant (modo agregación + pesos + umbrales).
--                                       1 fila por tenant. Lo carga el admin en la pantalla
--                                       de admin de BPM.
--   2. bpm_whb_classification_tbl   — valores Gravedad/Urgencia/Tendencia por processdef
--                                       (y opcional override por user_task vía flowelement_id).
--
-- FKs CROSS-MICROSERVICE: UUID sin FK física (referencia LÓGICA).
--   processdef_id, processversion_id, flowelement_id → entities virtuales en system EAV
--   tenant_id, *_by_id                               → schema iam
--
-- VERSIONADO: processversion_id (nullable) permite congelar un snapshot por versión del
--   processdef (decisión §5.4). NULL = fila "draft / current" a nivel processdef; al activar
--   una versión se persiste una fila con processversion_id seteado (mismo patrón
--   processdef→processversion del motor). F0 trabaja a nivel processdef (processversion_id NULL).
--
-- AUDITORÍA: las 10 columnas estándar de IMAP. RLS DESACTIVADO (ver nota abajo,
--   consistente con Paso14 — aislación por WHERE en repos hasta el cleanup RLS bpm-wide).
-- IDEMPOTENCIA: CREATE IF NOT EXISTS. BEGIN/COMMIT.
-- =============================================================================

-- (transacción gestionada por Flyway)

SET search_path TO bpm, public;

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. bpm_whb_tenantconfig_tbl — Política de priorización por tenant
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bpm.bpm_whb_tenantconfig_tbl (
    id                   UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    tenant_id            UUID NOT NULL,                  -- FK lógica iam_tenant (1 fila por tenant)
    -- Modo de agregación del score (§5.1):
    aggregation_mode     VARCHAR(10) NOT NULL DEFAULT 'SUM'
        CHECK (aggregation_mode IN ('SUM', 'PRODUCT')),
    -- Pesos (aplican en modo SUM; default 1 → suma simple):
    weight_gravity       NUMERIC(5,2) NOT NULL DEFAULT 1 CHECK (weight_gravity  >= 0),
    weight_urgency       NUMERIC(5,2) NOT NULL DEFAULT 1 CHECK (weight_urgency  >= 0),
    weight_trend         NUMERIC(5,2) NOT NULL DEFAULT 1 CHECK (weight_trend    >= 0),
    -- Umbrales del semáforo en % (único juego para ambos modos, §5.3):
    threshold_high_pct   NUMERIC(5,2) NOT NULL DEFAULT 80
        CHECK (threshold_high_pct   BETWEEN 0 AND 100),
    threshold_medium_pct NUMERIC(5,2) NOT NULL DEFAULT 50
        CHECK (threshold_medium_pct BETWEEN 0 AND 100),
    -- Auditoría estándar:
    state_id             UUID NOT NULL,
    created_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    created_by_id        UUID,
    updated_by_id        UUID,
    owned_by_id          UUID,
    timezone_id          UUID,
    table_history        TEXT,
    data_language_id     UUID,
    CONSTRAINT uq_whb_tenantconfig_tenant UNIQUE (tenant_id),
    CONSTRAINT ck_whb_threshold_order CHECK (threshold_high_pct >= threshold_medium_pct)
);


-- ─────────────────────────────────────────────────────────────────────────────
-- 2. bpm_whb_classification_tbl — Gravedad/Urgencia/Tendencia por proceso (o tarea)
--    flowelement_id NULL  → clasificación a nivel processdef (aplica a todas sus tareas).
--    flowelement_id NOT NULL → override para un user_task puntual.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bpm.bpm_whb_classification_tbl (
    id                   UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    tenant_id            UUID NOT NULL,                  -- FK lógica iam_tenant
    -- FKs LÓGICAS cross-microservice (definitions en system EAV):
    processdef_id        UUID NOT NULL,                  -- entity virtual bpm_pro_processdef
    processversion_id    UUID,                           -- snapshot por versión (NULL = draft/current)
    flowelement_id       UUID,                           -- NULL = nivel proceso; NOT NULL = user_task
    -- Clasificación (escala 1–10):
    gravity              SMALLINT NOT NULL CHECK (gravity BETWEEN 1 AND 10),
    urgency              SMALLINT NOT NULL CHECK (urgency BETWEEN 1 AND 10),
    trend                SMALLINT NOT NULL CHECK (trend   BETWEEN 1 AND 10),
    -- Auditoría estándar:
    state_id             UUID NOT NULL,
    created_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    created_by_id        UUID,
    updated_by_id        UUID,
    owned_by_id          UUID,
    timezone_id          UUID,
    table_history        TEXT,
    data_language_id     UUID
);
-- Unicidad: una clasificación por (tenant, processdef[, user_task]) dentro de la misma versión.
-- Se modela con índices parciales por la nulabilidad de flowelement_id y processversion_id.
CREATE UNIQUE INDEX IF NOT EXISTS uq_whb_cls_proc_current
    ON bpm.bpm_whb_classification_tbl(tenant_id, processdef_id)
    WHERE flowelement_id IS NULL AND processversion_id IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_whb_cls_task_current
    ON bpm.bpm_whb_classification_tbl(tenant_id, processdef_id, flowelement_id)
    WHERE flowelement_id IS NOT NULL AND processversion_id IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_whb_cls_proc_version
    ON bpm.bpm_whb_classification_tbl(tenant_id, processversion_id)
    WHERE flowelement_id IS NULL AND processversion_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_whb_cls_task_version
    ON bpm.bpm_whb_classification_tbl(tenant_id, processversion_id, flowelement_id)
    WHERE flowelement_id IS NOT NULL AND processversion_id IS NOT NULL;
-- Lookup principal del score en runtime:
CREATE INDEX IF NOT EXISTS idx_whb_cls_lookup
    ON bpm.bpm_whb_classification_tbl(tenant_id, processdef_id);


-- =============================================================================
-- RLS — multi-tenant isolation (policy idéntica a Paso02)
-- =============================================================================
-- RLS DESACTIVADO — consistente con la realidad actual de bpm (igual que
-- bpm_pro_message_start_subscription_tbl / Paso14): bpm conecta con un rol
-- owner que bypassa RLS y NO tiene un TenantContextService aplicando
-- SET LOCAL app.current_tenant_id en las queries JPA. La aislación tenant se
-- garantiza por WHERE explícito (tenant_id = :tenant) en los repositories.
-- Cuando se haga el cleanup RLS BPM-wide (iter dedicado) estas tablas se
-- reactivan con el mismo patrón que system/inventory.
ALTER TABLE bpm.bpm_whb_tenantconfig_tbl   DISABLE ROW LEVEL SECURITY;
ALTER TABLE bpm.bpm_whb_classification_tbl DISABLE ROW LEVEL SECURITY;


-- =============================================================================
-- GRANTs — asume que el user 'bpm_app' existe (mismo patrón Paso02).
-- =============================================================================
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bpm_app') THEN
        GRANT SELECT, INSERT, UPDATE, DELETE ON
            bpm.bpm_whb_tenantconfig_tbl,
            bpm.bpm_whb_classification_tbl
            TO bpm_app;
        RAISE NOTICE 'GRANTs WorkHub aplicados a bpm_app';
    ELSE
        RAISE NOTICE 'User bpm_app NO existe — GRANTs salteados.';
    END IF;
END $$;


-- =============================================================================
-- Verificación final
-- =============================================================================
DO $$
DECLARE
    v_n_tables INT;
BEGIN
    SELECT COUNT(*) INTO v_n_tables
        FROM information_schema.tables
        WHERE table_schema = 'bpm' AND table_name LIKE 'bpm_whb_%';

    RAISE NOTICE '--- WorkHub (V005) OK ---';
    RAISE NOTICE 'WorkHub tables: %', v_n_tables;
END $$;
-- (commit gestionado por Flyway)
