-- =============================================================================
-- IterBPM Paso 02 — Schema 'bpm' + tablas físicas del microservicio bpm
-- =============================================================================
-- Crea el schema 'bpm' en la BD 'iam' (database-per-service estricto puede
-- esperar; arrancamos con schema-per-service como hace system).
--
-- TABLAS FÍSICAS (en este orden):
--   1. bpm_dataelement_tbl       — cell-store local (clon de sys_baseelement_tbl)
--                                  para entities virtuales BPM con
--                                  microservice_code='bpm' (catálogos del módulo).
--   2-9. 8 tablas Convencionales para ejecución (alto volumen, FKs físicas):
--        - bpm_pro_processinstance_tbl
--        - bpm_pro_token_tbl
--        - bpm_hum_taskinstance_tbl
--        - bpm_pro_variable_tbl
--        - bpm_pro_auditlog_tbl
--        - bpm_pro_jobexecutor_tbl
--        - bpm_pro_messagecorrelation_tbl
--        - bpm_pro_compensation_tbl
--
-- FKs CROSS-MICROSERVICE: UUID sin FK física (la referencia es LÓGICA).
--   processversion_id    → entity virtual viviendo en system EAV
--   assigned_user_id     → iam_user_tbl (schema iam)
--   tenant_id            → iam_tenant_tbl (schema iam)
--   created_by_id, etc.  → iam_user_tbl
--
-- FKs WITHIN-MICROSERVICE: FK física Postgres declarada.
--
-- AUDITORÍA: las 10 columnas estándar de IMAP en todas las tablas.
-- RLS: policy idéntica a system (current_tenant OR system_tenant OR bypass).
--
-- IDEMPOTENCIA: CREATE IF NOT EXISTS donde Postgres lo permite.
-- =============================================================================

-- (transacción gestionada por Flyway — sin BEGIN/COMMIT explícitos)

-- ─────────────────────────────────────────────────────────────────────────────
-- SCHEMA
-- ─────────────────────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS bpm;
-- Incluí 'public' para que gin_trgm_ops (de la extensión pg_trgm) se resuelva.
SET search_path TO bpm, public;

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. bpm_dataelement_tbl — Cell-store local del microservicio bpm
--    Clon idéntico de system.sys_baseelement_tbl. Cualquier entity virtual
--    con microservice_code='bpm' guarda sus cells acá.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bpm.bpm_dataelement_tbl (
    id                  UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    tenant_id           UUID NOT NULL,                  -- FK lógica iam_tenant
    scope_id            UUID NOT NULL,                  -- FK lógica entity_def (vive en system)
    father_id           UUID NOT NULL,                  -- cell-store: N cells con mismo father_id = 1 record
    fieldtable_id       UUID NOT NULL,                  -- FK lógica entitystructure (vive en system)
    dataelement         TEXT,                           -- el valor del cell (renombrado de baseelement)
    is_key              BOOLEAN NOT NULL DEFAULT false,
    state_id            UUID NOT NULL,                  -- FK lógica sys_state
    created_at          TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    created_by_id       UUID,                           -- FK lógica iam_user
    updated_by_id       UUID,
    owned_by_id         UUID,
    timezone_id         UUID,
    table_history       TEXT,
    data_language_id    UUID
);

-- Indexes (mismos que sys_baseelement_tbl)
CREATE INDEX IF NOT EXISTS idx_de_scope_field        ON bpm.bpm_dataelement_tbl(scope_id, fieldtable_id);
CREATE INDEX IF NOT EXISTS idx_de_father             ON bpm.bpm_dataelement_tbl(father_id);
CREATE INDEX IF NOT EXISTS idx_de_scope_tenant       ON bpm.bpm_dataelement_tbl(scope_id, tenant_id);
CREATE INDEX IF NOT EXISTS idx_de_scope_field_iskey  ON bpm.bpm_dataelement_tbl(scope_id, fieldtable_id, is_key)
    WHERE is_key = true;
CREATE INDEX IF NOT EXISTS idx_de_value_trgm         ON bpm.bpm_dataelement_tbl USING gin(dataelement gin_trgm_ops);


-- ─────────────────────────────────────────────────────────────────────────────
-- 2. bpm_pro_processinstance_tbl — Instancia viva del proceso
--    Snapshot del processversion_id (que vive en system EAV).
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bpm.bpm_pro_processinstance_tbl (
    id                   UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    tenant_id            UUID NOT NULL,
    -- FKs LÓGICAS cross-microservice (definitions en system):
    processdef_id        UUID NOT NULL,                 -- entity virtual bpm_pro_processdef
    processversion_id    UUID NOT NULL,                 -- entity virtual bpm_pro_processversion
    -- Hierarchy:
    parent_instance_id   UUID,                          -- self-FK para sub-procesos (call activity)
    -- State:
    lifecycle            VARCHAR(20) NOT NULL DEFAULT 'active'
        CHECK (lifecycle IN ('active', 'completed', 'cancelled', 'failed', 'suspended')),
    started_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    ended_at             TIMESTAMP WITH TIME ZONE,
    started_by_id        UUID,                          -- FK lógica iam_user
    correlation_key      VARCHAR(255),                  -- para encontrar instance por business key
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
    CONSTRAINT fk_pi_parent FOREIGN KEY (parent_instance_id)
        REFERENCES bpm.bpm_pro_processinstance_tbl(id) ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS idx_pi_tenant_lifecycle  ON bpm.bpm_pro_processinstance_tbl(tenant_id, lifecycle);
CREATE INDEX IF NOT EXISTS idx_pi_processdef        ON bpm.bpm_pro_processinstance_tbl(processdef_id);
CREATE INDEX IF NOT EXISTS idx_pi_correlation       ON bpm.bpm_pro_processinstance_tbl(correlation_key)
    WHERE correlation_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_pi_parent            ON bpm.bpm_pro_processinstance_tbl(parent_instance_id)
    WHERE parent_instance_id IS NOT NULL;


-- ─────────────────────────────────────────────────────────────────────────────
-- 3. bpm_pro_token_tbl — Tokens activos avanzando por flow_elements
--    1..N tokens por instance (parallel gateways crean tokens hijos).
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bpm.bpm_pro_token_tbl (
    id                   UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    tenant_id            UUID NOT NULL,
    processinstance_id   UUID NOT NULL,
    -- FK lógica al flow_element (entity virtual en system):
    current_element_id   UUID NOT NULL,
    -- Hierarchy de tokens (parallel gateway split):
    parent_token_id      UUID,                          -- self-FK
    -- State:
    lifecycle            VARCHAR(20) NOT NULL DEFAULT 'active'
        CHECK (lifecycle IN ('active', 'waiting', 'consumed')),
    entered_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
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
    CONSTRAINT fk_tok_instance FOREIGN KEY (processinstance_id)
        REFERENCES bpm.bpm_pro_processinstance_tbl(id) ON DELETE CASCADE,
    CONSTRAINT fk_tok_parent FOREIGN KEY (parent_token_id)
        REFERENCES bpm.bpm_pro_token_tbl(id) ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS idx_tok_instance     ON bpm.bpm_pro_token_tbl(processinstance_id);
CREATE INDEX IF NOT EXISTS idx_tok_element      ON bpm.bpm_pro_token_tbl(current_element_id);
CREATE INDEX IF NOT EXISTS idx_tok_active       ON bpm.bpm_pro_token_tbl(processinstance_id, lifecycle)
    WHERE lifecycle IN ('active', 'waiting');


-- ─────────────────────────────────────────────────────────────────────────────
-- 4. bpm_hum_taskinstance_tbl — Tareas humanas
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bpm.bpm_hum_taskinstance_tbl (
    id                   UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    tenant_id            UUID NOT NULL,
    processinstance_id   UUID NOT NULL,
    -- FK lógica al user_task flow_element (entity virtual en system):
    flowelement_id       UUID NOT NULL,
    token_id             UUID,                          -- el token que generó la task
    -- State:
    lifecycle            VARCHAR(20) NOT NULL DEFAULT 'created'
        CHECK (lifecycle IN ('created', 'assigned', 'in_progress', 'completed', 'cancelled', 'expired')),
    -- Asignación:
    assigned_user_id     UUID,                          -- FK lógica iam_user
    assigned_role        VARCHAR(100),                  -- fallback si no hay user
    priority             INTEGER NOT NULL DEFAULT 50,
    due_at               TIMESTAMP WITH TIME ZONE,      -- SLA
    -- Timestamps de ciclo de vida:
    assigned_at          TIMESTAMP WITH TIME ZONE,
    started_at           TIMESTAMP WITH TIME ZONE,
    completed_at         TIMESTAMP WITH TIME ZONE,
    -- Data:
    input_data_jsonb     JSONB,                         -- datos al crearse
    output_data_jsonb    JSONB,                         -- datos al completarse
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
    CONSTRAINT fk_ti_instance FOREIGN KEY (processinstance_id)
        REFERENCES bpm.bpm_pro_processinstance_tbl(id) ON DELETE CASCADE,
    CONSTRAINT fk_ti_token FOREIGN KEY (token_id)
        REFERENCES bpm.bpm_pro_token_tbl(id) ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS idx_ti_assigned_user ON bpm.bpm_hum_taskinstance_tbl(assigned_user_id, lifecycle)
    WHERE assigned_user_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_ti_assigned_role ON bpm.bpm_hum_taskinstance_tbl(assigned_role, lifecycle)
    WHERE assigned_role IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_ti_instance      ON bpm.bpm_hum_taskinstance_tbl(processinstance_id);
CREATE INDEX IF NOT EXISTS idx_ti_due           ON bpm.bpm_hum_taskinstance_tbl(due_at)
    WHERE due_at IS NOT NULL AND lifecycle IN ('created', 'assigned', 'in_progress');
CREATE INDEX IF NOT EXISTS idx_ti_tenant_pending ON bpm.bpm_hum_taskinstance_tbl(tenant_id, lifecycle)
    WHERE lifecycle IN ('created', 'assigned', 'in_progress');


-- ─────────────────────────────────────────────────────────────────────────────
-- 5. bpm_pro_variable_tbl — Variables del proceso (k/v por instance)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bpm.bpm_pro_variable_tbl (
    id                   UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    tenant_id            UUID NOT NULL,
    processinstance_id   UUID NOT NULL,
    var_name             VARCHAR(100) NOT NULL,
    var_value            TEXT,
    var_type             VARCHAR(20) NOT NULL
        CHECK (var_type IN ('string', 'number', 'date', 'datetime', 'boolean', 'json')),
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
    CONSTRAINT fk_var_instance FOREIGN KEY (processinstance_id)
        REFERENCES bpm.bpm_pro_processinstance_tbl(id) ON DELETE CASCADE,
    CONSTRAINT uq_var_instance_name UNIQUE (processinstance_id, var_name)
);
CREATE INDEX IF NOT EXISTS idx_var_instance ON bpm.bpm_pro_variable_tbl(processinstance_id);


-- ─────────────────────────────────────────────────────────────────────────────
-- 6. bpm_pro_auditlog_tbl — Log de cada transición del proceso
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bpm.bpm_pro_auditlog_tbl (
    id                   UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    tenant_id            UUID NOT NULL,
    processinstance_id   UUID NOT NULL,
    event_type           VARCHAR(50) NOT NULL,          -- 'instance.started', 'token.entered',
                                                         -- 'task.assigned', 'task.completed',
                                                         -- 'gateway.evaluated', 'instance.ended', etc.
    flowelement_id       UUID,                          -- FK lógica (puede ser NULL para eventos globales)
    token_id             UUID,                          -- FK lógica
    user_id              UUID,                          -- FK lógica iam_user (quién disparó)
    occurred_at          TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    data_jsonb           JSONB,                         -- contexto adicional
    correlation_id       VARCHAR(100),                  -- para tracing
    -- Auditoría estándar (created_at = occurred_at conceptualmente):
    state_id             UUID NOT NULL,
    created_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    created_by_id        UUID,
    updated_by_id        UUID,
    owned_by_id          UUID,
    timezone_id          UUID,
    table_history        TEXT,
    data_language_id     UUID,
    CONSTRAINT fk_al_instance FOREIGN KEY (processinstance_id)
        REFERENCES bpm.bpm_pro_processinstance_tbl(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_al_instance      ON bpm.bpm_pro_auditlog_tbl(processinstance_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_al_event_type    ON bpm.bpm_pro_auditlog_tbl(event_type);
CREATE INDEX IF NOT EXISTS idx_al_correlation   ON bpm.bpm_pro_auditlog_tbl(correlation_id)
    WHERE correlation_id IS NOT NULL;


-- ─────────────────────────────────────────────────────────────────────────────
-- 7. bpm_pro_jobexecutor_tbl — Timers programados (cron-like del motor)
--    Job worker corre cada N segundos: SELECT WHERE fire_at <= NOW() AND state='scheduled'
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bpm.bpm_pro_jobexecutor_tbl (
    id                   UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    tenant_id            UUID NOT NULL,
    processinstance_id   UUID NOT NULL,
    token_id             UUID,                          -- timer asociado a un token específico
    job_type             VARCHAR(30) NOT NULL
        CHECK (job_type IN ('timer', 'retry', 'escalation', 'async_continuation')),
    fire_at              TIMESTAMP WITH TIME ZONE NOT NULL,
    config_jsonb         JSONB,                         -- duration, retry policy, etc
    lifecycle            VARCHAR(20) NOT NULL DEFAULT 'scheduled'
        CHECK (lifecycle IN ('scheduled', 'firing', 'fired', 'cancelled', 'failed')),
    retries              INTEGER NOT NULL DEFAULT 0,
    max_retries          INTEGER NOT NULL DEFAULT 3,
    last_error           TEXT,
    fired_at             TIMESTAMP WITH TIME ZONE,
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
    CONSTRAINT fk_job_instance FOREIGN KEY (processinstance_id)
        REFERENCES bpm.bpm_pro_processinstance_tbl(id) ON DELETE CASCADE,
    CONSTRAINT fk_job_token FOREIGN KEY (token_id)
        REFERENCES bpm.bpm_pro_token_tbl(id) ON DELETE CASCADE
);
-- Index crítico para el job worker:
CREATE INDEX IF NOT EXISTS idx_job_due ON bpm.bpm_pro_jobexecutor_tbl(fire_at)
    WHERE lifecycle = 'scheduled';
CREATE INDEX IF NOT EXISTS idx_job_instance ON bpm.bpm_pro_jobexecutor_tbl(processinstance_id);


-- ─────────────────────────────────────────────────────────────────────────────
-- 8. bpm_pro_messagecorrelation_tbl — Match mensajes entrantes con instances
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bpm.bpm_pro_messagecorrelation_tbl (
    id                   UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    tenant_id            UUID NOT NULL,
    processinstance_id   UUID NOT NULL,
    token_id             UUID,                          -- el token waiting por el mensaje
    -- FK lógica al messagedef (entity virtual en system):
    messagedef_id        UUID NOT NULL,
    correlation_key      VARCHAR(255) NOT NULL,         -- ej: "order_12345"
    lifecycle            VARCHAR(20) NOT NULL DEFAULT 'waiting'
        CHECK (lifecycle IN ('waiting', 'matched', 'expired', 'cancelled')),
    expires_at           TIMESTAMP WITH TIME ZONE,
    matched_at           TIMESTAMP WITH TIME ZONE,
    matched_payload_jsonb JSONB,                        -- payload del mensaje que matcheó
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
    CONSTRAINT fk_mc_instance FOREIGN KEY (processinstance_id)
        REFERENCES bpm.bpm_pro_processinstance_tbl(id) ON DELETE CASCADE,
    CONSTRAINT fk_mc_token FOREIGN KEY (token_id)
        REFERENCES bpm.bpm_pro_token_tbl(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_mc_correlation ON bpm.bpm_pro_messagecorrelation_tbl(messagedef_id, correlation_key)
    WHERE lifecycle = 'waiting';
CREATE INDEX IF NOT EXISTS idx_mc_instance ON bpm.bpm_pro_messagecorrelation_tbl(processinstance_id);


-- ─────────────────────────────────────────────────────────────────────────────
-- 9. bpm_pro_compensation_tbl — Handlers de compensación registrados
--    Cuando un activity completa y tiene compensation handler, se registra acá.
--    Si después aparece un compensation throw, se invocan los handlers
--    correspondientes en orden inverso al de ejecución.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bpm.bpm_pro_compensation_tbl (
    id                       UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    tenant_id                UUID NOT NULL,
    processinstance_id       UUID NOT NULL,
    -- FK lógica al activity (flow_element) original:
    completed_element_id     UUID NOT NULL,
    -- FK lógica al compensation handler (flow_element):
    compensation_element_id  UUID NOT NULL,
    completion_order         INTEGER NOT NULL,           -- inverso al ejecutar
    completion_data_jsonb    JSONB,                      -- snapshot de variables al completar
    lifecycle                VARCHAR(20) NOT NULL DEFAULT 'registered'
        CHECK (lifecycle IN ('registered', 'compensating', 'compensated', 'failed', 'cancelled')),
    compensated_at           TIMESTAMP WITH TIME ZONE,
    -- Auditoría estándar:
    state_id                 UUID NOT NULL,
    created_at               TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at               TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    created_by_id            UUID,
    updated_by_id            UUID,
    owned_by_id              UUID,
    timezone_id              UUID,
    table_history            TEXT,
    data_language_id         UUID,
    CONSTRAINT fk_comp_instance FOREIGN KEY (processinstance_id)
        REFERENCES bpm.bpm_pro_processinstance_tbl(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_comp_instance ON bpm.bpm_pro_compensation_tbl(processinstance_id, completion_order DESC)
    WHERE lifecycle = 'registered';


-- =============================================================================
-- RLS — multi-tenant isolation (policy idéntica a system)
-- =============================================================================
-- Aplica a todas las tablas: current_tenant OR system_tenant OR bypass_rls
DO $$
DECLARE
    v_table TEXT;
    v_tables TEXT[] := ARRAY[
        'bpm_dataelement_tbl',
        'bpm_pro_processinstance_tbl',
        'bpm_pro_token_tbl',
        'bpm_hum_taskinstance_tbl',
        'bpm_pro_variable_tbl',
        'bpm_pro_auditlog_tbl',
        'bpm_pro_jobexecutor_tbl',
        'bpm_pro_messagecorrelation_tbl',
        'bpm_pro_compensation_tbl'
    ];
BEGIN
    FOREACH v_table IN ARRAY v_tables LOOP
        EXECUTE format('ALTER TABLE bpm.%I ENABLE ROW LEVEL SECURITY', v_table);
        EXECUTE format('DROP POLICY IF EXISTS rls_tenant_isolation ON bpm.%I', v_table);
        EXECUTE format($p$
            CREATE POLICY rls_tenant_isolation ON bpm.%I
            USING (
                tenant_id = '00000000-0000-0000-0000-000000000001'::uuid
                OR tenant_id::text = current_setting('app.current_tenant_id', true)
                OR current_setting('app.bypass_rls', true) = 'true'
            )
        $p$, v_table);
    END LOOP;
END $$;


-- =============================================================================
-- GRANTs — asume que el user 'bpm_app' existe.
-- Si NO existe, este SQL falla. Crear primero (manualmente, fuera de este SQL):
--   CREATE USER bpm_app WITH ENCRYPTED PASSWORD '<set-via-vault>';
-- =============================================================================
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'bpm_app') THEN
        GRANT USAGE ON SCHEMA bpm TO bpm_app;
        GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA bpm TO bpm_app;
        GRANT USAGE ON ALL SEQUENCES IN SCHEMA bpm TO bpm_app;
        ALTER DEFAULT PRIVILEGES IN SCHEMA bpm
            GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO bpm_app;
        ALTER DEFAULT PRIVILEGES IN SCHEMA bpm
            GRANT USAGE ON SEQUENCES TO bpm_app;
        RAISE NOTICE 'GRANTs aplicados a bpm_app';
    ELSE
        RAISE NOTICE 'User bpm_app NO existe — GRANTs salteados. Crear user y re-ejecutar el bloque DO de GRANTs.';
    END IF;
END $$;


-- =============================================================================
-- Verificación final
-- =============================================================================
DO $$
DECLARE
    v_n_tables INT;
    v_n_indexes INT;
    v_n_policies INT;
BEGIN
    SELECT COUNT(*) INTO v_n_tables
        FROM information_schema.tables WHERE table_schema = 'bpm' AND table_type = 'BASE TABLE';
    SELECT COUNT(*) INTO v_n_indexes
        FROM pg_indexes WHERE schemaname = 'bpm';
    SELECT COUNT(*) INTO v_n_policies
        FROM pg_policy p JOIN pg_class c ON c.oid = p.polrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'bpm';

    RAISE NOTICE '--- IterBPM Paso 02 OK ---';
    RAISE NOTICE 'BPM tables: %', v_n_tables;
    RAISE NOTICE 'BPM indexes: %', v_n_indexes;
    RAISE NOTICE 'BPM RLS policies: %', v_n_policies;
END $$;
-- (commit gestionado por Flyway)
