-- =============================================================================
-- BPM V015 — Tablas RELACIONALES de definición de procesos (Fase 1 de la migración
--            de ownership: defs de proceso system(EAV) → bpm(relacional))
-- =============================================================================
-- Diseño: IMAP_BPM_OWNERSHIP_MIGRATION.md (decisión Juan 2026-06-30: bpm dueño de
-- la DATA de defs + su gestión; convenciones: IMAP_BACKEND_HEXAGONAL_POSTGRES.md).
--
-- Crea 9 tablas CONVENCIONALES relacionales en schema bpm para las defs que hoy
-- viven como EAV en system.sys_baseelement_tbl. Motivo del relacional vs EAV:
-- las defs son un GRAFO (FKs reales: version→def, flowelement→version,
-- sequenceflow→source/target flowelement, rule→decisiondef, etc.) → integridad
-- referencial nativa + queries de topología simples + forma Camunda.
--
-- FASE 1 = SOLO estructura (tablas vacías). La carga de datos (Fase 3) y el
-- cutover de código/engine/frontend (Fases 4-7) van aparte. Estas tablas quedan
-- INERTES: ningún código las lee/escribe todavía → cero impacto en el path BPM
-- actual (que sigue usando el cell-store de system).
--
-- Convenciones (idénticas a las tablas bpm existentes V001/V006/V010):
--   PK uuidv7() · núcleo 7-col (tenant_id, state_id, created_at/by_id,
--   updated_at/by_id, owned_by_id) · FKs intra-schema reales · state_id = FK
--   lógica a system.sys_state · RLS rls_tenant_isolation · triggers fn_fill_nucleo
--   + fn_audit_trigger · JSONB para payloads variables (definition/config/schemas).
-- Idempotente (CREATE IF NOT EXISTS / DO-guards).
-- admindb (Flyway DDL).
-- =============================================================================
SET search_path TO bpm, public;

-- ─────────────────────────────────────────────────────────────────────────────
-- 1. processdef
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bpm.bpm_pro_processdef_tbl (
    id                 UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    tenant_id          UUID NOT NULL,
    code               TEXT NOT NULL,
    name               TEXT NOT NULL,
    description        TEXT,
    currentversion_id  UUID,                          -- pointer lógico (circular con processversion → sin FK física)
    lifecycle          VARCHAR(20) NOT NULL DEFAULT 'active',
    start_permission   TEXT,
    state_id           UUID NOT NULL,                 -- FK lógica system.sys_state
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by_id      UUID,
    updated_by_id      UUID,
    owned_by_id        UUID
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_processdef_code ON bpm.bpm_pro_processdef_tbl(tenant_id, code);

-- ─────────────────────────────────────────────────────────────────────────────
-- 2. processversion
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bpm.bpm_pro_processversion_tbl (
    id                 UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    tenant_id          UUID NOT NULL,
    processdef_id      UUID NOT NULL,
    version            INTEGER NOT NULL,
    published_at       TIMESTAMPTZ,
    definition         JSONB,
    is_locked          BOOLEAN NOT NULL DEFAULT false,
    description        TEXT,
    state_id           UUID NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by_id      UUID,
    updated_by_id      UUID,
    owned_by_id        UUID,
    CONSTRAINT fk_pv_processdef FOREIGN KEY (processdef_id)
        REFERENCES bpm.bpm_pro_processdef_tbl(id)
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_processversion ON bpm.bpm_pro_processversion_tbl(tenant_id, processdef_id, version);
CREATE INDEX IF NOT EXISTS idx_pv_processdef ON bpm.bpm_pro_processversion_tbl(processdef_id);

-- ─────────────────────────────────────────────────────────────────────────────
-- 3. flowelement
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bpm.bpm_pro_flowelement_tbl (
    id                 UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    tenant_id          UUID NOT NULL,
    processversion_id  UUID NOT NULL,
    element_code       TEXT NOT NULL,
    element_type       VARCHAR(40) NOT NULL,
    name               TEXT,
    config             JSONB,
    sort_order         INTEGER,
    state_id           UUID NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by_id      UUID,
    updated_by_id      UUID,
    owned_by_id        UUID,
    CONSTRAINT fk_fe_processversion FOREIGN KEY (processversion_id)
        REFERENCES bpm.bpm_pro_processversion_tbl(id)
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_flowelement ON bpm.bpm_pro_flowelement_tbl(tenant_id, processversion_id, element_code);
CREATE INDEX IF NOT EXISTS idx_fe_processversion ON bpm.bpm_pro_flowelement_tbl(processversion_id);

-- ─────────────────────────────────────────────────────────────────────────────
-- 4. sequenceflow
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bpm.bpm_pro_sequenceflow_tbl (
    id                 UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    tenant_id          UUID NOT NULL,
    processversion_id  UUID NOT NULL,
    source_id          UUID NOT NULL,
    target_id          UUID NOT NULL,
    condition_expr     TEXT,
    sort_order         INTEGER,
    description        TEXT,
    state_id           UUID NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by_id      UUID,
    updated_by_id      UUID,
    owned_by_id        UUID,
    CONSTRAINT fk_sf_processversion FOREIGN KEY (processversion_id)
        REFERENCES bpm.bpm_pro_processversion_tbl(id),
    CONSTRAINT fk_sf_source FOREIGN KEY (source_id)
        REFERENCES bpm.bpm_pro_flowelement_tbl(id),
    CONSTRAINT fk_sf_target FOREIGN KEY (target_id)
        REFERENCES bpm.bpm_pro_flowelement_tbl(id)
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_sequenceflow ON bpm.bpm_pro_sequenceflow_tbl(tenant_id, processversion_id, source_id, target_id);
CREATE INDEX IF NOT EXISTS idx_sf_processversion ON bpm.bpm_pro_sequenceflow_tbl(processversion_id);

-- ─────────────────────────────────────────────────────────────────────────────
-- 5. taskform
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bpm.bpm_hum_taskform_tbl (
    id                 UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    tenant_id          UUID NOT NULL,
    flowelement_id     UUID NOT NULL,
    entitydef_id       UUID,                          -- FK lógica system.sys_entity_def (cross-service)
    mode               VARCHAR(20),
    state_id           UUID NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by_id      UUID,
    updated_by_id      UUID,
    owned_by_id        UUID,
    CONSTRAINT fk_tf_flowelement FOREIGN KEY (flowelement_id)
        REFERENCES bpm.bpm_pro_flowelement_tbl(id)
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_taskform ON bpm.bpm_hum_taskform_tbl(tenant_id, flowelement_id);

-- ─────────────────────────────────────────────────────────────────────────────
-- 6. decisiondef (DMN)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bpm.bpm_dmn_decisiondef_tbl (
    id                 UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    tenant_id          UUID NOT NULL,
    code               TEXT NOT NULL,
    name               TEXT NOT NULL,
    description        TEXT,
    input_schema       JSONB,
    output_schema      JSONB,
    hit_policy         VARCHAR(20),
    state_id           UUID NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by_id      UUID,
    updated_by_id      UUID,
    owned_by_id        UUID
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_decisiondef_code ON bpm.bpm_dmn_decisiondef_tbl(tenant_id, code);

-- ─────────────────────────────────────────────────────────────────────────────
-- 7. dmn rule
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bpm.bpm_dmn_rule_tbl (
    id                 UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    tenant_id          UUID NOT NULL,
    decisiondef_id     UUID NOT NULL,
    priority           INTEGER NOT NULL,
    inputs             JSONB,
    outputs            JSONB,
    description        TEXT,
    state_id           UUID NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by_id      UUID,
    updated_by_id      UUID,
    owned_by_id        UUID,
    CONSTRAINT fk_rule_decisiondef FOREIGN KEY (decisiondef_id)
        REFERENCES bpm.bpm_dmn_decisiondef_tbl(id)
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_dmn_rule ON bpm.bpm_dmn_rule_tbl(tenant_id, decisiondef_id, priority);
CREATE INDEX IF NOT EXISTS idx_rule_decisiondef ON bpm.bpm_dmn_rule_tbl(decisiondef_id);

-- ─────────────────────────────────────────────────────────────────────────────
-- 8. migrationplan
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bpm.bpm_pro_migrationplan_tbl (
    id                        UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    tenant_id                 UUID NOT NULL,
    code                      TEXT NOT NULL,
    description               TEXT,
    source_processversion_id  UUID NOT NULL,
    target_processversion_id  UUID NOT NULL,
    status                    VARCHAR(20) NOT NULL,
    applied_at                TIMESTAMPTZ,
    applied_by                UUID,
    stats                     JSONB,
    state_id                  UUID NOT NULL,
    created_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by_id             UUID,
    updated_by_id             UUID,
    owned_by_id               UUID,
    CONSTRAINT fk_mp_source FOREIGN KEY (source_processversion_id)
        REFERENCES bpm.bpm_pro_processversion_tbl(id),
    CONSTRAINT fk_mp_target FOREIGN KEY (target_processversion_id)
        REFERENCES bpm.bpm_pro_processversion_tbl(id)
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_migrationplan_code ON bpm.bpm_pro_migrationplan_tbl(tenant_id, code);

-- ─────────────────────────────────────────────────────────────────────────────
-- 9. migrationrule
-- ─────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bpm.bpm_pro_migrationrule_tbl (
    id                          UUID NOT NULL DEFAULT uuidv7() PRIMARY KEY,
    tenant_id                   UUID NOT NULL,
    migrationplan_id            UUID NOT NULL,
    source_flowelement_code     TEXT NOT NULL,
    target_flowelement_code     TEXT,
    action                      VARCHAR(20) NOT NULL,
    sort_order                  INTEGER,
    notes                       TEXT,
    state_id                    UUID NOT NULL,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_by_id               UUID,
    updated_by_id               UUID,
    owned_by_id                 UUID,
    CONSTRAINT fk_mr_migrationplan FOREIGN KEY (migrationplan_id)
        REFERENCES bpm.bpm_pro_migrationplan_tbl(id)
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_migrationrule ON bpm.bpm_pro_migrationrule_tbl(tenant_id, migrationplan_id, source_flowelement_code);
CREATE INDEX IF NOT EXISTS idx_mr_migrationplan ON bpm.bpm_pro_migrationrule_tbl(migrationplan_id);

-- ─────────────────────────────────────────────────────────────────────────────
-- RLS + triggers (mismo patrón que V006/V010) sobre las 9 tablas nuevas
-- ─────────────────────────────────────────────────────────────────────────────
DO $$
DECLARE t text;
DECLARE tbls text[] := ARRAY[
    'bpm_pro_processdef_tbl','bpm_pro_processversion_tbl','bpm_pro_flowelement_tbl',
    'bpm_pro_sequenceflow_tbl','bpm_hum_taskform_tbl','bpm_dmn_decisiondef_tbl',
    'bpm_dmn_rule_tbl','bpm_pro_migrationplan_tbl','bpm_pro_migrationrule_tbl'];
BEGIN
    FOREACH t IN ARRAY tbls LOOP
        -- RLS
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
        -- núcleo fill + audit triggers (PG18: CREATE OR REPLACE TRIGGER)
        EXECUTE format('CREATE OR REPLACE TRIGGER trg_fill_nucleo BEFORE INSERT OR UPDATE ON bpm.%I FOR EACH ROW EXECUTE FUNCTION bpm.fn_fill_nucleo()', t);
        EXECUTE format('CREATE OR REPLACE TRIGGER trg_audit_%s AFTER INSERT OR UPDATE OR DELETE ON bpm.%I FOR EACH ROW EXECUTE FUNCTION bpm.fn_audit_trigger()', t, t);
    END LOOP;
END $$;

DO $$
DECLARE v_n INT;
BEGIN
    SELECT count(*) INTO v_n FROM information_schema.tables
     WHERE table_schema='bpm' AND table_name IN (
        'bpm_pro_processdef_tbl','bpm_pro_processversion_tbl','bpm_pro_flowelement_tbl',
        'bpm_pro_sequenceflow_tbl','bpm_hum_taskform_tbl','bpm_dmn_decisiondef_tbl',
        'bpm_dmn_rule_tbl','bpm_pro_migrationplan_tbl','bpm_pro_migrationrule_tbl');
    RAISE NOTICE '--- V015: % / 9 tablas relacionales de def creadas en schema bpm ---', v_n;
END $$;
