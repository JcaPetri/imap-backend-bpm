-- =============================================================================
-- bpm — recrear bpm_dataelement_tbl como cell-store EAV (modelo system)
-- =============================================================================
-- Réplica fiel de system.sys_baseelement_tbl (15 cols, SIN table_history) para el
-- cell-store EAV propio del micro (cargar datos EAV del propio bpm).
-- Decisiones: fill_nucleo SÍ (llena owned_by_id NOT NULL), audit NO (convención
-- per-micro: el dataelement no se audita). Borra la tabla vieja + sus datos.
-- RLS estándar + GRANT a bpm_app. uuidv7()/pg_trgm son DB-wide. admindb.
-- =============================================================================
SET search_path TO bpm, public;

DROP TABLE IF EXISTS bpm.bpm_dataelement_tbl CASCADE;

CREATE TABLE bpm.bpm_dataelement_tbl (
    id               uuid        NOT NULL DEFAULT uuidv7(),
    tenant_id        uuid        NOT NULL,
    scope_id         uuid        NOT NULL,
    father_id        uuid        NOT NULL,
    fieldtable_id    uuid        NOT NULL,
    baseelement      text,
    is_key           boolean     NOT NULL DEFAULT false,
    state_id         uuid        NOT NULL,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    created_by_id    uuid,
    updated_by_id    uuid,
    owned_by_id      uuid        NOT NULL,
    timezone_id      uuid,
    data_language_id uuid,
    CONSTRAINT bpm_dataelement_tbl_pkey PRIMARY KEY (id)
);

-- índices genéricos del cell-store (NO los uq_*_business_key específicos de system)
CREATE INDEX idx_bpm_de_scope_field       ON bpm.bpm_dataelement_tbl (scope_id, fieldtable_id);
CREATE INDEX idx_bpm_de_father            ON bpm.bpm_dataelement_tbl (father_id);
CREATE INDEX idx_bpm_de_scope_tenant      ON bpm.bpm_dataelement_tbl (scope_id, tenant_id);
CREATE INDEX idx_bpm_de_scope_field_iskey ON bpm.bpm_dataelement_tbl (scope_id, fieldtable_id, is_key) WHERE is_key = true;
CREATE INDEX idx_bpm_de_value_trgm        ON bpm.bpm_dataelement_tbl USING gin (baseelement gin_trgm_ops);

-- RLS estándar (tenant isolation) + grant al rol de app
ALTER TABLE bpm.bpm_dataelement_tbl ENABLE ROW LEVEL SECURITY;
CREATE POLICY bpm_tenant_isolation ON bpm.bpm_dataelement_tbl
  USING (tenant_id IS NULL OR tenant_id = (NULLIF(current_setting('app.current_tenant_id', true), ''))::uuid);
GRANT SELECT, INSERT, UPDATE, DELETE ON bpm.bpm_dataelement_tbl TO bpm_app;

-- núcleo (OBLIGATORIO: llena owned_by_id NOT NULL). SIN audit (convención per-micro).
CREATE TRIGGER trg_fill_nucleo BEFORE INSERT OR UPDATE ON bpm.bpm_dataelement_tbl
  FOR EACH ROW EXECUTE FUNCTION bpm.fn_fill_nucleo();

DO $$
DECLARE c int;
BEGIN
  SELECT count(*) INTO c FROM information_schema.columns
   WHERE table_schema='bpm' AND table_name='bpm_dataelement_tbl';
  RAISE NOTICE '--- bpm_dataelement_tbl recreado: % columnas ---', c;
END $$;
