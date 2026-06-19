-- =============================================================================
-- bpm V010 — Auditoría Doctrina D v2 (alta desde cero; bpm no tenía change-audit)
-- =============================================================================
-- bpm NO tenía auditoría de cambios a nivel DB (bpm_pro_auditlog_tbl es audit de
-- PROCESO escrito por el motor, NO un change-log → se deja intacto).
-- Núcleo ya completo en las tablas de negocio. Esta migración agrega:
--   1. fn_fill_nucleo (jsonb) + owned_by_id NOT NULL.
--   2. fn_audit_trigger v2 + bpm_audit_log_tbl particionada + append-only + cobertura.
-- changed_by_id: bpm no setea app.current_user_id → quedará SYSTEM (captura qué/cuándo).
-- Excluidas: bpm_dataelement (EAV), bpm_inbox_event (cola), bpm_pro_auditlog (audit de proceso).
-- admindb. PG 18.4.
-- =============================================================================
SET search_path TO bpm, public;

CREATE OR REPLACE FUNCTION bpm.fn_fill_nucleo()
RETURNS trigger LANGUAGE plpgsql AS $fn$
DECLARE
    g     uuid  := COALESCE(nullif(current_setting('app.current_user_id', true), '')::uuid,
                            '00000000-0000-0000-0000-000000000000');
    j     jsonb := to_jsonb(NEW);
    patch jsonb := '{}'::jsonb;
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF j ? 'created_by_id' AND j->>'created_by_id' IS NULL THEN patch := patch || jsonb_build_object('created_by_id', g); END IF;
        IF j ? 'created_at'    AND j->>'created_at'    IS NULL THEN patch := patch || jsonb_build_object('created_at', now()); END IF;
        IF j ? 'owned_by_id'   AND j->>'owned_by_id'   IS NULL THEN patch := patch || jsonb_build_object('owned_by_id', COALESCE(j->>'created_by_id', g::text)); END IF;
        IF j ? 'updated_at'    AND j->>'updated_at'    IS NULL THEN patch := patch || jsonb_build_object('updated_at', now()); END IF;
        IF j ? 'updated_by_id' AND j->>'updated_by_id' IS NULL THEN patch := patch || jsonb_build_object('updated_by_id', g); END IF;
    ELSIF TG_OP = 'UPDATE' THEN
        IF j ? 'updated_at'    THEN patch := patch || jsonb_build_object('updated_at', now()); END IF;
        IF j ? 'updated_by_id' THEN patch := patch || jsonb_build_object('updated_by_id', g); END IF;
    END IF;
    IF patch <> '{}'::jsonb THEN NEW := jsonb_populate_record(NEW, patch); END IF;
    RETURN NEW;
END;
$fn$;

DO $loop$
DECLARE t text;
BEGIN
    FOR t IN
        SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname='bpm' AND c.relkind='r' AND c.relname LIKE 'bpm_%'
          AND c.relname NOT IN ('bpm_dataelement_tbl','bpm_inbox_event_tbl','bpm_pro_auditlog_tbl')
          AND c.relname NOT LIKE 'bpm_audit_log%'
          AND EXISTS (SELECT 1 FROM information_schema.columns x WHERE x.table_schema='bpm' AND x.table_name=c.relname AND x.column_name='tenant_id')
          AND EXISTS (SELECT 1 FROM information_schema.columns x WHERE x.table_schema='bpm' AND x.table_name=c.relname AND x.column_name='owned_by_id')
        ORDER BY c.relname
    LOOP
        EXECUTE format('UPDATE bpm.%I SET owned_by_id = COALESCE(created_by_id, %L::uuid) WHERE owned_by_id IS NULL', t, '00000000-0000-0000-0000-000000000000');
        EXECUTE format('DROP TRIGGER IF EXISTS trg_set_updated_at ON bpm.%I', t);
        EXECUTE format('DROP TRIGGER IF EXISTS trg_fill_nucleo ON bpm.%I', t);
        EXECUTE format('CREATE TRIGGER trg_fill_nucleo BEFORE INSERT OR UPDATE ON bpm.%I FOR EACH ROW EXECUTE FUNCTION bpm.fn_fill_nucleo()', t);
        EXECUTE format('ALTER TABLE bpm.%I ALTER COLUMN owned_by_id SET NOT NULL', t);
    END LOOP;
END $loop$;

CREATE OR REPLACE FUNCTION bpm.fn_audit_trigger()
RETURNS TRIGGER LANGUAGE plpgsql SECURITY DEFINER AS $fn$
DECLARE
    v_old JSONB; v_new JSONB; v_old_diff JSONB; v_new_diff JSONB; v_user_id UUID;
    c_system CONSTANT UUID := '00000000-0000-0000-0000-000000000000';
BEGIN
    IF TG_OP = 'INSERT' THEN RETURN NEW; END IF;
    BEGIN v_user_id := nullif(current_setting('app.current_user_id', true), '')::UUID;
    EXCEPTION WHEN OTHERS THEN v_user_id := NULL; END;
    v_user_id := COALESCE(v_user_id, c_system);
    IF TG_OP = 'DELETE' THEN
        INSERT INTO bpm.bpm_audit_log_tbl (tenant_id, table_name, operation, row_id, old_data, new_data, changed_by_id)
        VALUES (OLD.tenant_id, TG_TABLE_NAME, 'D', OLD.id, to_jsonb(OLD), NULL, v_user_id);
        RETURN OLD;
    END IF;
    v_old := to_jsonb(OLD); v_new := to_jsonb(NEW);
    SELECT jsonb_object_agg(k, v_new -> k) INTO v_new_diff FROM jsonb_object_keys(v_new) AS k WHERE (v_new -> k) IS DISTINCT FROM (v_old -> k);
    IF v_new_diff IS NULL THEN RETURN NEW; END IF;
    SELECT jsonb_object_agg(k, v_old -> k) INTO v_old_diff FROM jsonb_object_keys(v_old) AS k WHERE (v_new -> k) IS DISTINCT FROM (v_old -> k);
    INSERT INTO bpm.bpm_audit_log_tbl (tenant_id, table_name, operation, row_id, old_data, new_data, changed_by_id)
    VALUES (NEW.tenant_id, TG_TABLE_NAME, 'U', NEW.id, v_old_diff, v_new_diff, v_user_id);
    RETURN NEW;
END;
$fn$;

CREATE TABLE bpm.bpm_audit_log_tbl (
    id UUID NOT NULL DEFAULT gen_random_uuid(),
    tenant_id UUID,
    table_name VARCHAR(100) NOT NULL,
    operation CHAR(1) NOT NULL CHECK (operation IN ('U','D')),
    row_id UUID,
    old_data JSONB,
    new_data JSONB,
    changed_by_id UUID NOT NULL,
    changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (id, changed_at)
) PARTITION BY RANGE (changed_at);

DO $part$
DECLARE d date := date '2026-06-01'; e date; nm text;
BEGIN
    WHILE d < date '2027-07-01' LOOP
        e := (d + interval '1 month')::date;
        nm := 'bpm_audit_log_' || to_char(d, 'YYYY_MM');
        EXECUTE format('CREATE TABLE IF NOT EXISTS bpm.%I PARTITION OF bpm.bpm_audit_log_tbl FOR VALUES FROM (%L) TO (%L)', nm, d, e);
        d := e;
    END LOOP;
END $part$;
CREATE TABLE IF NOT EXISTS bpm.bpm_audit_log_default PARTITION OF bpm.bpm_audit_log_tbl DEFAULT;

CREATE INDEX idx_bpm_audit_table_row ON bpm.bpm_audit_log_tbl (table_name, row_id, changed_at DESC);
CREATE INDEX idx_bpm_audit_tenant    ON bpm.bpm_audit_log_tbl (tenant_id, changed_at DESC) WHERE tenant_id IS NOT NULL;

ALTER TABLE bpm.bpm_audit_log_tbl ENABLE ROW LEVEL SECURITY;
CREATE POLICY bpm_audit_isolation ON bpm.bpm_audit_log_tbl
    USING      (tenant_id IS NULL OR tenant_id = nullif(current_setting('app.current_tenant_id', true), '')::uuid OR current_setting('app.bypass_rls', true) = 'true')
    WITH CHECK (tenant_id IS NULL OR tenant_id = nullif(current_setting('app.current_tenant_id', true), '')::uuid OR current_setting('app.bypass_rls', true) = 'true');

REVOKE ALL            ON bpm.bpm_audit_log_tbl FROM bpm_app;
GRANT  SELECT         ON bpm.bpm_audit_log_tbl TO   bpm_app;
REVOKE UPDATE, DELETE ON bpm.bpm_audit_log_tbl FROM PUBLIC;

CREATE OR REPLACE FUNCTION bpm.fn_audit_append_only()
RETURNS TRIGGER LANGUAGE plpgsql AS $ao$
BEGIN
    RAISE EXCEPTION 'bpm.bpm_audit_log_tbl es append-only (intento de % bloqueado)', TG_OP USING ERRCODE = 'check_violation';
END;
$ao$;
CREATE TRIGGER trg_audit_append_only BEFORE UPDATE OR DELETE ON bpm.bpm_audit_log_tbl FOR EACH ROW EXECUTE FUNCTION bpm.fn_audit_append_only();

DO $cov$
DECLARE t text;
BEGIN
    FOR t IN
        SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname='bpm' AND c.relkind='r' AND c.relname LIKE 'bpm_%'
          AND c.relname NOT IN ('bpm_dataelement_tbl','bpm_inbox_event_tbl','bpm_pro_auditlog_tbl')
          AND c.relname NOT LIKE 'bpm_audit_log%'
          AND EXISTS (SELECT 1 FROM information_schema.columns x WHERE x.table_schema='bpm' AND x.table_name=c.relname AND x.column_name='id')
          AND EXISTS (SELECT 1 FROM information_schema.columns x WHERE x.table_schema='bpm' AND x.table_name=c.relname AND x.column_name='tenant_id')
        ORDER BY c.relname
    LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS trg_audit_%s ON bpm.%I', t, t);
        EXECUTE format('CREATE TRIGGER trg_audit_%s AFTER INSERT OR UPDATE OR DELETE ON bpm.%I FOR EACH ROW EXECUTE FUNCTION bpm.fn_audit_trigger()', t, t);
    END LOOP;
END $cov$;

DO $$
DECLARE v_parts int; v_trg int; v_fill int;
BEGIN
    SELECT count(*) INTO v_parts FROM pg_inherits WHERE inhparent='bpm.bpm_audit_log_tbl'::regclass;
    SELECT count(DISTINCT c.relname) INTO v_trg FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='bpm' AND t.tgname LIKE 'trg_audit_%' AND NOT t.tgisinternal AND c.relkind='r' AND c.relname NOT LIKE 'bpm_audit_log%';
    SELECT count(*) INTO v_fill FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='bpm' AND t.tgname='trg_fill_nucleo' AND NOT t.tgisinternal;
    RAISE NOTICE '--- V010 — bpm audit v2: % part, % audit, % fill ---', v_parts, v_trg, v_fill;
END $$;
