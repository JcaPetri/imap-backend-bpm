-- =============================================================================
-- BPM V017 — fn_fill_nucleo defensivo en UPDATE (fix arranque de procesos)
-- =============================================================================
-- BUG (pre-existente, destapado al validar F8): arrancar un proceso tiraba
-- "null value in column owned_by_id of bpm_pro_token_tbl violates not-null".
-- Causa: fn_fill_nucleo (V010) en UPDATE solo llenaba updated_at/updated_by_id,
-- NO re-llenaba owned_by_id. El engine crea el token (INSERT → trigger llena
-- owned_by_id en la DB), pero la entity Hibernate en memoria queda con
-- owned_by_id=null; al hacer moveTokenToElement (UPDATE) manda ese null →
-- viola NOT NULL. Mismo patrón que el created_at del processdef.
--
-- FIX: en UPDATE, re-llenar owned_by_id / created_at / created_by_id cuando la
-- fila los manda NULL (patrón Hibernate stale-entity tras INSERT trigger-filled).
-- Solo actúa cuando vienen null → no pisa valores reales. En bpm owned_by_id es
-- siempre zero-uuid (sin user-GUC) → sin pérdida. CREATE OR REPLACE propaga a
-- todos los triggers trg_fill_nucleo ya attacheados (no hay que re-attachear).
--
-- Fix de libro (follow-up): @Column(updatable=false) en las columnas de audit de
-- las entities para que Hibernate no las incluya en el UPDATE. admindb.
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
        -- Defensivo (V017): re-llenar columnas de audit inmutables si la entity manda NULL
        -- (Hibernate no refresca la entity tras el INSERT trigger-filled). Evita violar NOT NULL.
        IF j ? 'created_at'    AND j->>'created_at'    IS NULL THEN patch := patch || jsonb_build_object('created_at', now()); END IF;
        IF j ? 'created_by_id' AND j->>'created_by_id' IS NULL THEN patch := patch || jsonb_build_object('created_by_id', g); END IF;
        IF j ? 'owned_by_id'   AND j->>'owned_by_id'   IS NULL THEN patch := patch || jsonb_build_object('owned_by_id', COALESCE(j->>'created_by_id', g::text)); END IF;
    END IF;
    IF patch <> '{}'::jsonb THEN NEW := jsonb_populate_record(NEW, patch); END IF;
    RETURN NEW;
END;
$fn$;

DO $$ BEGIN RAISE NOTICE '--- V017: fn_fill_nucleo ahora re-llena owned_by_id/created_at/created_by_id en UPDATE (fix arranque procesos) ---'; END $$;
