-- =============================================================================
-- bpm — drop data_language_id + table_history (columnas muertas del núcleo §3 i18n)
-- =============================================================================
-- data_language_id: el idioma NO va por fila (lo resuelve el store i18n). table_history:
-- lo reemplazó <m>_audit_log_tbl (Doctrina D v2). Ambas muertas (no las usa el código).
-- DO-loop genérico (relkind r/p, attinhcount=0 → drop del padre cascadea a particiones). admindb.
-- =============================================================================
SET search_path TO bpm, public;

DO $$
DECLARE col text; t text; n int := 0;
BEGIN
  FOREACH col IN ARRAY ARRAY['data_language_id','table_history'] LOOP
    FOR t IN
      SELECT c.relname FROM pg_class c JOIN pg_namespace nsp ON nsp.oid = c.relnamespace
      WHERE nsp.nspname = 'bpm' AND c.relkind IN ('r','p')
        AND EXISTS (SELECT 1 FROM pg_attribute a
                    WHERE a.attrelid = c.oid AND a.attname = col
                      AND NOT a.attisdropped AND a.attinhcount = 0)
      ORDER BY c.relname
    LOOP
      EXECUTE format('ALTER TABLE bpm.%I DROP COLUMN %I', t, col);
      n := n + 1;
    END LOOP;
  END LOOP;
  RAISE NOTICE '--- drop data_language_id+table_history: % columnas en bpm ---', n;
END $$;
