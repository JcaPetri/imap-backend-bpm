-- =============================================================================
-- bpm — drop timezone_id de TODAS las tablas del schema
-- =============================================================================
-- timezone_id es redundante con timestamptz (instante UTC); la tz de display es del
-- usuario/tenant, no de cada fila. Limpieza del núcleo. 
-- admindb.
-- =============================================================================
SET search_path TO bpm, public;

DO $$
DECLARE t text; n int := 0;
BEGIN
  FOR t IN
    -- relkind r=tabla, p=particionada (padre). Dropear del padre cascadea a las hijas;
    -- attinhcount=0 evita tocar columnas heredadas de una partición hija (error).
    SELECT c.relname FROM pg_class c JOIN pg_namespace nsp ON nsp.oid = c.relnamespace
    WHERE nsp.nspname = 'bpm' AND c.relkind IN ('r','p')
      AND EXISTS (SELECT 1 FROM pg_attribute a
                  WHERE a.attrelid = c.oid AND a.attname = 'timezone_id'
                    AND NOT a.attisdropped AND a.attinhcount = 0)
      
    ORDER BY c.relname
  LOOP
    EXECUTE format('ALTER TABLE bpm.%I DROP COLUMN timezone_id', t);
    n := n + 1;
  END LOOP;
  RAISE NOTICE '--- drop timezone_id: % tablas en bpm ---', n;
END $$;
