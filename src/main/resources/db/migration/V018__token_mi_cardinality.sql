-- V018 — Multi-instance parallel MVP: cardinalidad del token ancla.
--
-- Una activity (user_task / sub_process) marcada con config.multiInstance se
-- ejecuta N veces (una por item de una coleccion de runtime). El token que
-- entra a esa activity queda como "ancla" del join: guarda N en mi_cardinality
-- y espera hasta que los N tokens hijos terminen (join por cardinalidad).
--
-- NULL = token normal (no multi-instance). Columna nullable, sin backfill.
-- No toca RLS (solo agrega columna). Corre como admindb via split Flyway.

ALTER TABLE bpm.bpm_pro_token_tbl
    ADD COLUMN IF NOT EXISTS mi_cardinality INTEGER;

COMMENT ON COLUMN bpm.bpm_pro_token_tbl.mi_cardinality IS
    'Multi-instance (parallel): N de ejecuciones esperadas en el token ancla. NULL = token normal.';
