-- =============================================================================
-- IterBPM Paso 05 — Soporte de sub_process (call activity)
-- =============================================================================
-- Agrega columna parent_token_id a bpm.bpm_pro_processinstance_tbl para que
-- el motor BPM (Paso 07 B1) pueda relacionar una child ProcessInstance con
-- el TOKEN específico del parent que la spawnneó.
--
-- Por qué hace falta: parent_instance_id sola identifica al parent process,
-- pero si el parent tiene múltiples sub_process flow_elements en paralelo
-- (ej tras un parallel_gateway SPLIT), no podríamos discriminar qué token
-- avanzar al completar el child. La columna parent_token_id resuelve esto.
--
-- IDEMPOTENCIA: ADD COLUMN IF NOT EXISTS (Postgres 9.6+) y CREATE INDEX
-- IF NOT EXISTS. Re-ejecutable sin error.
-- =============================================================================

-- (transacción gestionada por Flyway)

ALTER TABLE bpm.bpm_pro_processinstance_tbl
    ADD COLUMN IF NOT EXISTS parent_token_id UUID;

COMMENT ON COLUMN bpm.bpm_pro_processinstance_tbl.parent_token_id IS
    'Si es child de un sub_process call activity, apunta al token del parent que está waiting en el sub_process flow_element. Null si es root instance.';

-- Index para lookup rápido cuando el child completa y necesita notificar al parent
CREATE INDEX IF NOT EXISTS idx_pi_parent_token
    ON bpm.bpm_pro_processinstance_tbl (parent_token_id)
    WHERE parent_token_id IS NOT NULL;

-- Index para reverse lookup (admin/debug: listar children de un parent)
CREATE INDEX IF NOT EXISTS idx_pi_parent_instance
    ON bpm.bpm_pro_processinstance_tbl (parent_instance_id)
    WHERE parent_instance_id IS NOT NULL;

DO $$
DECLARE
    v_count INT;
BEGIN
    SELECT COUNT(*) INTO v_count
      FROM information_schema.columns
     WHERE table_schema = 'bpm'
       AND table_name = 'bpm_pro_processinstance_tbl'
       AND column_name = 'parent_token_id';
    RAISE NOTICE '--- IterBPM Paso 05 OK ---';
    RAISE NOTICE 'parent_token_id column present: %', (v_count = 1);
END $$;
-- (commit gestionado por Flyway)
