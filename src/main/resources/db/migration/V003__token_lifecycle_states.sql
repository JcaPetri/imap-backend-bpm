-- =============================================================================
-- IterBPM_Schema_BD_Paso13 — agrega 'failed' y 'cancelled' al CHECK de token.lifecycle
-- 2026-05-23 — Fase 0.B.1 del plan Inventory (ServiceTaskRegistry)
-- =============================================================================
--
-- Bug latente descubierto durante el smoke de ServiceTaskRegistry: el constraint
-- bpm_pro_token_tbl_lifecycle_check solo aceptaba {active, waiting, consumed}.
-- El motor BPM intenta setear 'failed' (cuando un service_task agota retries)
-- y 'cancelled' (en cancelInstance + migration skip handler) — ambos fallaban
-- silently con DataIntegrityViolationException → rollback del trans + 500.
--
-- Idempotente — DROP IF EXISTS antes del nuevo CREATE.
-- =============================================================================

ALTER TABLE bpm.bpm_pro_token_tbl
  DROP CONSTRAINT IF EXISTS bpm_pro_token_tbl_lifecycle_check;

ALTER TABLE bpm.bpm_pro_token_tbl
  ADD CONSTRAINT bpm_pro_token_tbl_lifecycle_check
  CHECK (lifecycle IN ('active', 'waiting', 'consumed', 'cancelled', 'failed'));

-- Verify
SELECT conname, pg_get_constraintdef(c.oid)
  FROM pg_constraint c
  JOIN pg_class cl ON cl.oid = c.conrelid
 WHERE cl.relname = 'bpm_pro_token_tbl'
   AND c.contype = 'c';
