-- V022 — Ola 7.1: DMN DRD chaining. Dependencias entre decisiones.
--
-- Una decision puede requerir OTRAS decisiones (informationRequirements del spec DMN):
-- para evaluarla, primero se evaluan sus dependencias (orden topologico) y sus outputs
-- se inyectan como inputs. required_decisions guarda un array JSON de codes de decision.
-- NULL / [] = decision hoja (sin dependencias, comportamiento previo intacto).
-- NOTA: sin sintaxis de placeholder dollar-brace en este archivo (Flyway placeholder-replacement).

ALTER TABLE bpm.bpm_dmn_decisiondef_tbl
    ADD COLUMN IF NOT EXISTS required_decisions JSONB;
