-- V021 — Ola 6.1: correlation_key en las suscripciones de event sub-process.
--
-- Para los triggers de MESSAGE (punto-a-punto), la suscripción necesita una clave de
-- correlación para rutear el mensaje a la instancia correcta (a diferencia de signal,
-- que es broadcast). Se resuelve al registrar la suscripción (literal o expresión JEXL
-- contra las variables de la instancia). NULL / '*' = wildcard (matchea cualquier
-- correlationKey de ese messageCode). No aplica a signal/error/timer.
-- NOTA: sin sintaxis de placeholder dollar-brace en este archivo — Flyway hace
-- placeholder-replacement y falla el parse si encuentra una var no configurada.

ALTER TABLE bpm.bpm_pro_event_subscription_tbl
    ADD COLUMN IF NOT EXISTS correlation_key VARCHAR(200);
