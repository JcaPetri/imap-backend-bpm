// ─── GOLDEN-RULES:BEGIN (auto · golden-rules.json · no editar a mano) ───
// REGLAS DE ORO IMAP — cumplir SIEMPRE (ver IMAP_GUIA_DESARROLLO.md):
//  • HTTP-only entre servicios (+ s2s auth; no SQL cross-service; futuro Kafka)
//  • Names en inglés
//  • UUIDv7 en ids
//  • i18n: idioma del string, no de la fila; datos (UUID, field, idioma)
//  • VtR: único canal con el frontend (front solo ve virtual)
//  • Hexagonal estricto (domain no depende de infra)
//  • No secrets en código (.env en C:\Applications, nunca hardcodear)
//  • Idempotencia en operaciones de negocio (idempotency key)
//  • [bpm] Orquestador central (tipo Temporal/Step Functions)
//  • [bpm] processdef→processversion: snapshot inmutable al activar
//  • [bpm] Las defs de proceso viven acá (no en system)
//  • [bpm] Camunda 8 como norte; interim form-driven en prod
// ─── GOLDEN-RULES:END ───

package com.imap.bpm.domain.workhub;

/**
 * Resultado del cálculo de prioridad de una tarea para la bandeja del WorkHub.
 *
 * @param prioridadPct  score normalizado a % del máximo (0–100). 0 si la tarea
 *                      no tiene clasificación.
 * @param color         banda del semáforo (combina prioridad% con estado de SLA).
 * @param classified    true si el processdef/user_task tiene clasificación G/U/T
 *                      cargada; false si el % proviene solo del default (0).
 */
public record TaskPriority(double prioridadPct, SemaphoreColor color, boolean classified) {}
