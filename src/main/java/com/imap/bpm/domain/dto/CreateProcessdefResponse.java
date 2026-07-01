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

package com.imap.bpm.domain.dto;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.UUID;

/**
 * Response del POST /v1/bpm/admin/processdef.
 *
 * `processdefId` y `processversionId` son null si fue dryRun (solo validación).
 * `stats` cuenta cuántas rows relacionales se crearon por tabla.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record CreateProcessdefResponse(
    UUID processdefId,
    UUID processversionId,
    Integer version,
    Stats stats,
    String message,
    Boolean dryRun
) {
    public record Stats(
        int flowElements,
        int sequenceFlows,
        int taskForms
    ) {}
}
