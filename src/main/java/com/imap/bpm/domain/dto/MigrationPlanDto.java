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

import java.util.List;

/**
 * DTOs para el endpoint /v1/bpm/admin/migration-plans (Hito 3 multi-version).
 *
 * Copia fiel del contrato de system.domain.dto.MigrationPlanDto — la gestión de
 * migration plans se portó a bpm relacional (F4-mgmt Chunk B): el plan ahora vive
 * en bpm_pro_migrationplan_tbl / bpm_pro_migrationrule_tbl (V015) en vez del
 * cell-store EAV de system. Records anidados coherentes con el resto de bpm.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MigrationPlanDto {

    public record CreatePlanRequest(
        String code,
        String description,
        String sourceProcessversionId,
        String targetProcessversionId
    ) {}

    public record RuleDto(
        String sourceFlowElementCode,
        String targetFlowElementCode,
        String action,    // "map" | "skip" | "cancel"
        Integer sortOrder,
        String notes
    ) {}

    public record UpdateRulesRequest(
        List<RuleDto> rules
    ) {}

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record PlanSummary(
        String planId,
        String code,
        String description,
        String sourceProcessversionId,
        String targetProcessversionId,
        String status,         // draft | validated | applied | failed
        String appliedAt,
        String appliedBy,
        String stats           // JSON string
    ) {}

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record PlanDetail(
        PlanSummary header,
        List<RuleDto> rules,
        ValidationReport validation   // null si no se validó aún
    ) {}

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public record ValidationReport(
        boolean valid,
        int sourceInstancesActive,
        List<String> errors,        // ej: "source code 'fill_form' no tiene regla"
        List<String> warnings       // ej: "target 'review' tiene tokens vivos en otra instance"
    ) {}
}
