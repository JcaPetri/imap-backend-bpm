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

package com.imap.bpm.application.engine;

import java.util.List;
import java.util.UUID;

/**
 * Snapshot inmutable de una decision table DMN publicada en system, traída
 * via GET /v1/admin/bpm/decisiondef/by-code/{code}/full y cacheada en
 * Caffeine. Sirve como input al `DmnEvaluator`.
 *
 * Las rules están ordenadas por `priority` ascendente.
 */
public record DecisionDefinition(
    UUID id,
    String code,
    String name,
    String description,
    /** "unique" | "first" — MVP. Priority/collect/any quedan pendientes. */
    String hitPolicy,
    List<SchemaEntry> inputSchema,
    List<SchemaEntry> outputSchema,
    List<Rule> rules,
    /** DRD chaining (Ola 7.1): codes de decisiones que hay que evaluar ANTES (sus outputs → inputs). */
    List<String> requiredDecisions
) {
    /** Entrada del schema: nombre de la variable + type ("string", "number", "boolean", "date"). */
    public record SchemaEntry(String varName, String type) {}

    /** Una fila de la decision table. */
    public record Rule(
        UUID id,
        int priority,
        List<RuleInput> inputs,
        List<RuleOutput> outputs,
        String description
    ) {}

    /**
     * Condición sobre una variable de input.
     *   varName  — nombre de la variable a evaluar (debe matchear inputSchema)
     *   operator — "any" | "eq" | "ne" | "lt" | "lte" | "gt" | "gte" | "in" | "between"
     *   value    — literal o array, según operator:
     *                eq/ne/lt/lte/gt/gte → escalar
     *                in → array de valores aceptados
     *                between → array [min, max]
     *                any → ignorado (wildcard, siempre matchea)
     */
    public record RuleInput(String varName, String operator, Object value) {}

    /** Valor de output de una rule. Si la rule matchea, se setea como variable del processinstance. */
    public record RuleOutput(String varName, Object value) {}
}
