package com.imap.bpm.domain.engine;

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
    List<Rule> rules
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
