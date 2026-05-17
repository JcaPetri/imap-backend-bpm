package com.imap.bpm.domain.engine;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Snapshot inmutable de una processversion publicada en system, traída via
 * GET /v1/admin/bpm/processversion/{id}/full y cacheada en Caffeine con TTL
 * infinito (porque la version es lock-once-used).
 *
 * Sirve como input al ProcessEngine: tiene TODO lo necesario para arrancar
 * + avanzar instancias sin volver a llamar system.
 */
public record ProcessDefinition(
    UUID processVersionId,
    UUID processdefId,
    String processdefCode,
    String processdefName,
    int version,
    List<FlowElement> flowElements,
    List<SequenceFlow> sequenceFlows,
    List<TaskForm> taskForms
) {
    /** Lookup por code (ej "start", "ing_pte"). */
    public FlowElement findElementByCode(String code) {
        return flowElements.stream().filter(fe -> code.equals(fe.code())).findFirst().orElse(null);
    }

    /** Lookup por id. */
    public FlowElement findElementById(UUID id) {
        return flowElements.stream().filter(fe -> id.equals(fe.id())).findFirst().orElse(null);
    }

    /** TaskForm asociado a un user_task flowelement (null si no tiene). */
    public TaskForm findTaskFormByFlowElement(UUID flowElementId) {
        return taskForms.stream()
            .filter(tf -> flowElementId.equals(tf.flowElementId()))
            .findFirst().orElse(null);
    }

    /** Sequence flows salientes de un flowelement, ordenados. */
    public List<SequenceFlow> outgoingFlows(UUID sourceId) {
        return sequenceFlows.stream()
            .filter(sf -> sourceId.equals(sf.sourceId()))
            .sorted(java.util.Comparator.comparingInt(SequenceFlow::sortOrder))
            .toList();
    }

    public record FlowElement(
        UUID id, String code, String type, String name,
        Map<String, Object> config, int sortOrder
    ) {}

    public record SequenceFlow(
        UUID id, UUID sourceId, UUID targetId,
        String sourceCode, String targetCode,
        String conditionExpr, int sortOrder
    ) {}

    public record TaskForm(
        UUID flowElementId, String flowElementCode,
        UUID entityDefId, String entityDefCode, String mode
    ) {}
}
