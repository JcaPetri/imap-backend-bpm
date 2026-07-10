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

    /**
     * B2 — devuelve los boundary_event flow_elements adjuntos a una activity
     * (por code). Convención: el boundary_event tiene
     * config.boundary.attachedTo = elementCode del activity.
     */
    @SuppressWarnings("unchecked")
    public List<FlowElement> findBoundariesFor(String elementCode) {
        if (elementCode == null) return java.util.List.of();
        return flowElements.stream()
            .filter(fe -> "boundary_event".equals(fe.type()))
            .filter(fe -> {
                Map<String, Object> cfg = fe.config();
                if (cfg == null) return false;
                Map<String, Object> boundary = (Map<String, Object>) cfg.get("boundary");
                if (boundary == null) return false;
                return elementCode.equals(boundary.get("attachedTo"));
            })
            .toList();
    }

    /**
     * Advanced — filtra boundaries adjuntos por errorCode matching para
     * raiseTaskError. Acepta:
     *   - boundaries con `error.errorCode == errorCode` (exact match)
     *   - boundaries con `escalation.escalationCode == errorCode` (escalation flavor)
     *   - boundaries con `error.errorCode == "*"` o `null` (catch-all)
     *
     * Si errorCode == null, retorna solo catch-all.
     */
    @SuppressWarnings("unchecked")
    public List<FlowElement> findErrorBoundariesFor(String elementCode, String errorCode) {
        return findBoundariesFor(elementCode).stream()
            .filter(fe -> {
                Map<String, Object> cfg = fe.config();
                Map<String, Object> errorCfg = (Map<String, Object>) cfg.get("error");
                Map<String, Object> escalCfg = (Map<String, Object>) cfg.get("escalation");
                if (errorCfg == null && escalCfg == null) return false;
                String declared = errorCfg != null ? (String) errorCfg.get("errorCode")
                                : (String) escalCfg.get("escalationCode");
                if (declared == null || "*".equals(declared)) return true;   // catch-all
                return declared.equals(errorCode);
            })
            .toList();
    }

    /**
     * Compensation handler de una activity: un service_task off-path marcado con
     * config.compensationFor = elementCode del activity compensable. Null si no hay.
     */
    @SuppressWarnings("unchecked")
    public FlowElement findCompensationHandlerFor(String activityCode) {
        if (activityCode == null) return null;
        return flowElements.stream()
            .filter(fe -> "service_task".equals(fe.type()) && fe.config() != null)
            .filter(fe -> activityCode.equals(fe.config().get("compensationFor")))
            .findFirst().orElse(null);
    }

    /** Sequence flows salientes de un flowelement, ordenados. */
    public List<SequenceFlow> outgoingFlows(UUID sourceId) {
        return sequenceFlows.stream()
            .filter(sf -> sourceId.equals(sf.sourceId()))
            .sorted(java.util.Comparator.comparingInt(SequenceFlow::sortOrder))
            .toList();
    }

    /** Sequence flows entrantes a un flowelement (uso: detectar JOIN gateways). */
    public List<SequenceFlow> incomingFlows(UUID targetId) {
        return sequenceFlows.stream()
            .filter(sf -> targetId.equals(sf.targetId()))
            .sorted(java.util.Comparator.comparingInt(SequenceFlow::sortOrder))
            .toList();
    }

    public record FlowElement(
        UUID id, String code, String type, String name,
        Map<String, Object> config, int sortOrder
    ) {
        /**
         * Multi-instance marker: la activity se ejecuta N veces (una por item
         * de una coleccion de runtime). Convencion: config.multiInstance =
         * { collection, elementVar, mode, outputCollection? }.
         */
        public boolean hasMultiInstance() {
            return config != null && config.get("multiInstance") instanceof Map;
        }

        @SuppressWarnings("unchecked")
        public Map<String, Object> multiInstance() {
            return config == null ? null : (Map<String, Object>) config.get("multiInstance");
        }
    }

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
