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
import java.util.Map;

/**
 * Payload del endpoint POST /v1/bpm/admin/processdef.
 *
 * Crea un processdef nuevo con su processversion v1 + flow_elements +
 * sequence_flows + (opcional) task_forms en una sola transacción atómica sobre
 * las tablas RELACIONALES de bpm (V015). Reemplaza al equivalente EAV de system
 * (Fase 4-mgmt de IMAP_BPM_OWNERSHIP_MIGRATION.md).
 *
 * Versionado v1-only en esta iter — re-publish del mismo code devuelve 409.
 * Para "editar" un processdef hay que crear uno nuevo con otro code.
 *
 * Validaciones cruzadas (server-side):
 *   - header.code único contra bpm_pro_processdef_tbl(tenant_id, code) existentes
 *   - cada flowElement.type es uno de los 10 soportados
 *   - sequenceFlows: sourceCode/targetCode existen en flowElements (mismo req)
 *   - taskForms.flowElementCode existe en flowElements y es de tipo user_task
 *   - taskForms.entityDefCode existe en system (resuelto por HTTP s2s)
 *   - conditionExpr básico (balance de paréntesis + longitud)
 *   - Topología: ≥1 start_event, ≥1 end_event, todos los elements alcanzables desde algún start
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record CreateProcessdefRequest(
    Header header,
    List<FlowElement> flowElements,
    List<SequenceFlow> sequenceFlows,
    List<TaskForm> taskForms,
    Boolean dryRun
) {

    public record Header(
        String code,
        String name,
        String description,
        String lifecycle,       // 'active' | 'draft' | 'inactive'
        String startPermission  // WorkHub: permiso IAM requerido para iniciar (opcional; vacío → convención bpm.start.<code>)
    ) {}

    /**
     * Flow element del processdef. El `config` es Map JSON con shape variable
     * según el `type`:
     *   - start_event, end_event: {} (vacío)
     *   - service_task: { "serviceCode": "string" }
     *   - user_task: { "assignment"?: {...} } (entity binding va en taskForms)
     *   - business_rule_task: { "decisionRef": "code" }
     *   - exclusive_gateway, parallel_gateway: {}
     *   - sub_process: { "calledProcessversionId": "uuid", "passVariables": [...],
     *                    "returnVariables": [...], "waitForCompletion": bool }
     *   - intermediate_event: { "subtype": "timer|message|signal", ...subconfig }
     *   - boundary_event: { "attachedToCode": "elementCode", "subtype": "timer|error|escalation",
     *                       "interrupting": bool, ...subconfig }
     */
    public record FlowElement(
        String code,
        String name,
        String type,           // start_event, end_event, user_task, etc.
        Integer sortOrder,
        Map<String, Object> config
    ) {}

    public record SequenceFlow(
        String sourceCode,
        String targetCode,
        String conditionExpr,    // JEXL, opcional (usualmente para gateways)
        Integer sortOrder
    ) {}

    public record TaskForm(
        String flowElementCode,   // debe ser un user_task del flowElements
        String entityDefCode,     // entity virtual (ej: bpm_hum_demoform)
        String mode               // 'edit' | 'readonly'
    ) {}

    /** Tipos de flow_element soportados por el motor BPM. */
    public static final List<String> SUPPORTED_TYPES = List.of(
        "start_event",
        "end_event",
        "service_task",
        "user_task",
        "business_rule_task",
        "exclusive_gateway",
        "parallel_gateway",
        "sub_process",
        "intermediate_event",
        "boundary_event"
    );
}
