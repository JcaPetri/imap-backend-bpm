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

package com.imap.bpm.domain.engine;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.imap.bpm.infrastructure.entity.*;
import com.imap.bpm.infrastructure.repository.*;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Arma los records de dominio {@link ProcessDefinition} / {@link DecisionDefinition}
 * (la SHAPE que consume el ProcessEngine) leyendo las tablas RELACIONALES locales
 * de bpm (V015), en vez del HTTP a system.
 *
 * Fase 4 de IMAP_BPM_OWNERSHIP_MIGRATION.md. En F5 los loaders
 * (ProcessDefinitionLoader / DecisionDefinitionLoader) delegan acá en vez de
 * hacer WebClient a system. El engine NO cambia (misma shape).
 *
 * DORMANT hasta F5: por ahora es un @Component disponible pero no cableado a los
 * loaders → cero cambio de comportamiento.
 */
@Component
public class LocalDefinitionReader {

    private final ProcessdefRepository processdefRepo;
    private final ProcessversionRepository processversionRepo;
    private final FlowelementRepository flowelementRepo;
    private final SequenceflowRepository sequenceflowRepo;
    private final TaskformRepository taskformRepo;
    private final DecisiondefRepository decisiondefRepo;
    private final DmnRuleRepository dmnRuleRepo;
    private final ObjectMapper mapper;

    public LocalDefinitionReader(ProcessdefRepository processdefRepo,
                                 ProcessversionRepository processversionRepo,
                                 FlowelementRepository flowelementRepo,
                                 SequenceflowRepository sequenceflowRepo,
                                 TaskformRepository taskformRepo,
                                 DecisiondefRepository decisiondefRepo,
                                 DmnRuleRepository dmnRuleRepo,
                                 ObjectMapper mapper) {
        this.processdefRepo = processdefRepo;
        this.processversionRepo = processversionRepo;
        this.flowelementRepo = flowelementRepo;
        this.sequenceflowRepo = sequenceflowRepo;
        this.taskformRepo = taskformRepo;
        this.decisiondefRepo = decisiondefRepo;
        this.dmnRuleRepo = dmnRuleRepo;
        this.mapper = mapper;
    }

    /** Construye la ProcessDefinition de un processversion, o null si no existe. */
    public ProcessDefinition loadProcessDefinition(UUID processVersionId) {
        Processversion pv = processversionRepo.findById(processVersionId).orElse(null);
        if (pv == null) return null;
        Processdef pd = processdefRepo.findById(pv.getProcessdefId()).orElse(null);

        List<Flowelement> feEntities = flowelementRepo.findByProcessversionIdOrderBySortOrder(processVersionId);
        // mapa id → code para resolver source/target de los sequence flows
        Map<UUID, String> codeById = feEntities.stream()
            .collect(Collectors.toMap(Flowelement::getId, Flowelement::getElementCode, (a, b) -> a));

        List<ProcessDefinition.FlowElement> flowElements = new ArrayList<>();
        for (Flowelement fe : feEntities) {
            flowElements.add(new ProcessDefinition.FlowElement(
                fe.getId(), fe.getElementCode(), fe.getElementType(), fe.getName(),
                parseMap(fe.getConfig()), fe.getSortOrder() == null ? 0 : fe.getSortOrder()));
        }

        List<ProcessDefinition.SequenceFlow> sequenceFlows = new ArrayList<>();
        for (Sequenceflow sf : sequenceflowRepo.findByProcessversionIdOrderBySortOrder(processVersionId)) {
            sequenceFlows.add(new ProcessDefinition.SequenceFlow(
                sf.getId(), sf.getSourceId(), sf.getTargetId(),
                codeById.get(sf.getSourceId()), codeById.get(sf.getTargetId()),
                sf.getConditionExpr(), sf.getSortOrder() == null ? 0 : sf.getSortOrder()));
        }

        List<UUID> feIds = new ArrayList<>(codeById.keySet());
        List<ProcessDefinition.TaskForm> taskForms = new ArrayList<>();
        if (!feIds.isEmpty()) {
            for (Taskform tf : taskformRepo.findByFlowelementIdIn(feIds)) {
                // entityDefCode vive en system (cross-service) → null local; el engine lo tolera.
                taskForms.add(new ProcessDefinition.TaskForm(
                    tf.getFlowelementId(), codeById.get(tf.getFlowelementId()),
                    tf.getEntitydefId(), null, tf.getMode()));
            }
        }

        return new ProcessDefinition(
            pv.getId(),
            pv.getProcessdefId(),
            pd == null ? null : pd.getCode(),
            pd == null ? null : pd.getName(),
            pv.getVersion() == null ? 0 : pv.getVersion(),
            flowElements, sequenceFlows, taskForms);
    }

    /** Construye la DecisionDefinition por code dentro de un tenant, o null. */
    public DecisionDefinition loadDecisionDefinition(UUID tenantId, String code) {
        Decisiondef dd = decisiondefRepo.findByTenantIdAndCode(tenantId, code).orElse(null);
        if (dd == null) return null;

        List<DecisionDefinition.Rule> rules = new ArrayList<>();
        for (DmnRule r : dmnRuleRepo.findByDecisiondefIdOrderByPriorityAsc(dd.getId())) {
            rules.add(new DecisionDefinition.Rule(
                r.getId(), r.getPriority() == null ? 0 : r.getPriority(),
                parseRuleInputs(r.getInputs()), parseRuleOutputs(r.getOutputs()),
                r.getDescription()));
        }

        return new DecisionDefinition(
            dd.getId(), dd.getCode(), dd.getName(), dd.getDescription(), dd.getHitPolicy(),
            parseSchema(dd.getInputSchema()), parseSchema(dd.getOutputSchema()), rules);
    }

    // ── parsers JSON (defensivos: null/blank → vacío) ──────────────────────────

    @SuppressWarnings("unchecked")
    private Map<String, Object> parseMap(String json) {
        if (json == null || json.isBlank()) return Map.of();
        try {
            return mapper.readValue(json, new TypeReference<Map<String, Object>>() {});
        } catch (Exception e) {
            return Map.of();
        }
    }

    private List<Map<String, Object>> parseArray(String json) {
        if (json == null || json.isBlank()) return List.of();
        try {
            return mapper.readValue(json, new TypeReference<List<Map<String, Object>>>() {});
        } catch (Exception e) {
            return List.of();
        }
    }

    private List<DecisionDefinition.SchemaEntry> parseSchema(String json) {
        List<DecisionDefinition.SchemaEntry> out = new ArrayList<>();
        for (Map<String, Object> m : parseArray(json)) {
            out.add(new DecisionDefinition.SchemaEntry(str(m.get("var_name")), str(m.get("type"))));
        }
        return out;
    }

    private List<DecisionDefinition.RuleInput> parseRuleInputs(String json) {
        List<DecisionDefinition.RuleInput> out = new ArrayList<>();
        for (Map<String, Object> m : parseArray(json)) {
            out.add(new DecisionDefinition.RuleInput(str(m.get("var_name")), str(m.get("operator")), m.get("value")));
        }
        return out;
    }

    private List<DecisionDefinition.RuleOutput> parseRuleOutputs(String json) {
        List<DecisionDefinition.RuleOutput> out = new ArrayList<>();
        for (Map<String, Object> m : parseArray(json)) {
            out.add(new DecisionDefinition.RuleOutput(str(m.get("var_name")), m.get("value")));
        }
        return out;
    }

    private static String str(Object o) { return o == null ? null : o.toString(); }
}
