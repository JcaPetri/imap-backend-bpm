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
//  • [controller] DTOs, nunca exponer entidades del domain en la API
//  • [bpm] Orquestador central (tipo Temporal/Step Functions)
//  • [bpm] processdef→processversion: snapshot inmutable al activar
//  • [bpm] Las defs de proceso viven acá (no en system)
//  • [bpm] Camunda 8 como norte; interim form-driven en prod
// ─── GOLDEN-RULES:END ───

package com.imap.bpm.infrastructure;

import com.imap.bpm.domain.engine.DecisionDefinition;
import com.imap.bpm.domain.engine.DecisionDefinitionLoader;
import com.imap.bpm.domain.engine.LocalDefinitionReader;
import com.imap.bpm.domain.engine.ProcessDefinition;
import com.imap.bpm.domain.engine.ProcessDefinitionLoader;
import com.imap.bpm.infrastructure.entity.Decisiondef;
import com.imap.bpm.infrastructure.entity.Processversion;
import com.imap.bpm.infrastructure.repository.DecisiondefRepository;
import com.imap.bpm.infrastructure.repository.ProcessversionRepository;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import java.util.*;

/**
 * F5 — diagnóstico de cutover: compara la ProcessDefinition/DecisionDefinition
 * que produce el path LOCAL (tablas relacionales bpm) vs el path SYSTEM (HTTP),
 * para TODOS los defs. Si dan estructuralmente idénticos, flipear
 * bpm.defs.source=local es seguro (el engine recibe el mismo input).
 *
 * Read-only. Usa bypass_rls para ver defs de todos los tenants. Tolera
 * entityDefCode (null en local, cross-service).
 */
@RestController
@RequestMapping("/v1/bpm/admin")
public class BpmDefCompareController {

    private final ProcessversionRepository processversionRepo;
    private final DecisiondefRepository decisiondefRepo;
    private final LocalDefinitionReader localReader;
    private final ProcessDefinitionLoader pdLoader;
    private final DecisionDefinitionLoader ddLoader;

    @PersistenceContext
    private EntityManager em;

    public BpmDefCompareController(ProcessversionRepository processversionRepo,
                                   DecisiondefRepository decisiondefRepo,
                                   LocalDefinitionReader localReader,
                                   ProcessDefinitionLoader pdLoader,
                                   DecisionDefinitionLoader ddLoader) {
        this.processversionRepo = processversionRepo;
        this.decisiondefRepo = decisiondefRepo;
        this.localReader = localReader;
        this.pdLoader = pdLoader;
        this.ddLoader = ddLoader;
    }

    @GetMapping("/def-compare")
    @Transactional
    public Map<String, Object> compare() {
        em.createNativeQuery("SET LOCAL app.bypass_rls = 'true'").executeUpdate();

        // ── processversions ──
        List<Map<String, Object>> pvMismatches = new ArrayList<>();
        int pvMatched = 0;
        List<Processversion> pvs = processversionRepo.findAll();
        for (Processversion pv : pvs) {
            List<String> diffs = new ArrayList<>();
            try {
                ProcessDefinition local = localReader.loadProcessDefinition(pv.getId());
                ProcessDefinition sys = pdLoader.fetchFromSystem(pv.getId(), null, pv.getTenantId());
                diffProcessDef(local, sys, diffs);
            } catch (Exception e) {
                diffs.add("EXCEPTION: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            }
            if (diffs.isEmpty()) pvMatched++;
            else pvMismatches.add(Map.of("processversionId", pv.getId().toString(), "diffs", diffs));
        }

        // ── decisiondefs ──
        List<Map<String, Object>> ddMismatches = new ArrayList<>();
        int ddMatched = 0;
        List<Decisiondef> dds = decisiondefRepo.findAll();
        for (Decisiondef dd : dds) {
            List<String> diffs = new ArrayList<>();
            try {
                DecisionDefinition local = localReader.loadDecisionDefinition(dd.getTenantId(), dd.getCode());
                DecisionDefinition sys = ddLoader.fetchFromSystem(dd.getCode(), null, dd.getTenantId());
                diffDecisionDef(local, sys, diffs);
            } catch (Exception e) {
                diffs.add("EXCEPTION: " + e.getClass().getSimpleName() + ": " + e.getMessage());
            }
            if (diffs.isEmpty()) ddMatched++;
            else ddMismatches.add(Map.of("code", dd.getCode(), "diffs", diffs));
        }

        Map<String, Object> out = new LinkedHashMap<>();
        out.put("processversions", Map.of("total", pvs.size(), "matched", pvMatched, "mismatches", pvMismatches));
        out.put("decisiondefs", Map.of("total", dds.size(), "matched", ddMatched, "mismatches", ddMismatches));
        return out;
    }

    private void diffProcessDef(ProcessDefinition l, ProcessDefinition s, List<String> diffs) {
        if (l == null || s == null) { diffs.add("null def local=" + (l == null) + " sys=" + (s == null)); return; }
        eq(diffs, "processdefCode", l.processdefCode(), s.processdefCode());
        eq(diffs, "version", l.version(), s.version());
        // flowElements por code
        Map<String, ProcessDefinition.FlowElement> lfe = byKey(l.flowElements(), ProcessDefinition.FlowElement::code);
        Map<String, ProcessDefinition.FlowElement> sfe = byKey(s.flowElements(), ProcessDefinition.FlowElement::code);
        eq(diffs, "flowElement codes", lfe.keySet(), sfe.keySet());
        for (String code : lfe.keySet()) {
            ProcessDefinition.FlowElement a = lfe.get(code), b = sfe.get(code);
            if (b == null) continue;
            eq(diffs, "fe[" + code + "].type", a.type(), b.type());
            eq(diffs, "fe[" + code + "].sortOrder", a.sortOrder(), b.sortOrder());
            eq(diffs, "fe[" + code + "].config", a.config(), b.config());
        }
        // sequenceFlows por sourceCode→targetCode
        Map<String, ProcessDefinition.SequenceFlow> lsf = byKey(l.sequenceFlows(), sf -> sf.sourceCode() + "->" + sf.targetCode());
        Map<String, ProcessDefinition.SequenceFlow> ssf = byKey(s.sequenceFlows(), sf -> sf.sourceCode() + "->" + sf.targetCode());
        eq(diffs, "sequenceFlow edges", lsf.keySet(), ssf.keySet());
        for (String k : lsf.keySet()) {
            ProcessDefinition.SequenceFlow a = lsf.get(k), b = ssf.get(k);
            if (b == null) continue;
            eq(diffs, "sf[" + k + "].conditionExpr", a.conditionExpr(), b.conditionExpr());
            eq(diffs, "sf[" + k + "].sortOrder", a.sortOrder(), b.sortOrder());
        }
        // taskForms por flowElementCode (ignora entityDefCode — cross-service null local)
        Map<String, ProcessDefinition.TaskForm> ltf = byKey(l.taskForms(), ProcessDefinition.TaskForm::flowElementCode);
        Map<String, ProcessDefinition.TaskForm> stf = byKey(s.taskForms(), ProcessDefinition.TaskForm::flowElementCode);
        eq(diffs, "taskForm codes", ltf.keySet(), stf.keySet());
        for (String k : ltf.keySet()) {
            ProcessDefinition.TaskForm a = ltf.get(k), b = stf.get(k);
            if (b == null) continue;
            eq(diffs, "tf[" + k + "].mode", a.mode(), b.mode());
            eq(diffs, "tf[" + k + "].entityDefId", a.entityDefId(), b.entityDefId());
        }
    }

    private void diffDecisionDef(DecisionDefinition l, DecisionDefinition s, List<String> diffs) {
        if (l == null || s == null) { diffs.add("null def local=" + (l == null) + " sys=" + (s == null)); return; }
        eq(diffs, "code", l.code(), s.code());
        eq(diffs, "hitPolicy", l.hitPolicy(), s.hitPolicy());
        eq(diffs, "inputSchema", l.inputSchema(), s.inputSchema());
        eq(diffs, "outputSchema", l.outputSchema(), s.outputSchema());
        eq(diffs, "rules.size", l.rules().size(), s.rules().size());
        Map<Integer, DecisionDefinition.Rule> lr = byKey(l.rules(), DecisionDefinition.Rule::priority);
        Map<Integer, DecisionDefinition.Rule> sr = byKey(s.rules(), DecisionDefinition.Rule::priority);
        for (Integer p : lr.keySet()) {
            DecisionDefinition.Rule a = lr.get(p), b = sr.get(p);
            if (b == null) { diffs.add("rule priority " + p + " missing in sys"); continue; }
            eq(diffs, "rule[" + p + "].inputs", a.inputs(), b.inputs());
            eq(diffs, "rule[" + p + "].outputs", a.outputs(), b.outputs());
        }
    }

    private static <T, K> Map<K, T> byKey(List<T> list, java.util.function.Function<T, K> keyFn) {
        Map<K, T> m = new LinkedHashMap<>();
        for (T t : list) m.put(keyFn.apply(t), t);
        return m;
    }

    private static void eq(List<String> diffs, String field, Object a, Object b) {
        if (!Objects.equals(a, b)) diffs.add(field + ": local=" + a + " | sys=" + b);
    }
}
