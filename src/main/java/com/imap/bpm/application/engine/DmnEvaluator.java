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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * Evaluador DMN — recorre las rules de una `DecisionDefinition` aplicando
 * los operadores declarativos definidos en cada `RuleInput` contra el mapa
 * de variables del processinstance, y aplica el hit policy.
 *
 * Hit policies soportadas:
 *   - unique        — exactamente 1 match; si ≥2 → error
 *   - first         — primer match por orden de priority asc
 *   - priority      — match con mayor `priority` numérico gana
 *   - any           — todos los matched deben dar el MISMO output; si discrepan → error
 *   - collect       — outputs combinados como List<value> por var (orden no preservado)
 *   - rule-order    — outputs combinados como List<value> en orden de las rules (priority asc)
 *   - output-order  — outputs combinados como List<value> ordenado por value asc
 *
 * Operadores: any | eq | ne | lt | lte | gt | gte | in | between
 *
 * Diferido: FEEL real, decision tables anidadas (DRD).
 */
@Service
public class DmnEvaluator {

    private static final Logger log = LoggerFactory.getLogger(DmnEvaluator.class);

    /**
     * Result del evaluate.
     *
     * - `matchedRule`: la rule "ganadora" en políticas single (unique/first/
     *    priority/any). Null si no hubo match O si la política es multi
     *    (collect/rule-order/output-order).
     * - `outputs`: map varName→value. Para single: value es escalar. Para
     *    multi: value es List<Object>.
     * - `totalMatched`: cuántas rules matchearon en total.
     * - `allMatched`: las rules matched (para auditoría).
     */
    public record EvaluationResult(
        DecisionDefinition.Rule matchedRule,
        Map<String, Object> outputs,
        int totalMatched,
        List<DecisionDefinition.Rule> allMatched
    ) {}

    /**
     * Evalúa la decision contra el mapa de inputs (vars del processinstance).
     * Lanza IllegalStateException si:
     *   - hit_policy=unique y matchearon ≥2 rules
     *   - hit_policy=any y los outputs de las matched no son idénticos
     */
    public EvaluationResult evaluate(DecisionDefinition def, Map<String, Object> inputs) {
        List<DecisionDefinition.Rule> matched = new ArrayList<>();
        for (DecisionDefinition.Rule rule : def.rules()) {
            if (ruleMatches(rule, inputs)) {
                matched.add(rule);
            }
        }

        String policy = def.hitPolicy() == null ? "first" : def.hitPolicy().toLowerCase();

        if (matched.isEmpty()) {
            return new EvaluationResult(null, new LinkedHashMap<>(), 0, List.of());
        }

        switch (policy) {
            case "unique" -> {
                if (matched.size() > 1) {
                    throw new IllegalStateException("DMN unique hit policy violated for decision '"
                        + def.code() + "': " + matched.size() + " rules matched (priorities="
                        + matched.stream().map(r -> String.valueOf(r.priority())).toList() + ")");
                }
                return singleResult(matched.get(0), matched);
            }
            case "first" -> {
                // rules ya ordenadas por priority asc en el loader
                return singleResult(matched.get(0), matched);
            }
            case "priority" -> {
                // Mayor priority numérico gana. Si ties, el último insertado.
                DecisionDefinition.Rule winner = matched.stream()
                    .max(Comparator.comparingInt(DecisionDefinition.Rule::priority))
                    .orElse(matched.get(0));
                return singleResult(winner, matched);
            }
            case "any" -> {
                // Todos los matched deben dar EL MISMO output. Si discrepan → error.
                Map<String, Object> first = ruleOutputsToMap(matched.get(0));
                for (int i = 1; i < matched.size(); i++) {
                    Map<String, Object> other = ruleOutputsToMap(matched.get(i));
                    if (!first.equals(other)) {
                        throw new IllegalStateException("DMN 'any' hit policy violated for decision '"
                            + def.code() + "': rules priority=" + matched.get(0).priority()
                            + " vs priority=" + matched.get(i).priority()
                            + " disagree on output (any requires all matches return same output).");
                    }
                }
                return singleResult(matched.get(0), matched);
            }
            case "collect" -> {
                // Outputs combinados como List<value>. Orden no preservado pero
                // implementación práctica usa orden de matched.
                return multiResult(matched, false);
            }
            case "rule-order", "rule_order", "ruleorder" -> {
                // Como collect pero preservando explícitamente orden de las rules.
                // (matched ya viene en orden de rules; lo dejamos igual.)
                return multiResult(matched, false);
            }
            case "output-order", "output_order", "outputorder" -> {
                // Como collect pero list ordenada por value asc (per output).
                return multiResult(matched, true);
            }
            default -> {
                log.warn("Unsupported hit policy '{}' for decision '{}' — falling back to 'first'",
                    policy, def.code());
                return singleResult(matched.get(0), matched);
            }
        }
    }

    private EvaluationResult singleResult(DecisionDefinition.Rule winner,
                                          List<DecisionDefinition.Rule> matched) {
        Map<String, Object> outputs = ruleOutputsToMap(winner);
        return new EvaluationResult(winner, outputs, matched.size(), matched);
    }

    /**
     * Multi-output: combina outputs de todas las rules matched en List<value>
     * por var. Si `sortByValue=true`, ordena cada lista ascendente.
     */
    @SuppressWarnings({"unchecked","rawtypes"})
    private EvaluationResult multiResult(List<DecisionDefinition.Rule> matched, boolean sortByValue) {
        Map<String, List<Object>> agg = new LinkedHashMap<>();
        for (DecisionDefinition.Rule rule : matched) {
            for (DecisionDefinition.RuleOutput out : rule.outputs()) {
                agg.computeIfAbsent(out.varName(), k -> new ArrayList<>()).add(out.value());
            }
        }
        if (sortByValue) {
            for (List<Object> list : agg.values()) {
                list.sort((a, b) -> {
                    if (a instanceof Comparable && b instanceof Comparable && a.getClass() == b.getClass()) {
                        return ((Comparable) a).compareTo(b);
                    }
                    return String.valueOf(a).compareTo(String.valueOf(b));
                });
            }
        }
        Map<String, Object> outputs = new LinkedHashMap<>();
        for (Map.Entry<String, List<Object>> e : agg.entrySet()) outputs.put(e.getKey(), e.getValue());
        return new EvaluationResult(null, outputs, matched.size(), matched);
    }

    private Map<String, Object> ruleOutputsToMap(DecisionDefinition.Rule rule) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (DecisionDefinition.RuleOutput o : rule.outputs()) out.put(o.varName(), o.value());
        return out;
    }

    /** Una rule matchea si TODOS sus inputs matchean. */
    private boolean ruleMatches(DecisionDefinition.Rule rule, Map<String, Object> inputs) {
        for (DecisionDefinition.RuleInput input : rule.inputs()) {
            if (!operatorMatches(inputs.get(input.varName()), input.operator(), input.value())) {
                return false;
            }
        }
        return true;
    }

    /** Aplica el operador. Si el operador no es reconocido, devuelve false + warn. */
    @SuppressWarnings("unchecked")
    private boolean operatorMatches(Object actualValue, String operator, Object ruleValue) {
        if (operator == null) return false;
        String op = operator.toLowerCase();

        switch (op) {
            case "any":
            case "*":
                return true;
            case "eq":
                return String.valueOf(actualValue).equals(String.valueOf(ruleValue));
            case "ne":
                return !String.valueOf(actualValue).equals(String.valueOf(ruleValue));
            case "lt":  return compareNumeric(actualValue, ruleValue, (a, b) -> a < b);
            case "lte": return compareNumeric(actualValue, ruleValue, (a, b) -> a <= b);
            case "gt":  return compareNumeric(actualValue, ruleValue, (a, b) -> a > b);
            case "gte": return compareNumeric(actualValue, ruleValue, (a, b) -> a >= b);
            case "in":
                if (ruleValue instanceof List<?> list) {
                    String actualStr = String.valueOf(actualValue);
                    for (Object v : list) {
                        if (actualStr.equals(String.valueOf(v))) return true;
                    }
                }
                return false;
            case "between":
                if (ruleValue instanceof List<?> list && list.size() == 2) {
                    Double a = toDouble(actualValue);
                    Double min = toDouble(list.get(0));
                    Double max = toDouble(list.get(1));
                    if (a == null || min == null || max == null) return false;
                    return a >= min && a <= max;
                }
                return false;
            default:
                log.warn("DmnEvaluator: unknown operator '{}'", operator);
                return false;
        }
    }

    @FunctionalInterface
    private interface DoubleBiPredicate { boolean test(double a, double b); }

    private boolean compareNumeric(Object actual, Object rule, DoubleBiPredicate cmp) {
        Double a = toDouble(actual);
        Double r = toDouble(rule);
        if (a == null || r == null) return false;
        return cmp.test(a, r);
    }

    private Double toDouble(Object o) {
        if (o == null) return null;
        if (o instanceof Number n) return n.doubleValue();
        try {
            return Double.parseDouble(String.valueOf(o));
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
