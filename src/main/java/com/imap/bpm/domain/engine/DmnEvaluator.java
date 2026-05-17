package com.imap.bpm.domain.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * Evaluador DMN — recorre las rules de una `DecisionDefinition` aplicando
 * los operadores declarativos definidos en cada `RuleInput` contra el mapa
 * de variables del processinstance, y aplica el hit policy.
 *
 * MVP B3:
 *   - Operadores: any | eq | ne | lt | lte | gt | gte | in | between
 *   - Hit policies: unique | first
 *
 * Diferido para iter futura: priority/collect/rule-order/any, FEEL real,
 * decision tables anidadas (DRD).
 *
 * Convención de tipos: comparaciones numéricas convierten Object→Double con
 * tolerancia. Si la conversión falla, el operador retorna false (no rompe).
 * Igualdad usa String.valueOf() para flexibilidad cross-type.
 */
@Service
public class DmnEvaluator {

    private static final Logger log = LoggerFactory.getLogger(DmnEvaluator.class);

    public record EvaluationResult(
        DecisionDefinition.Rule matchedRule,
        Map<String, Object> outputs,
        int totalMatched
    ) {}

    /**
     * Evalúa la decision contra el mapa de inputs (vars del processinstance).
     * Devuelve EvaluationResult con la rule que ganó (según hit_policy) y
     * el output map. Si no hay matches, devuelve EvaluationResult vacío.
     *
     * Lanza IllegalStateException si hit_policy=unique y matchearon ≥2 rules.
     */
    public EvaluationResult evaluate(DecisionDefinition def, Map<String, Object> inputs) {
        List<DecisionDefinition.Rule> matched = new ArrayList<>();
        for (DecisionDefinition.Rule rule : def.rules()) {
            if (ruleMatches(rule, inputs)) {
                matched.add(rule);
            }
        }

        String policy = def.hitPolicy() == null ? "first" : def.hitPolicy().toLowerCase();
        DecisionDefinition.Rule winner;

        switch (policy) {
            case "unique" -> {
                if (matched.size() > 1) {
                    throw new IllegalStateException("DMN unique hit policy violated for decision '"
                        + def.code() + "': " + matched.size() + " rules matched (rules priorities="
                        + matched.stream().map(r -> String.valueOf(r.priority())).toList() + ")");
                }
                winner = matched.isEmpty() ? null : matched.get(0);
            }
            case "first" -> {
                // rules vienen ordenadas por priority en el loader
                winner = matched.isEmpty() ? null : matched.get(0);
            }
            default -> {
                log.warn("Unsupported hit policy '{}' for decision '{}' — falling back to 'first'",
                    policy, def.code());
                winner = matched.isEmpty() ? null : matched.get(0);
            }
        }

        Map<String, Object> outputs = new LinkedHashMap<>();
        if (winner != null) {
            for (DecisionDefinition.RuleOutput out : winner.outputs()) {
                outputs.put(out.varName(), out.value());
            }
        }
        return new EvaluationResult(winner, outputs, matched.size());
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
