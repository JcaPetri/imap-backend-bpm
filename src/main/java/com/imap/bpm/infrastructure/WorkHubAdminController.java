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

import com.imap.bpm.application.workhub.WorkHubConfigService;
import com.imap.bpm.infrastructure.entity.WhbClassification;
import com.imap.bpm.infrastructure.entity.WhbTenantConfig;
import com.imap.eav.engine.context.EavTenantSession;
import com.imap.platform.security.UserContext;
import com.imap.platform.security.UserContextHolder;
import com.imap.platform.tenant.TenantContextHolder;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.util.*;

/**
 * Pantalla admin del WorkHub (la usa el admin del tenant): política de
 * priorización por tenant + clasificación G/U/T por processdef/user_task.
 * Ver docs/architecture/workhub-northstar.md §5.4.
 *
 *   GET  /v1/bpm/workhub/config            → política del tenant (o defaults)
 *   PUT  /v1/bpm/workhub/config            → upsert política
 *   GET  /v1/bpm/workhub/classifications   → lista clasificaciones del tenant
 *   PUT  /v1/bpm/workhub/classifications   → upsert una clasificación
 */
@RestController
@RequestMapping("/v1/bpm/workhub")
public class WorkHubAdminController {

    private final WorkHubConfigService svc;
    private final EavTenantSession tenantSession;

    public WorkHubAdminController(WorkHubConfigService svc, EavTenantSession tenantSession) {
        this.svc = svc;
        this.tenantSession = tenantSession;
    }

    // ─── Config por tenant ──────────────────────────────────────────────────

    @GetMapping("/config")
    @Transactional(readOnly = true)
    public Map<String, Object> getConfig() {
        tenantSession.applyToCurrentTransaction();
        WhbTenantConfig c = svc.getConfig(TenantContextHolder.get());
        return c == null ? defaultConfigMap() : toConfigMap(c);
    }

    @PutMapping("/config")
    @Transactional
    public ResponseEntity<?> putConfig(@RequestBody Map<String, Object> body) {
        tenantSession.applyToCurrentTransaction();
        UUID tenantId = TenantContextHolder.get();
        UUID userId = currentUserId();

        String mode;
        BigDecimal wg, wu, wt, high, medium;
        try {
            mode = str(body.getOrDefault("aggregationMode", "SUM")).toUpperCase();
            wg = bd(body.getOrDefault("weightGravity", "1"));
            wu = bd(body.getOrDefault("weightUrgency", "1"));
            wt = bd(body.getOrDefault("weightTrend", "1"));
            high = bd(body.getOrDefault("thresholdHighPct", "80"));
            medium = bd(body.getOrDefault("thresholdMediumPct", "50"));
        } catch (RuntimeException e) {
            return badRequest("invalid_number", "Valor numérico inválido: " + e.getMessage());
        }

        if (!mode.equals("SUM") && !mode.equals("PRODUCT")) {
            return badRequest("invalid_mode", "aggregationMode debe ser SUM o PRODUCT.");
        }
        if (sign(wg) < 0 || sign(wu) < 0 || sign(wt) < 0) {
            return badRequest("invalid_weights", "Los pesos no pueden ser negativos.");
        }
        if (outOfPct(high) || outOfPct(medium)) {
            return badRequest("invalid_thresholds", "Los umbrales deben estar entre 0 y 100.");
        }
        if (high.compareTo(medium) < 0) {
            return badRequest("invalid_thresholds", "El umbral alto debe ser ≥ al medio.");
        }

        WhbTenantConfig c = svc.upsertConfig(tenantId, userId, mode, wg, wu, wt, high, medium);
        return ResponseEntity.ok(toConfigMap(c));
    }

    // ─── Clasificación G/U/T ────────────────────────────────────────────────

    @GetMapping("/classifications")
    @Transactional(readOnly = true)
    public List<Map<String, Object>> listClassifications() {
        tenantSession.applyToCurrentTransaction();
        List<Map<String, Object>> out = new ArrayList<>();
        for (WhbClassification c : svc.listClassifications(TenantContextHolder.get())) {
            out.add(toClassMap(c));
        }
        return out;
    }

    @PutMapping("/classifications")
    @Transactional
    public ResponseEntity<?> putClassification(@RequestBody Map<String, Object> body) {
        tenantSession.applyToCurrentTransaction();
        UUID tenantId = TenantContextHolder.get();
        UUID userId = currentUserId();

        UUID processdefId, flowelementId;
        short gravity, urgency, trend;
        try {
            if (body.get("processdefId") == null) {
                return badRequest("missing_processdef", "processdefId es requerido.");
            }
            processdefId = UUID.fromString(str(body.get("processdefId")));
            flowelementId = body.get("flowelementId") == null
                ? null : UUID.fromString(str(body.get("flowelementId")));
            gravity = shortVal(body.get("gravity"));
            urgency = shortVal(body.get("urgency"));
            trend = shortVal(body.get("trend"));
        } catch (RuntimeException e) {
            return badRequest("invalid_input", "Entrada inválida: " + e.getMessage());
        }

        if (outOf1to10(gravity) || outOf1to10(urgency) || outOf1to10(trend)) {
            return badRequest("out_of_range", "Gravedad/Urgencia/Tendencia deben estar entre 1 y 10.");
        }

        WhbClassification c = svc.upsertClassification(
            tenantId, userId, processdefId, flowelementId, gravity, urgency, trend);
        return ResponseEntity.ok(toClassMap(c));
    }

    // ─── Helpers ────────────────────────────────────────────────────────────

    private static UUID currentUserId() {
        UserContext u = UserContextHolder.get();
        return u != null ? u.userId() : null;
    }

    private static Map<String, Object> toConfigMap(WhbTenantConfig c) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("tenantId", c.getTenantId() == null ? null : c.getTenantId().toString());
        m.put("aggregationMode", c.getAggregationMode());
        m.put("weightGravity", c.getWeightGravity());
        m.put("weightUrgency", c.getWeightUrgency());
        m.put("weightTrend", c.getWeightTrend());
        m.put("thresholdHighPct", c.getThresholdHighPct());
        m.put("thresholdMediumPct", c.getThresholdMediumPct());
        m.put("isDefault", false);
        return m;
    }

    private static Map<String, Object> defaultConfigMap() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("aggregationMode", "SUM");
        m.put("weightGravity", new BigDecimal("1"));
        m.put("weightUrgency", new BigDecimal("1"));
        m.put("weightTrend", new BigDecimal("1"));
        m.put("thresholdHighPct", new BigDecimal("80"));
        m.put("thresholdMediumPct", new BigDecimal("50"));
        m.put("isDefault", true);
        return m;
    }

    private static Map<String, Object> toClassMap(WhbClassification c) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("id", c.getId() == null ? null : c.getId().toString());
        m.put("processdefId", c.getProcessdefId() == null ? null : c.getProcessdefId().toString());
        m.put("flowelementId", c.getFlowelementId() == null ? null : c.getFlowelementId().toString());
        m.put("gravity", c.getGravity());
        m.put("urgency", c.getUrgency());
        m.put("trend", c.getTrend());
        return m;
    }

    private static ResponseEntity<Map<String, Object>> badRequest(String error, String message) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("error", error);
        m.put("message", message);
        return ResponseEntity.badRequest().body(m);
    }

    private static String str(Object o) { return o == null ? null : o.toString().trim(); }
    private static BigDecimal bd(Object o) { return new BigDecimal(o.toString().trim()); }
    private static short shortVal(Object o) { return (short) Math.round(Double.parseDouble(o.toString().trim())); }
    private static int sign(BigDecimal b) { return b.signum(); }
    private static boolean outOfPct(BigDecimal b) { return b.signum() < 0 || b.compareTo(new BigDecimal("100")) > 0; }
    private static boolean outOf1to10(short v) { return v < 1 || v > 10; }
}
