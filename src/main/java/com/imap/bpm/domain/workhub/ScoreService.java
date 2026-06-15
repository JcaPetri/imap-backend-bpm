package com.imap.bpm.domain.workhub;

import com.imap.bpm.infrastructure.entity.WhbClassification;
import com.imap.bpm.infrastructure.entity.WhbTenantConfig;
import com.imap.bpm.infrastructure.repository.WhbClassificationRepository;
import com.imap.bpm.infrastructure.repository.WhbTenantConfigRepository;
import org.springframework.stereotype.Service;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;

/**
 * Calcula la prioridad (score normalizado a %) y el color de semáforo de una
 * tarea para la bandeja del WorkHub.
 *
 * Diseño cerrado en docs/architecture/workhub-northstar.md §5:
 *   score_base  = SUM:     wg·G + wu·U + wt·T   |  PRODUCT: G·U·T
 *   prioridad%  = score_base / score_max · 100  (SUM 30 y PRODUCT 1000 → 100%)
 *   semáforo    = umbrales fijos sobre prioridad% (en %), combinados con SLA:
 *                 vencida → ROJO ; vence pronto → al menos AMARILLO.
 *
 * El modo, los pesos y los umbrales viven a nivel TENANT (bpm_whb_tenantconfig_tbl).
 * Si el tenant no tiene config, se usan los defaults (SUM, pesos=1, 80/50).
 * La presión de SLA dinámica fina y el override por DMN (forzar a MAX) se
 * resuelven en runtime aparte; acá se aplica la regla de SLA por due_at.
 */
@Service
public class ScoreService {

    static final String MODE_SUM     = "SUM";
    static final String MODE_PRODUCT = "PRODUCT";

    // Defaults cuando el tenant no tiene fila de config.
    static final double DEFAULT_WEIGHT   = 1.0;
    static final double DEFAULT_HIGH_PCT  = 80.0;
    static final double DEFAULT_MEDIUM_PCT = 50.0;

    /** Escala máxima de cada eje (CHECK 1..10 en la tabla). */
    static final double AXIS_MAX = 10.0;

    /** Ventana "vence pronto" (hoy/próxima) → al menos AMARILLO. */
    static final long DUE_SOON_HOURS = 24;

    private final WhbTenantConfigRepository configRepo;
    private final WhbClassificationRepository classRepo;

    public ScoreService(WhbTenantConfigRepository configRepo,
                        WhbClassificationRepository classRepo) {
        this.configRepo = configRepo;
        this.classRepo = classRepo;
    }

    /** Conveniencia para callers de runtime (usa el reloj UTC actual). */
    public TaskPriority compute(UUID tenantId, UUID processdefId, UUID flowelementId,
                                OffsetDateTime dueAt) {
        return computeAt(tenantId, processdefId, flowelementId, dueAt,
                         OffsetDateTime.now(ZoneOffset.UTC));
    }

    /**
     * Núcleo testeable: recibe `now` explícito.
     */
    public TaskPriority computeAt(UUID tenantId, UUID processdefId, UUID flowelementId,
                                  OffsetDateTime dueAt, OffsetDateTime now) {

        WhbTenantConfig cfg = configRepo.findByTenantId(tenantId).orElse(null);
        Optional<WhbClassification> cls = resolveClassification(tenantId, processdefId, flowelementId);

        double pct = 0.0;
        boolean classified = cls.isPresent();
        if (classified) {
            pct = prioridadPct(cls.get(), cfg);
        }

        SemaphoreColor color = colorFromPct(pct, cfg).max(slaColor(dueAt, now));
        return new TaskPriority(pct, color, classified);
    }

    // ── Resolución de clasificación: override por user_task, si no nivel processdef ──
    private Optional<WhbClassification> resolveClassification(UUID tenantId, UUID processdefId,
                                                              UUID flowelementId) {
        if (flowelementId != null) {
            Optional<WhbClassification> override = classRepo
                .findByTenantIdAndProcessdefIdAndFlowelementIdAndProcessversionIdIsNull(
                    tenantId, processdefId, flowelementId);
            if (override.isPresent()) return override;
        }
        return classRepo
            .findByTenantIdAndProcessdefIdAndFlowelementIdIsNullAndProcessversionIdIsNull(
                tenantId, processdefId);
    }

    // ── score_base / score_max · 100, según modo ──
    private double prioridadPct(WhbClassification c, WhbTenantConfig cfg) {
        double g = c.getGravity();
        double u = c.getUrgency();
        double t = c.getTrend();

        String mode = cfg != null ? cfg.getAggregationMode() : MODE_SUM;

        double base, max;
        if (MODE_PRODUCT.equals(mode)) {
            base = g * u * t;
            max  = AXIS_MAX * AXIS_MAX * AXIS_MAX;        // 1000
        } else { // SUM (default)
            double wg = weight(cfg != null ? doubleOrNull(cfg.getWeightGravity()) : null);
            double wu = weight(cfg != null ? doubleOrNull(cfg.getWeightUrgency()) : null);
            double wt = weight(cfg != null ? doubleOrNull(cfg.getWeightTrend())   : null);
            base = wg * g + wu * u + wt * t;
            max  = (wg + wu + wt) * AXIS_MAX;
        }
        if (max <= 0) return 0.0;
        double pct = base / max * 100.0;
        return clamp(pct, 0.0, 100.0);
    }

    private SemaphoreColor colorFromPct(double pct, WhbTenantConfig cfg) {
        double high   = cfg != null ? cfg.getThresholdHighPct().doubleValue()   : DEFAULT_HIGH_PCT;
        double medium = cfg != null ? cfg.getThresholdMediumPct().doubleValue() : DEFAULT_MEDIUM_PCT;
        if (pct >= high)   return SemaphoreColor.RED;
        if (pct >= medium) return SemaphoreColor.YELLOW;
        return SemaphoreColor.GREEN;
    }

    private SemaphoreColor slaColor(OffsetDateTime dueAt, OffsetDateTime now) {
        if (dueAt == null) return SemaphoreColor.GREEN;
        if (dueAt.isBefore(now)) return SemaphoreColor.RED;                       // vencida
        if (dueAt.isBefore(now.plusHours(DUE_SOON_HOURS))) return SemaphoreColor.YELLOW; // vence pronto
        return SemaphoreColor.GREEN;
    }

    private static double weight(Double w) { return w != null ? w : DEFAULT_WEIGHT; }
    private static Double doubleOrNull(java.math.BigDecimal b) { return b != null ? b.doubleValue() : null; }
    private static double clamp(double v, double lo, double hi) { return Math.max(lo, Math.min(hi, v)); }
}
