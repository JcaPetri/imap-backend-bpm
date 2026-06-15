package com.imap.bpm.domain.workhub;

import com.imap.bpm.infrastructure.entity.WhbClassification;
import com.imap.bpm.infrastructure.entity.WhbTenantConfig;
import com.imap.bpm.infrastructure.repository.WhbClassificationRepository;
import com.imap.bpm.infrastructure.repository.WhbTenantConfigRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Lógica de la pantalla admin del WorkHub (la carga el admin del tenant): política
 * de priorización por tenant + clasificación G/U/T por processdef/user_task.
 * Ver docs/architecture/workhub-northstar.md §5.4. Upsert idempotente sobre la
 * fila "current" (processversion_id NULL).
 */
@Service
public class WorkHubConfigService {

    /** Mismo estado activo que usa el resto de bpm (ProcessDefinitionLoader). */
    static final UUID DEFAULT_STATE_ACTIVE = UUID.fromString("019dc6f0-aa83-7333-8888-000000000001");

    private final WhbTenantConfigRepository configRepo;
    private final WhbClassificationRepository classRepo;

    public WorkHubConfigService(WhbTenantConfigRepository configRepo,
                                WhbClassificationRepository classRepo) {
        this.configRepo = configRepo;
        this.classRepo = classRepo;
    }

    public WhbTenantConfig getConfig(UUID tenantId) {
        return configRepo.findByTenantId(tenantId).orElse(null);
    }

    @Transactional
    public WhbTenantConfig upsertConfig(UUID tenantId, UUID userId, String mode,
                                        BigDecimal wg, BigDecimal wu, BigDecimal wt,
                                        BigDecimal highPct, BigDecimal mediumPct) {
        OffsetDateTime now = OffsetDateTime.now();
        WhbTenantConfig c = configRepo.findByTenantId(tenantId).orElseGet(() -> {
            WhbTenantConfig n = new WhbTenantConfig();
            n.setId(UUID.randomUUID());
            n.setTenantId(tenantId);
            n.setStateId(DEFAULT_STATE_ACTIVE);
            n.setCreatedAt(now);
            n.setCreatedById(userId);
            return n;
        });
        c.setAggregationMode(mode);
        c.setWeightGravity(wg);
        c.setWeightUrgency(wu);
        c.setWeightTrend(wt);
        c.setThresholdHighPct(highPct);
        c.setThresholdMediumPct(mediumPct);
        c.setUpdatedAt(now);
        c.setUpdatedById(userId);
        return configRepo.save(c);
    }

    public List<WhbClassification> listClassifications(UUID tenantId) {
        return classRepo.findByTenantIdAndProcessversionIdIsNull(tenantId);
    }

    @Transactional
    public WhbClassification upsertClassification(UUID tenantId, UUID userId,
                                                  UUID processdefId, UUID flowelementId,
                                                  short gravity, short urgency, short trend) {
        OffsetDateTime now = OffsetDateTime.now();
        Optional<WhbClassification> existing = (flowelementId != null)
            ? classRepo.findByTenantIdAndProcessdefIdAndFlowelementIdAndProcessversionIdIsNull(
                tenantId, processdefId, flowelementId)
            : classRepo.findByTenantIdAndProcessdefIdAndFlowelementIdIsNullAndProcessversionIdIsNull(
                tenantId, processdefId);

        WhbClassification c = existing.orElseGet(() -> {
            WhbClassification n = new WhbClassification();
            n.setId(UUID.randomUUID());
            n.setTenantId(tenantId);
            n.setProcessdefId(processdefId);
            n.setFlowelementId(flowelementId);   // NULL = nivel proceso
            n.setStateId(DEFAULT_STATE_ACTIVE);
            n.setCreatedAt(now);
            n.setCreatedById(userId);
            return n;
        });
        c.setGravity(gravity);
        c.setUrgency(urgency);
        c.setTrend(trend);
        c.setUpdatedAt(now);
        c.setUpdatedById(userId);
        return classRepo.save(c);
    }
}
