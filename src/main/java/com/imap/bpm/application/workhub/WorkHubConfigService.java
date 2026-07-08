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

package com.imap.bpm.application.workhub;

import com.imap.bpm.infrastructure.entity.WhbClassificationEntity;
import com.imap.bpm.infrastructure.entity.WhbTenantConfigEntity;
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

    public WhbTenantConfigEntity getConfig(UUID tenantId) {
        return configRepo.findByTenantId(tenantId).orElse(null);
    }

    @Transactional
    public WhbTenantConfigEntity upsertConfig(UUID tenantId, UUID userId, String mode,
                                        BigDecimal wg, BigDecimal wu, BigDecimal wt,
                                        BigDecimal highPct, BigDecimal mediumPct) {
        OffsetDateTime now = OffsetDateTime.now();
        WhbTenantConfigEntity c = configRepo.findByTenantId(tenantId).orElseGet(() -> {
            WhbTenantConfigEntity n = new WhbTenantConfigEntity();
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

    public List<WhbClassificationEntity> listClassifications(UUID tenantId) {
        return classRepo.findByTenantIdAndProcessversionIdIsNull(tenantId);
    }

    @Transactional
    public WhbClassificationEntity upsertClassification(UUID tenantId, UUID userId,
                                                  UUID processdefId, UUID flowelementId,
                                                  short gravity, short urgency, short trend) {
        OffsetDateTime now = OffsetDateTime.now();
        Optional<WhbClassificationEntity> existing = (flowelementId != null)
            ? classRepo.findByTenantIdAndProcessdefIdAndFlowelementIdAndProcessversionIdIsNull(
                tenantId, processdefId, flowelementId)
            : classRepo.findByTenantIdAndProcessdefIdAndFlowelementIdIsNullAndProcessversionIdIsNull(
                tenantId, processdefId);

        WhbClassificationEntity c = existing.orElseGet(() -> {
            WhbClassificationEntity n = new WhbClassificationEntity();
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
