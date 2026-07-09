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

import com.imap.bpm.domain.model.WhbClassification;
import com.imap.bpm.domain.model.WhbTenantConfig;
import com.imap.bpm.domain.port.out.WhbClassificationRepository;
import com.imap.bpm.domain.port.out.WhbTenantConfigRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.List;
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
        WhbTenantConfig existing = configRepo.findByTenantId(tenantId).orElse(null);
        UUID id                  = existing != null ? existing.getId() : UUID.randomUUID();
        OffsetDateTime createdAt = existing != null ? existing.getCreatedAt() : now;
        UUID createdById         = existing != null ? existing.getCreatedById() : userId;
        UUID ownedById           = existing != null ? existing.getOwnedById() : null;
        UUID stateId             = existing != null ? existing.getStateId() : DEFAULT_STATE_ACTIVE;

        WhbTenantConfig model = new WhbTenantConfig(id, tenantId, mode, wg, wu, wt, highPct, mediumPct,
            stateId, createdAt, createdById, now, userId, ownedById);
        return configRepo.save(model);
    }

    public List<WhbClassification> listClassifications(UUID tenantId) {
        return classRepo.findCurrentByTenant(tenantId);
    }

    @Transactional
    public WhbClassification upsertClassification(UUID tenantId, UUID userId,
                                                  UUID processdefId, UUID flowelementId,
                                                  short gravity, short urgency, short trend) {
        OffsetDateTime now = OffsetDateTime.now();
        WhbClassification existing = (flowelementId != null)
            ? classRepo.findCurrentForFlowelement(tenantId, processdefId, flowelementId).orElse(null)
            : classRepo.findCurrentProcessLevel(tenantId, processdefId).orElse(null);

        UUID id                  = existing != null ? existing.getId() : UUID.randomUUID();
        OffsetDateTime createdAt = existing != null ? existing.getCreatedAt() : now;
        UUID createdById         = existing != null ? existing.getCreatedById() : userId;
        UUID ownedById           = existing != null ? existing.getOwnedById() : null;
        UUID stateId             = existing != null ? existing.getStateId() : DEFAULT_STATE_ACTIVE;
        UUID processversionId    = existing != null ? existing.getProcessversionId() : null;

        WhbClassification model = new WhbClassification(id, tenantId, processdefId, processversionId,
            flowelementId, gravity, urgency, trend, stateId, createdAt, createdById, now, userId, ownedById);
        return classRepo.save(model);
    }
}
