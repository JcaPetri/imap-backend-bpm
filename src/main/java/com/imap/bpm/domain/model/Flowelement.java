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
//  • [persistence] Audit7: tenant_id|state_id|created_at|created_by_id|updated_at|updated_by_id|owned_by_id
//  • [persistence] RLS por tenant + tenant_id en toda tabla
//  • [persistence] Soft-delete por state (archived), nunca DELETE físico
//  • [persistence] Naming SQL Opción B (id PK · _id FKs · _at timestamps · is_* booleans)
//  • [persistence] Flyway: cambios de schema versionados, nunca DDL ad-hoc en prod
//  • [persistence] Native queries: CAST(x AS t), NO x::t (Hibernate confunde :: con bind param)
//  • [bpm] Orquestador central (tipo Temporal/Step Functions)
//  • [bpm] processdef→processversion: snapshot inmutable al activar
//  • [bpm] Las defs de proceso viven acá (no en system)
//  • [bpm] Camunda 8 como norte; interim form-driven en prod
// ─── GOLDEN-RULES:END ───

package com.imap.bpm.domain.model;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * Un nodo del grafo del proceso (V015): start/end/task/gateway/event…
 * Agregado de dominio puro (sin JPA).
 */
public class Flowelement {

    private final UUID id;
    private final UUID tenantId;
    private final UUID processversionId;
    private final String elementCode;
    private final String elementType;
    private final String name;
    private final String config;
    private final Integer sortOrder;
    private final UUID stateId;
    private final OffsetDateTime createdAt;
    private final OffsetDateTime updatedAt;
    private final UUID createdById;
    private final UUID updatedById;
    private final UUID ownedById;

    public Flowelement(UUID id, UUID tenantId, UUID processversionId, String elementCode,
                       String elementType, String name, String config, Integer sortOrder, UUID stateId,
                       OffsetDateTime createdAt, OffsetDateTime updatedAt,
                       UUID createdById, UUID updatedById, UUID ownedById) {
        this.id = id;
        this.tenantId = tenantId;
        this.processversionId = processversionId;
        this.elementCode = elementCode;
        this.elementType = elementType;
        this.name = name;
        this.config = config;
        this.sortOrder = sortOrder;
        this.stateId = stateId;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.createdById = createdById;
        this.updatedById = updatedById;
        this.ownedById = ownedById;
    }

    public UUID getId() { return id; }
    public UUID getTenantId() { return tenantId; }
    public UUID getProcessversionId() { return processversionId; }
    public String getElementCode() { return elementCode; }
    public String getElementType() { return elementType; }
    public String getName() { return name; }
    public String getConfig() { return config; }
    public Integer getSortOrder() { return sortOrder; }
    public UUID getStateId() { return stateId; }
    public OffsetDateTime getCreatedAt() { return createdAt; }
    public OffsetDateTime getUpdatedAt() { return updatedAt; }
    public UUID getCreatedById() { return createdById; }
    public UUID getUpdatedById() { return updatedById; }
    public UUID getOwnedById() { return ownedById; }
}
