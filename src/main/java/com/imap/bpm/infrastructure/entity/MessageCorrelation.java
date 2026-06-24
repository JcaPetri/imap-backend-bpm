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

package com.imap.bpm.infrastructure.entity;

import io.hypersistence.utils.hibernate.type.json.JsonBinaryType;
import jakarta.persistence.*;
import org.hibernate.annotations.Type;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

/**
 * JPA entity para bpm.bpm_pro_messagecorrelation_tbl.
 *
 * Cada row representa un token waiting por:
 *   - un message dirigido (correlation_key específica, ej "order_12345")
 *   - un signal broadcast (correlation_key="__BROADCAST__")
 *
 * Cuando el motor recibe POST /v1/bpm/messages/correlate o /signals/broadcast,
 * busca rows matching y reactiva los tokens correspondientes.
 *
 * messagedef_id es un UUID determinístico generado por
 * `UUID.nameUUIDFromBytes("message:<code>")` o `"signal:<code>"`. Esto evita
 * un lookup vs system EAV para resolver el UUID del messagedef definition.
 */
@Entity
@Table(name = "bpm_pro_messagecorrelation_tbl")
public class MessageCorrelation {

    public static final String BROADCAST_KEY = "__BROADCAST__";

    @Id @Column(name = "id")                                 private UUID id;
    @Column(name = "tenant_id", nullable = false)            private UUID tenantId;
    @Column(name = "processinstance_id", nullable = false)   private UUID processinstanceId;
    @Column(name = "token_id")                               private UUID tokenId;
    @Column(name = "messagedef_id", nullable = false)        private UUID messagedefId;
    @Column(name = "correlation_key", nullable = false, length = 255) private String correlationKey;
    @Column(name = "lifecycle", nullable = false, length = 20) private String lifecycle;
    @Column(name = "expires_at")                             private OffsetDateTime expiresAt;
    @Column(name = "matched_at")                             private OffsetDateTime matchedAt;

    @Type(JsonBinaryType.class)
    @Column(name = "matched_payload_jsonb", columnDefinition = "jsonb")
    private Map<String, Object> matchedPayload;

    @Column(name = "state_id", nullable = false)             private UUID stateId;
    @Column(name = "created_at", nullable = false)           private OffsetDateTime createdAt;
    @Column(name = "updated_at", nullable = false)           private OffsetDateTime updatedAt;
    @Column(name = "created_by_id")                          private UUID createdById;
    @Column(name = "updated_by_id")                          private UUID updatedById;
    @Column(name = "owned_by_id")                            private UUID ownedById;

    public MessageCorrelation() {}

    /**
     * Genera el messagedef_id determinístico para un message code.
     * Mismo input siempre devuelve mismo UUID; permite correlation sin
     * persistir messagedef formales en MVP.
     */
    public static UUID messageRefId(String messageCode) {
        return UUID.nameUUIDFromBytes(("message:" + messageCode).getBytes());
    }

    /** Idem para signal codes (namespace distinto evita colisión). */
    public static UUID signalRefId(String signalCode) {
        return UUID.nameUUIDFromBytes(("signal:" + signalCode).getBytes());
    }

    public UUID getId()                       { return id; }
    public void setId(UUID id)                { this.id = id; }
    public UUID getTenantId()                 { return tenantId; }
    public void setTenantId(UUID id)          { this.tenantId = id; }
    public UUID getProcessinstanceId()        { return processinstanceId; }
    public void setProcessinstanceId(UUID id) { this.processinstanceId = id; }
    public UUID getTokenId()                  { return tokenId; }
    public void setTokenId(UUID id)           { this.tokenId = id; }
    public UUID getMessagedefId()             { return messagedefId; }
    public void setMessagedefId(UUID id)      { this.messagedefId = id; }
    public String getCorrelationKey()         { return correlationKey; }
    public void setCorrelationKey(String s)   { this.correlationKey = s; }
    public String getLifecycle()              { return lifecycle; }
    public void setLifecycle(String s)        { this.lifecycle = s; }
    public OffsetDateTime getExpiresAt()      { return expiresAt; }
    public void setExpiresAt(OffsetDateTime t){ this.expiresAt = t; }
    public OffsetDateTime getMatchedAt()      { return matchedAt; }
    public void setMatchedAt(OffsetDateTime t){ this.matchedAt = t; }
    public Map<String, Object> getMatchedPayload() { return matchedPayload; }
    public void setMatchedPayload(Map<String, Object> m) { this.matchedPayload = m; }

    public UUID getStateId()                  { return stateId; }
    public void setStateId(UUID id)           { this.stateId = id; }
    public OffsetDateTime getCreatedAt()      { return createdAt; }
    public void setCreatedAt(OffsetDateTime t){ this.createdAt = t; }
    public OffsetDateTime getUpdatedAt()      { return updatedAt; }
    public void setUpdatedAt(OffsetDateTime t){ this.updatedAt = t; }
    public UUID getCreatedById()              { return createdById; }
    public void setCreatedById(UUID id)       { this.createdById = id; }
    public UUID getUpdatedById()              { return updatedById; }
    public void setUpdatedById(UUID id)       { this.updatedById = id; }
    public UUID getOwnedById()                { return ownedById; }
    public void setOwnedById(UUID id)         { this.ownedById = id; }
}
