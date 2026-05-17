package com.imap.bpm.infrastructure.entity;

import io.hypersistence.utils.hibernate.type.json.JsonBinaryType;
import jakarta.persistence.*;
import org.hibernate.annotations.Type;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

/**
 * JPA entity para bpm.bpm_pro_jobexecutor_tbl.
 *
 * Un job es una tarea programada del motor (timer / retry / escalation /
 * async_continuation). Worker @Scheduled lo polea cada N segundos y dispara
 * la acción correspondiente cuando fire_at <= NOW().
 *
 * MVP (A2): solo job_type='timer' soportado — habilita intermediate_event
 * con timer. Retry/escalation/async_continuation se agregan después.
 */
@Entity
@Table(name = "bpm_pro_jobexecutor_tbl")
public class JobExecutor {

    @Id @Column(name = "id")                                 private UUID id;
    @Column(name = "tenant_id", nullable = false)            private UUID tenantId;
    @Column(name = "processinstance_id", nullable = false)   private UUID processinstanceId;
    @Column(name = "token_id")                               private UUID tokenId;
    @Column(name = "job_type", nullable = false, length = 30) private String jobType;
    @Column(name = "fire_at", nullable = false)              private OffsetDateTime fireAt;

    @Type(JsonBinaryType.class)
    @Column(name = "config_jsonb", columnDefinition = "jsonb")
    private Map<String, Object> config;

    @Column(name = "lifecycle", nullable = false, length = 20) private String lifecycle;
    @Column(name = "retries", nullable = false)              private Integer retries;
    @Column(name = "max_retries", nullable = false)          private Integer maxRetries;
    @Column(name = "last_error")                             private String lastError;
    @Column(name = "fired_at")                               private OffsetDateTime firedAt;

    @Column(name = "state_id", nullable = false)             private UUID stateId;
    @Column(name = "created_at", nullable = false)           private OffsetDateTime createdAt;
    @Column(name = "updated_at", nullable = false)           private OffsetDateTime updatedAt;
    @Column(name = "created_by_id")                          private UUID createdById;
    @Column(name = "updated_by_id")                          private UUID updatedById;
    @Column(name = "owned_by_id")                            private UUID ownedById;
    @Column(name = "timezone_id")                            private UUID timezoneId;
    @Column(name = "table_history")                          private String tableHistory;
    @Column(name = "data_language_id")                       private UUID dataLanguageId;

    public JobExecutor() {}

    public UUID getId()                       { return id; }
    public void setId(UUID id)                { this.id = id; }
    public UUID getTenantId()                 { return tenantId; }
    public void setTenantId(UUID id)          { this.tenantId = id; }
    public UUID getProcessinstanceId()        { return processinstanceId; }
    public void setProcessinstanceId(UUID id) { this.processinstanceId = id; }
    public UUID getTokenId()                  { return tokenId; }
    public void setTokenId(UUID id)           { this.tokenId = id; }
    public String getJobType()                { return jobType; }
    public void setJobType(String s)          { this.jobType = s; }
    public OffsetDateTime getFireAt()         { return fireAt; }
    public void setFireAt(OffsetDateTime t)   { this.fireAt = t; }
    public Map<String, Object> getConfig()    { return config; }
    public void setConfig(Map<String, Object> m) { this.config = m; }
    public String getLifecycle()              { return lifecycle; }
    public void setLifecycle(String s)        { this.lifecycle = s; }
    public Integer getRetries()               { return retries; }
    public void setRetries(Integer i)         { this.retries = i; }
    public Integer getMaxRetries()            { return maxRetries; }
    public void setMaxRetries(Integer i)      { this.maxRetries = i; }
    public String getLastError()              { return lastError; }
    public void setLastError(String s)        { this.lastError = s; }
    public OffsetDateTime getFiredAt()        { return firedAt; }
    public void setFiredAt(OffsetDateTime t)  { this.firedAt = t; }

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
    public UUID getTimezoneId()               { return timezoneId; }
    public void setTimezoneId(UUID id)        { this.timezoneId = id; }
    public String getTableHistory()           { return tableHistory; }
    public void setTableHistory(String s)     { this.tableHistory = s; }
    public UUID getDataLanguageId()           { return dataLanguageId; }
    public void setDataLanguageId(UUID id)    { this.dataLanguageId = id; }
}
