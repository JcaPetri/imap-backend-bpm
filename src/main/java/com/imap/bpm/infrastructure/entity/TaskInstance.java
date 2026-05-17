package com.imap.bpm.infrastructure.entity;

import io.hypersistence.utils.hibernate.type.json.JsonBinaryType;
import jakarta.persistence.*;
import org.hibernate.annotations.Type;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.UUID;

/**
 * JPA entity para bpm.bpm_hum_taskinstance_tbl. Tarea humana asignada.
 */
@Entity
@Table(name = "bpm_hum_taskinstance_tbl")
public class TaskInstance {

    @Id @Column(name = "id")                                 private UUID id;
    @Column(name = "tenant_id", nullable = false)            private UUID tenantId;
    @Column(name = "processinstance_id", nullable = false)   private UUID processinstanceId;
    @Column(name = "flowelement_id", nullable = false)       private UUID flowelementId;
    @Column(name = "token_id")                                private UUID tokenId;
    @Column(name = "lifecycle", nullable = false, length = 20) private String lifecycle;
    @Column(name = "assigned_user_id")                       private UUID assignedUserId;
    @Column(name = "assigned_role", length = 100)            private String assignedRole;
    @Column(name = "priority", nullable = false)             private Integer priority;
    @Column(name = "due_at")                                 private OffsetDateTime dueAt;
    @Column(name = "assigned_at")                            private OffsetDateTime assignedAt;
    @Column(name = "started_at")                             private OffsetDateTime startedAt;
    @Column(name = "completed_at")                           private OffsetDateTime completedAt;

    @Type(JsonBinaryType.class)
    @Column(name = "input_data_jsonb", columnDefinition = "jsonb")
    private Map<String, Object> inputData;

    @Type(JsonBinaryType.class)
    @Column(name = "output_data_jsonb", columnDefinition = "jsonb")
    private Map<String, Object> outputData;

    @Column(name = "state_id", nullable = false)             private UUID stateId;
    @Column(name = "created_at", nullable = false)           private OffsetDateTime createdAt;
    @Column(name = "updated_at", nullable = false)           private OffsetDateTime updatedAt;
    @Column(name = "created_by_id")                          private UUID createdById;
    @Column(name = "updated_by_id")                          private UUID updatedById;
    @Column(name = "owned_by_id")                            private UUID ownedById;
    @Column(name = "timezone_id")                            private UUID timezoneId;
    @Column(name = "table_history")                          private String tableHistory;
    @Column(name = "data_language_id")                       private UUID dataLanguageId;

    public TaskInstance() {}

    public UUID getId()                       { return id; }
    public void setId(UUID id)                { this.id = id; }
    public UUID getTenantId()                 { return tenantId; }
    public void setTenantId(UUID id)          { this.tenantId = id; }
    public UUID getProcessinstanceId()        { return processinstanceId; }
    public void setProcessinstanceId(UUID id) { this.processinstanceId = id; }
    public UUID getFlowelementId()            { return flowelementId; }
    public void setFlowelementId(UUID id)     { this.flowelementId = id; }
    public UUID getTokenId()                  { return tokenId; }
    public void setTokenId(UUID id)           { this.tokenId = id; }
    public String getLifecycle()              { return lifecycle; }
    public void setLifecycle(String s)        { this.lifecycle = s; }
    public UUID getAssignedUserId()           { return assignedUserId; }
    public void setAssignedUserId(UUID id)    { this.assignedUserId = id; }
    public String getAssignedRole()           { return assignedRole; }
    public void setAssignedRole(String s)     { this.assignedRole = s; }
    public Integer getPriority()              { return priority; }
    public void setPriority(Integer p)        { this.priority = p; }
    public OffsetDateTime getDueAt()          { return dueAt; }
    public void setDueAt(OffsetDateTime t)    { this.dueAt = t; }
    public OffsetDateTime getAssignedAt()     { return assignedAt; }
    public void setAssignedAt(OffsetDateTime t) { this.assignedAt = t; }
    public OffsetDateTime getStartedAt()      { return startedAt; }
    public void setStartedAt(OffsetDateTime t){ this.startedAt = t; }
    public OffsetDateTime getCompletedAt()    { return completedAt; }
    public void setCompletedAt(OffsetDateTime t) { this.completedAt = t; }
    public Map<String, Object> getInputData() { return inputData; }
    public void setInputData(Map<String, Object> m) { this.inputData = m; }
    public Map<String, Object> getOutputData(){ return outputData; }
    public void setOutputData(Map<String, Object> m) { this.outputData = m; }

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
