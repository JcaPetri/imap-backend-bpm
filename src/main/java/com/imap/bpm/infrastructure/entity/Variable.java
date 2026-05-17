package com.imap.bpm.infrastructure.entity;

import jakarta.persistence.*;

import java.time.OffsetDateTime;
import java.util.UUID;

/**
 * JPA entity para bpm.bpm_pro_variable_tbl. Variables k/v por instance.
 * UNIQUE(processinstance_id, var_name).
 */
@Entity
@Table(name = "bpm_pro_variable_tbl")
public class Variable {

    @Id @Column(name = "id")                                 private UUID id;
    @Column(name = "tenant_id", nullable = false)            private UUID tenantId;
    @Column(name = "processinstance_id", nullable = false)   private UUID processinstanceId;
    @Column(name = "var_name", nullable = false, length = 100) private String varName;
    @Column(name = "var_value", columnDefinition = "text")   private String varValue;
    @Column(name = "var_type", nullable = false, length = 20) private String varType;

    @Column(name = "state_id", nullable = false)             private UUID stateId;
    @Column(name = "created_at", nullable = false)           private OffsetDateTime createdAt;
    @Column(name = "updated_at", nullable = false)           private OffsetDateTime updatedAt;
    @Column(name = "created_by_id")                          private UUID createdById;
    @Column(name = "updated_by_id")                          private UUID updatedById;
    @Column(name = "owned_by_id")                            private UUID ownedById;
    @Column(name = "timezone_id")                            private UUID timezoneId;
    @Column(name = "table_history")                          private String tableHistory;
    @Column(name = "data_language_id")                       private UUID dataLanguageId;

    public Variable() {}

    public UUID getId()                       { return id; }
    public void setId(UUID id)                { this.id = id; }
    public UUID getTenantId()                 { return tenantId; }
    public void setTenantId(UUID id)          { this.tenantId = id; }
    public UUID getProcessinstanceId()        { return processinstanceId; }
    public void setProcessinstanceId(UUID id) { this.processinstanceId = id; }
    public String getVarName()                { return varName; }
    public void setVarName(String s)          { this.varName = s; }
    public String getVarValue()               { return varValue; }
    public void setVarValue(String s)         { this.varValue = s; }
    public String getVarType()                { return varType; }
    public void setVarType(String s)          { this.varType = s; }

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
