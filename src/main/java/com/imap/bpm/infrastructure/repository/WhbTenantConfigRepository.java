package com.imap.bpm.infrastructure.repository;

import com.imap.bpm.infrastructure.entity.WhbTenantConfig;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;
import java.util.UUID;

/**
 * Política de priorización del WorkHub por tenant (1 fila por tenant).
 * Filtro por tenant explícito (la RLS de bpm es inerte hasta el sprint RLS-bpm-wide).
 */
public interface WhbTenantConfigRepository extends JpaRepository<WhbTenantConfig, UUID> {

    Optional<WhbTenantConfig> findByTenantId(UUID tenantId);
}
