package com.imap.bpm.infrastructure.repository;

import com.imap.bpm.infrastructure.entity.MessageCorrelation;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface MessageCorrelationRepository extends JpaRepository<MessageCorrelation, UUID> {

    /**
     * Lookup para mensaje dirigido (point-to-point). Devuelve correlations
     * waiting que matcheen exactamente messagedefId + correlationKey.
     * Típicamente devuelve 1 row (un token waiting por ese key).
     */
    List<MessageCorrelation> findByMessagedefIdAndCorrelationKeyAndLifecycle(
        UUID messagedefId, String correlationKey, String lifecycle);

    /**
     * Lookup para signal (broadcast). Devuelve TODAS las correlations
     * waiting con ese messagedefId (correlationKey siempre BROADCAST_KEY
     * para señales, pero el repo no asume — el caller filtra).
     */
    List<MessageCorrelation> findByMessagedefIdAndLifecycle(
        UUID messagedefId, String lifecycle);

    List<MessageCorrelation> findByTokenIdAndLifecycle(UUID tokenId, String lifecycle);

    /** Para cascade DELETE de instance (admin cleanup). */
    long deleteByProcessinstanceId(UUID processinstanceId);
}
