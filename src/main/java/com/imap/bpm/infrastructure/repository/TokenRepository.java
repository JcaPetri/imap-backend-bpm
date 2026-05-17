package com.imap.bpm.infrastructure.repository;

import com.imap.bpm.infrastructure.entity.Token;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface TokenRepository extends JpaRepository<Token, UUID> {
    List<Token> findByProcessinstanceIdAndLifecycleIn(UUID processinstanceId, List<String> lifecycles);

    /**
     * A1 — Usada por parallel_gateway JOIN para sincronizar siblings:
     * cuenta cuántos tokens hermanos (misma parentTokenId) ya llegaron al
     * mismo gateway con lifecycle=waiting.
     */
    List<Token> findByProcessinstanceIdAndCurrentElementIdAndLifecycle(
        UUID processinstanceId, UUID currentElementId, String lifecycle);

    /** Para cascade DELETE de instance (admin cleanup). */
    long deleteByProcessinstanceId(UUID processinstanceId);
}
