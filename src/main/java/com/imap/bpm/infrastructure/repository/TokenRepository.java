package com.imap.bpm.infrastructure.repository;

import com.imap.bpm.infrastructure.entity.Token;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

public interface TokenRepository extends JpaRepository<Token, UUID> {
    List<Token> findByProcessinstanceIdAndLifecycleIn(UUID processinstanceId, List<String> lifecycles);
}
