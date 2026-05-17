package com.imap.bpm.infrastructure.repository;

import com.imap.bpm.infrastructure.entity.Variable;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface VariableRepository extends JpaRepository<Variable, UUID> {
    Optional<Variable> findByProcessinstanceIdAndVarName(UUID processinstanceId, String varName);
    List<Variable> findByProcessinstanceId(UUID processinstanceId);

    /** Para cascade DELETE de instance (admin cleanup). */
    long deleteByProcessinstanceId(UUID processinstanceId);
}
