package com.intelbras.iotevents.domains.audit.adapters.persistence;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;

public interface AuditJpaRepository extends JpaRepository<AuditJpaEntity, Long>, JpaSpecificationExecutor<AuditJpaEntity> {
    boolean existsByEventId(String eventId);
}
