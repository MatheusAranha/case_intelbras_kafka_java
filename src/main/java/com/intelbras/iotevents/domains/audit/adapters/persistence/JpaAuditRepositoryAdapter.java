package com.intelbras.iotevents.domains.audit.adapters.persistence;

import com.intelbras.iotevents.domains.audit.core.AuditQueryFilter;
import com.intelbras.iotevents.domains.audit.core.AuditRecord;
import com.intelbras.iotevents.domains.audit.core.port.AuditRepositoryPort;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class JpaAuditRepositoryAdapter implements AuditRepositoryPort {

    private final AuditJpaRepository repository;

    public JpaAuditRepositoryAdapter(AuditJpaRepository repository) {
        this.repository = repository;
    }

    @Override
    public void save(AuditRecord record) {
        repository.save(AuditMapper.toEntity(record));
    }

    @Override
    public boolean existsByDedupKey(String eventId) {
        return repository.existsByEventId(eventId);
    }

    @Override
    public List<AuditRecord> query(AuditQueryFilter filter) {
        Specification<AuditJpaEntity> spec = Specification.where(null);
        if (filter.deviceId() != null) {
            spec = spec.and((root, q, cb) -> cb.equal(root.get("deviceId"), filter.deviceId()));
        }
        if (filter.eventType() != null) {
            spec = spec.and((root, q, cb) -> cb.equal(root.get("eventType"), filter.eventType()));
        }
        if (filter.from() != null) {
            spec = spec.and((root, q, cb) -> cb.greaterThanOrEqualTo(root.get("consumedAt"), filter.from()));
        }
        if (filter.to() != null) {
            spec = spec.and((root, q, cb) -> cb.lessThanOrEqualTo(root.get("consumedAt"), filter.to()));
        }
        return repository.findAll(spec).stream().map(AuditMapper::toDomain).toList();
    }
}
