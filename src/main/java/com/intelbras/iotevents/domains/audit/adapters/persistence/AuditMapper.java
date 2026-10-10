package com.intelbras.iotevents.domains.audit.adapters.persistence;

import com.intelbras.iotevents.domains.audit.core.AuditRecord;

public final class AuditMapper {

    private AuditMapper() {
    }

    public static AuditJpaEntity toEntity(AuditRecord record) {
        return new AuditJpaEntity(record.dedupKey(), record.deviceId(), record.eventCategory(), record.eventType(),
            record.eventTimestamp(), record.generalInfo(), record.consumedAt(), record.createdAt());
    }

    public static AuditRecord toDomain(AuditJpaEntity entity) {
        return new AuditRecord(entity.getEventId(), entity.getDeviceId(), entity.getEventCategory(),
            entity.getEventType(), entity.getEventTimestamp(), entity.getGeneralInfo(), entity.getConsumedAt(),
            entity.getCreatedAt());
    }
}
