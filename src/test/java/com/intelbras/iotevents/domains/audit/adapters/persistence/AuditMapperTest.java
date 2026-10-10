package com.intelbras.iotevents.domains.audit.adapters.persistence;

import com.intelbras.iotevents.domains.audit.core.AuditRecord;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class AuditMapperTest {

    @Test
    void shouldRoundTripBetweenDomainAndJpaEntity() {
        Instant now = Instant.now();
        AuditRecord record = new AuditRecord("evt-1", "DEVICE-00014", "PROPERTY_CHANGE", "iotProperty",
            Instant.parse("2026-09-25T08:47:51.157000Z"), "Device DEVICE-00014 reported a property change", now, now);

        AuditRecord roundTripped = AuditMapper.toDomain(AuditMapper.toEntity(record));

        assertThat(roundTripped).isEqualTo(record);
    }
}
