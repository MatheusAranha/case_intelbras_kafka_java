package com.intelbras.iotevents.domains.audit.core.usecase;

import com.intelbras.iotevents.domains.audit.core.AuditQueryFilter;
import com.intelbras.iotevents.domains.audit.core.AuditRecord;
import com.intelbras.iotevents.domains.audit.core.port.AuditRepositoryPort;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class QueryAuditsUseCaseTest {

    @Test
    void shouldReturnWhateverTheRepositoryPortReturnsForTheGivenFilter() {
        AuditRepositoryPort repositoryPort = mock(AuditRepositoryPort.class);
        AuditQueryFilter filter = new AuditQueryFilter("DEVICE-00014", null, null, null);
        AuditRecord record = new AuditRecord("evt-1", "DEVICE-00014", "PROPERTY_CHANGE", "iotProperty",
            Instant.parse("2026-09-25T08:47:51.157000Z"), "Device DEVICE-00014 reported a property change",
            Instant.now(), Instant.now());
        when(repositoryPort.query(filter)).thenReturn(List.of(record));

        assertThat(new QueryAuditsUseCase(repositoryPort).execute(filter)).containsExactly(record);
    }
}
