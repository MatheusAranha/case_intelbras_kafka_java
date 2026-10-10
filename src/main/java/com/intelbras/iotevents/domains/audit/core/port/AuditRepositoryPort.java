package com.intelbras.iotevents.domains.audit.core.port;

import com.intelbras.iotevents.domains.audit.core.AuditQueryFilter;
import com.intelbras.iotevents.domains.audit.core.AuditRecord;

import java.util.List;

public interface AuditRepositoryPort {
    void save(AuditRecord record);

    boolean existsByDedupKey(String eventId);

    List<AuditRecord> query(AuditQueryFilter filter);
}
