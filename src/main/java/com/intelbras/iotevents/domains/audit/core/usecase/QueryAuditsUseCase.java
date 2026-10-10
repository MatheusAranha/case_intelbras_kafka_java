package com.intelbras.iotevents.domains.audit.core.usecase;

import com.intelbras.iotevents.common.logging.ExecutionLog;
import com.intelbras.iotevents.domains.audit.core.AuditQueryFilter;
import com.intelbras.iotevents.domains.audit.core.AuditRecord;
import com.intelbras.iotevents.domains.audit.core.port.AuditRepositoryPort;

import java.util.List;
import java.util.Map;

public class QueryAuditsUseCase {

    private final AuditRepositoryPort auditRepositoryPort;

    public QueryAuditsUseCase(AuditRepositoryPort auditRepositoryPort) {
        this.auditRepositoryPort = auditRepositoryPort;
    }

    public List<AuditRecord> execute(AuditQueryFilter filter) {
        ExecutionLog log = ExecutionLog.start(QueryAuditsUseCase.class, "execute")
            .context("deviceId", filter.deviceId())
            .context("eventType", filter.eventType())
            .context("from", filter.from())
            .context("to", filter.to());
        try {
            List<AuditRecord> results = auditRepositoryPort.query(filter);
            log.step("Queried audit store", Map.of("resultCount", results.size()));

            log.info("Queried audits");
            return results;
        } catch (Exception ex) {
            log.step("Unhandled exception while querying audits");
            log.error("Failed to query audits", ex);
            throw ex;
        }
    }
}
