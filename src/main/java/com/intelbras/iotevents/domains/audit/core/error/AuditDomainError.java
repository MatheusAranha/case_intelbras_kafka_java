package com.intelbras.iotevents.domains.audit.core.error;

public class AuditDomainError extends RuntimeException {
    public AuditDomainError(String message) {
        super(message);
    }
}
