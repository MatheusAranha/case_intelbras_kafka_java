package com.intelbras.iotevents.domains.audit.adapters.web;

import com.intelbras.iotevents.domains.audit.core.AuditQueryFilter;
import com.intelbras.iotevents.domains.audit.core.AuditRecord;
import com.intelbras.iotevents.domains.audit.core.usecase.QueryAuditsUseCase;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.List;

@RestController
@RequestMapping("/audits")
public class AuditController {

    private final QueryAuditsUseCase queryAuditsUseCase;

    public AuditController(QueryAuditsUseCase queryAuditsUseCase) {
        this.queryAuditsUseCase = queryAuditsUseCase;
    }

    @GetMapping
    public List<AuditRecord> getAudits(
            @RequestParam(required = false) String deviceId,
            @RequestParam(required = false) String eventType,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to) {
        return queryAuditsUseCase.execute(new AuditQueryFilter(deviceId, eventType, from, to));
    }
}
