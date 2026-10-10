package com.intelbras.iotevents.domains.audit;

import com.intelbras.iotevents.common.metrics.EventMetrics;
import com.intelbras.iotevents.domains.audit.core.handler.DeviceEventAuditHandler;
import com.intelbras.iotevents.domains.audit.core.handler.OtherEventAuditHandler;
import com.intelbras.iotevents.domains.audit.core.port.AuditRepositoryPort;
import com.intelbras.iotevents.domains.audit.core.usecase.QueryAuditsUseCase;
import com.intelbras.iotevents.domains.audit.core.usecase.RecordDeviceEventAuditUseCase;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class AuditUseCaseConfig {

    @Bean
    public RecordDeviceEventAuditUseCase recordDeviceEventAuditUseCase(AuditRepositoryPort auditRepositoryPort,
                                                                         List<DeviceEventAuditHandler> handlers,
                                                                         OtherEventAuditHandler fallbackHandler,
                                                                         EventMetrics eventMetrics) {
        return new RecordDeviceEventAuditUseCase(auditRepositoryPort, handlers, fallbackHandler, eventMetrics);
    }

    @Bean
    public QueryAuditsUseCase queryAuditsUseCase(AuditRepositoryPort auditRepositoryPort) {
        return new QueryAuditsUseCase(auditRepositoryPort);
    }
}
