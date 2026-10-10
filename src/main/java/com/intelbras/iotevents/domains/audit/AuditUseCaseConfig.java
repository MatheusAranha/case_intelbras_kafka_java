package com.intelbras.iotevents.domains.audit;

import com.intelbras.iotevents.domains.audit.core.handler.DeviceEventAuditHandler;
import com.intelbras.iotevents.domains.audit.core.handler.MotionDetectionEventAuditHandler;
import com.intelbras.iotevents.domains.audit.core.handler.OtherEventAuditHandler;
import com.intelbras.iotevents.domains.audit.core.handler.PropertyChangeEventAuditHandler;
import com.intelbras.iotevents.domains.audit.core.handler.StatusChangeEventAuditHandler;
import com.intelbras.iotevents.domains.audit.core.port.AuditRepositoryPort;
import com.intelbras.iotevents.domains.audit.core.port.MetricsPort;
import com.intelbras.iotevents.domains.audit.core.usecase.QueryAuditsUseCase;
import com.intelbras.iotevents.domains.audit.core.usecase.RecordDeviceEventAuditUseCase;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

@Configuration
public class AuditUseCaseConfig {

    @Bean
    public PropertyChangeEventAuditHandler propertyChangeEventAuditHandler() {
        return new PropertyChangeEventAuditHandler();
    }

    @Bean
    public StatusChangeEventAuditHandler statusChangeEventAuditHandler() {
        return new StatusChangeEventAuditHandler();
    }

    @Bean
    public MotionDetectionEventAuditHandler motionDetectionEventAuditHandler() {
        return new MotionDetectionEventAuditHandler();
    }

    @Bean
    public OtherEventAuditHandler otherEventAuditHandler() {
        return new OtherEventAuditHandler();
    }

    @Bean
    public RecordDeviceEventAuditUseCase recordDeviceEventAuditUseCase(AuditRepositoryPort auditRepositoryPort,
                                                                         List<DeviceEventAuditHandler> handlers,
                                                                         OtherEventAuditHandler fallbackHandler,
                                                                         MetricsPort metricsPort) {
        return new RecordDeviceEventAuditUseCase(auditRepositoryPort, handlers, fallbackHandler, metricsPort);
    }

    @Bean
    public QueryAuditsUseCase queryAuditsUseCase(AuditRepositoryPort auditRepositoryPort) {
        return new QueryAuditsUseCase(auditRepositoryPort);
    }
}
