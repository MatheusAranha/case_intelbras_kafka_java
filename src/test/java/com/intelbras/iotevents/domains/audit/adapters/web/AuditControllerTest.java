package com.intelbras.iotevents.domains.audit.adapters.web;

import com.intelbras.iotevents.domains.audit.core.AuditQueryFilter;
import com.intelbras.iotevents.domains.audit.core.AuditRecord;
import com.intelbras.iotevents.domains.audit.core.usecase.QueryAuditsUseCase;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.util.List;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(AuditController.class)
class AuditControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private QueryAuditsUseCase queryAuditsUseCase;

    @Test
    void shouldReturnAuditsMatchingTheGivenFilter() throws Exception {
        AuditRecord record = new AuditRecord("evt-1", "DEVICE-00014", "PROPERTY_CHANGE", "iotProperty",
            "Device DEVICE-00014 reported a property change", Instant.parse("2026-09-25T08:47:51.157000Z"),
            Instant.parse("2026-09-25T08:47:51.157000Z"));
        when(queryAuditsUseCase.execute(new AuditQueryFilter("DEVICE-00014", null, null, null))).thenReturn(List.of(record));

        mockMvc.perform(get("/audits").param("deviceId", "DEVICE-00014"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$[0].deviceId").value("DEVICE-00014"))
            .andExpect(jsonPath("$[0].eventCategory").value("PROPERTY_CHANGE"));
    }
}
