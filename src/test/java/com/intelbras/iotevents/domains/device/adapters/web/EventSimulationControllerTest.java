package com.intelbras.iotevents.domains.device.adapters.web;

import com.intelbras.iotevents.domains.device.core.usecase.SimulateIngestionUseCase;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(EventSimulationController.class)
class EventSimulationControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private SimulateIngestionUseCase simulateIngestionUseCase;

    @Test
    void shouldReturnPublishedCountFromTheUseCase() throws Exception {
        when(simulateIngestionUseCase.execute()).thenReturn(104);

        mockMvc.perform(post("/events/simulate"))
            .andExpect(status().isOk())
            .andExpect(content().json("{\"publishedCount\":104}"));
    }
}
