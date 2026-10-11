package com.intelbras.iotevents.domains.device.adapters.web;

import com.intelbras.iotevents.domains.device.core.EventCategory;
import com.intelbras.iotevents.domains.device.core.RandomEventStreamConfig;
import com.intelbras.iotevents.domains.device.core.RandomEventStreamStatus;
import com.intelbras.iotevents.domains.device.core.usecase.GetRandomEventStreamStatusUseCase;
import com.intelbras.iotevents.domains.device.core.usecase.StartRandomEventStreamUseCase;
import com.intelbras.iotevents.domains.device.core.usecase.StopRandomEventStreamUseCase;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.util.List;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(RandomEventStreamController.class)
class RandomEventStreamControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private StartRandomEventStreamUseCase startUseCase;

    @MockBean
    private StopRandomEventStreamUseCase stopUseCase;

    @MockBean
    private GetRandomEventStreamStatusUseCase getStatusUseCase;

    @Test
    void startShouldDefaultToAllCategoriesWhenOmitted() throws Exception {
        RandomEventStreamConfig expectedConfig = new RandomEventStreamConfig(
            600, 0.05, 0.1, List.of(EventCategory.values()));
        RandomEventStreamStatus status = new RandomEventStreamStatus(true, expectedConfig, Instant.now(), 0, 0);
        when(startUseCase.execute(expectedConfig)).thenReturn(status);

        mockMvc.perform(post("/events/random-stream/start")
                .contentType("application/json")
                .content("{\"eventsPerMinute\":600,\"dlqRate\":0.05,\"duplicateRate\":0.1}"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.running").value(true));
    }

    @Test
    void startShouldReturn400WhenEventsPerMinuteIsMissing() throws Exception {
        mockMvc.perform(post("/events/random-stream/start")
                .contentType("application/json")
                .content("{\"dlqRate\":0.05}"))
            .andExpect(status().isBadRequest());
    }

    @Test
    void startShouldReturn400ForAnUnrecognizedCategoryName() throws Exception {
        mockMvc.perform(post("/events/random-stream/start")
                .contentType("application/json")
                .content("{\"eventsPerMinute\":60,\"categories\":[\"BOGUS\"]}"))
            .andExpect(status().isBadRequest());
    }

    @Test
    void stopShouldReturnTheResultingStatus() throws Exception {
        when(stopUseCase.execute()).thenReturn(new RandomEventStreamStatus(false, null, null, 5, 5));

        mockMvc.perform(post("/events/random-stream/stop"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.running").value(false));
    }

    @Test
    void statusShouldReturnTheCurrentStatus() throws Exception {
        when(getStatusUseCase.execute()).thenReturn(new RandomEventStreamStatus(false, null, null, 0, 0));

        mockMvc.perform(get("/events/random-stream/status"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.running").value(false));
    }
}
