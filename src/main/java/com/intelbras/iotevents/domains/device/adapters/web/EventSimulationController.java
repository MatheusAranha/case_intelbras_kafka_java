package com.intelbras.iotevents.domains.device.adapters.web;

import com.intelbras.iotevents.domains.device.core.usecase.SimulateIngestionUseCase;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/events")
public class EventSimulationController {

    private final SimulateIngestionUseCase simulateIngestionUseCase;

    public EventSimulationController(SimulateIngestionUseCase simulateIngestionUseCase) {
        this.simulateIngestionUseCase = simulateIngestionUseCase;
    }

    @PostMapping("/simulate")
    public ResponseEntity<SimulationResponse> simulate() {
        return ResponseEntity.ok(new SimulationResponse(simulateIngestionUseCase.execute()));
    }

    public record SimulationResponse(int publishedCount) {
    }
}
