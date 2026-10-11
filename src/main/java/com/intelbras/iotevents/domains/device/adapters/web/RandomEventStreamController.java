package com.intelbras.iotevents.domains.device.adapters.web;

import com.intelbras.iotevents.domains.device.core.EventCategory;
import com.intelbras.iotevents.domains.device.core.RandomEventStreamConfig;
import com.intelbras.iotevents.domains.device.core.RandomEventStreamStatus;
import com.intelbras.iotevents.domains.device.core.usecase.GetRandomEventStreamStatusUseCase;
import com.intelbras.iotevents.domains.device.core.usecase.StartRandomEventStreamUseCase;
import com.intelbras.iotevents.domains.device.core.usecase.StopRandomEventStreamUseCase;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/events/random-stream")
public class RandomEventStreamController {

    private final StartRandomEventStreamUseCase startUseCase;
    private final StopRandomEventStreamUseCase stopUseCase;
    private final GetRandomEventStreamStatusUseCase getStatusUseCase;

    public RandomEventStreamController(StartRandomEventStreamUseCase startUseCase,
                                        StopRandomEventStreamUseCase stopUseCase,
                                        GetRandomEventStreamStatusUseCase getStatusUseCase) {
        this.startUseCase = startUseCase;
        this.stopUseCase = stopUseCase;
        this.getStatusUseCase = getStatusUseCase;
    }

    @PostMapping("/start")
    public ResponseEntity<RandomEventStreamStatus> start(@RequestBody StartRandomEventStreamRequest request) {
        List<EventCategory> categories = request.categories() == null
            ? List.of(EventCategory.values())
            : request.categories();
        double eventsPerMinute = request.eventsPerMinute() == null ? 0 : request.eventsPerMinute();
        double dlqRate = request.dlqRate() == null ? 0.0 : request.dlqRate();
        double duplicateRate = request.duplicateRate() == null ? 0.0 : request.duplicateRate();

        RandomEventStreamConfig config = new RandomEventStreamConfig(eventsPerMinute, dlqRate, duplicateRate, categories);
        return ResponseEntity.ok(startUseCase.execute(config));
    }

    @PostMapping("/stop")
    public ResponseEntity<RandomEventStreamStatus> stop() {
        return ResponseEntity.ok(stopUseCase.execute());
    }

    @GetMapping("/status")
    public ResponseEntity<RandomEventStreamStatus> status() {
        return ResponseEntity.ok(getStatusUseCase.execute());
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> handleValidationError(IllegalArgumentException ex) {
        return ResponseEntity.badRequest().body(Map.of("error", ex.getMessage()));
    }
}
