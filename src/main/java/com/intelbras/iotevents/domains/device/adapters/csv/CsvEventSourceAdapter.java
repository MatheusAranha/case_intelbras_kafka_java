package com.intelbras.iotevents.domains.device.adapters.csv;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.intelbras.iotevents.common.logging.ExecutionLog;
import com.intelbras.iotevents.domains.device.core.DeviceEvent;
import com.intelbras.iotevents.domains.device.core.DeviceEventRow;
import com.intelbras.iotevents.domains.device.core.port.EventSourcePort;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Component
public class CsvEventSourceAdapter implements EventSourcePort {

    private static final String CSV_RESOURCE = "events.csv";

    private final ObjectMapper objectMapper;

    public CsvEventSourceAdapter(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public List<DeviceEvent> readAll() {
        ExecutionLog log = ExecutionLog.start(CsvEventSourceAdapter.class, "readAll").context("resource", CSV_RESOURCE);
        List<DeviceEvent> events = new ArrayList<>();
        int skipped = 0;

        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(CSV_RESOURCE)) {
            if (inputStream == null) {
                throw new IllegalStateException("CSV resource not found: " + CSV_RESOURCE);
            }
            Reader reader = new InputStreamReader(inputStream, StandardCharsets.UTF_8);
            CSVFormat format = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).build();

            for (CSVRecord record : format.parse(reader)) {
                try {
                    events.add(DeviceEvent.from(toRow(record)));
                } catch (Exception rowError) {
                    skipped++;
                    log.step("Skipped malformed CSV row", Map.of(
                        "rowNumber", record.getRecordNumber(), "reason", String.valueOf(rowError.getMessage())));
                }
            }

            log.step("Parsed CSV rows", Map.of("valid", events.size(), "skipped", skipped));
            log.info("Read device events from CSV");
            return events;
        } catch (Exception ex) {
            log.step("Unhandled exception while reading CSV");
            log.error("Failed to read device events from CSV", ex);
            throw new IllegalStateException("Failed to read device events from CSV", ex);
        }
    }

    private DeviceEventRow toRow(CSVRecord record) {
        Map<String, Object> bodyCompleto = parseJsonObject(getOrNull(record, "body_completo"));
        String deviceId = resolveDeviceId(getOrNull(record, "did"), bodyCompleto);
        String nativeId = resolveNativeId(bodyCompleto);
        Map<String, Object> payload = parseJsonObject(getOrNull(record, "content"));

        return new DeviceEventRow(nativeId, deviceId, record.get("msgType"), record.get("timestamp_log"), payload);
    }

    private String resolveDeviceId(String did, Map<String, Object> bodyCompleto) {
        if (did != null && !did.isBlank()) {
            return did;
        }
        Object fallback = bodyCompleto.get("deviceId");
        return fallback != null ? String.valueOf(fallback) : "UNKNOWN";
    }

    private String resolveNativeId(Map<String, Object> bodyCompleto) {
        Object id = bodyCompleto.get("id");
        if (id != null) {
            return String.valueOf(id);
        }
        Object alarmId = bodyCompleto.get("alarmId");
        return alarmId != null ? String.valueOf(alarmId) : null;
    }

    private Map<String, Object> parseJsonObject(String json) {
        if (json == null || json.isBlank()) {
            return Map.of();
        }
        try {
            return objectMapper.readValue(json, new TypeReference<Map<String, Object>>() {
            });
        } catch (Exception invalidJson) {
            return Map.of();
        }
    }

    private String getOrNull(CSVRecord record, String column) {
        return record.isMapped(column) && !record.get(column).isBlank() ? record.get(column) : null;
    }
}
