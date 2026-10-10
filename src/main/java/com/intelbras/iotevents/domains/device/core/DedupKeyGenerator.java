package com.intelbras.iotevents.domains.device.core;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.TreeMap;

public final class DedupKeyGenerator {

    private DedupKeyGenerator() {
    }

    public static String generate(String deviceId, String eventType, String timestampRaw, Map<String, Object> payload) {
        String canonicalPayload = new TreeMap<>(payload).toString();
        String input = deviceId + "|" + eventType + "|" + timestampRaw + "|" + canonicalPayload;
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder();
            for (byte b : hash) {
                hex.append(String.format("%02x", b));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 not available", impossible);
        }
    }
}
