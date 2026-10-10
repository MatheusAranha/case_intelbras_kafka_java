package com.intelbras.iotevents.domains.device.core;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class DedupKeyGeneratorTest {

    @Test
    void shouldBeDeterministicForTheSameInputs() {
        String first = DedupKeyGenerator.generate("DEVICE-00014", "iotProperty", "2026-09-25T08:47:51.157000+00:00", Map.of("x", 1));
        String second = DedupKeyGenerator.generate("DEVICE-00014", "iotProperty", "2026-09-25T08:47:51.157000+00:00", Map.of("x", 1));

        assertThat(first).isEqualTo(second);
    }

    @Test
    void shouldDifferWhenAnyInputDiffers() {
        String first = DedupKeyGenerator.generate("DEVICE-00014", "iotProperty", "2026-09-25T08:47:51.157000+00:00", Map.of());
        String second = DedupKeyGenerator.generate("DEVICE-00015", "iotProperty", "2026-09-25T08:47:51.157000+00:00", Map.of());

        assertThat(first).isNotEqualTo(second);
    }
}
