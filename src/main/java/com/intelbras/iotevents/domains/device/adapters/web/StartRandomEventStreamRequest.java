package com.intelbras.iotevents.domains.device.adapters.web;

import com.intelbras.iotevents.domains.device.core.EventCategory;

import java.util.List;

public record StartRandomEventStreamRequest(Double eventsPerMinute, Double dlqRate, Double duplicateRate,
                                              List<EventCategory> categories) {
}
