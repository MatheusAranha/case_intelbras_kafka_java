package com.intelbras.iotevents.domains.audit.adapters.persistence;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;

import java.time.Instant;

@Entity
@Table(name = "audit_records", uniqueConstraints = @UniqueConstraint(name = "uq_audit_records_event_id", columnNames = "event_id"))
public class AuditJpaEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id", nullable = false)
    private String eventId;

    @Column(name = "device_id", nullable = false)
    private String deviceId;

    @Column(name = "event_category", nullable = false)
    private String eventCategory;

    @Column(name = "event_type", nullable = false)
    private String eventType;

    @Column(name = "event_timestamp", nullable = false)
    private Instant eventTimestamp;

    @Column(name = "general_info", nullable = false, columnDefinition = "TEXT")
    private String generalInfo;

    @Column(name = "consumed_at", nullable = false)
    private Instant consumedAt;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    protected AuditJpaEntity() {
    }

    public AuditJpaEntity(String eventId, String deviceId, String eventCategory, String eventType,
                           Instant eventTimestamp, String generalInfo, Instant consumedAt, Instant createdAt) {
        this.eventId = eventId;
        this.deviceId = deviceId;
        this.eventCategory = eventCategory;
        this.eventType = eventType;
        this.eventTimestamp = eventTimestamp;
        this.generalInfo = generalInfo;
        this.consumedAt = consumedAt;
        this.createdAt = createdAt;
    }

    public Long getId() {
        return id;
    }

    public String getEventId() {
        return eventId;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public String getEventCategory() {
        return eventCategory;
    }

    public String getEventType() {
        return eventType;
    }

    public Instant getEventTimestamp() {
        return eventTimestamp;
    }

    public String getGeneralInfo() {
        return generalInfo;
    }

    public Instant getConsumedAt() {
        return consumedAt;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
