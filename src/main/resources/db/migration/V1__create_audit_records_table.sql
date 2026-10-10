CREATE TABLE audit_records (
    id BIGSERIAL PRIMARY KEY,
    event_id VARCHAR(255) NOT NULL,
    device_id VARCHAR(255) NOT NULL,
    event_category VARCHAR(50) NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    general_info TEXT NOT NULL,
    consumed_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now(),
    CONSTRAINT uq_audit_records_event_id UNIQUE (event_id)
);

CREATE INDEX idx_audit_records_device_id ON audit_records (device_id);
CREATE INDEX idx_audit_records_event_type ON audit_records (event_type);
