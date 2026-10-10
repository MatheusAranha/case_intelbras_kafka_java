ALTER TABLE audit_records ADD COLUMN event_timestamp TIMESTAMP NOT NULL DEFAULT now();

CREATE INDEX idx_audit_records_event_timestamp ON audit_records (event_timestamp);
