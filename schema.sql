CREATE TABLE IF NOT EXISTS m_factory ( 
    factoryId BIGINT AUTO_INCREMENT PRIMARY KEY,
    factory_name VARCHAR(100) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_factory_name (factory_name)
);

CREATE TABLE IF NOT EXISTS m_machine (
    machineId BIGINT AUTO_INCREMENT PRIMARY KEY,
    factoryId BIGINT NOT NULL,
    machine_code VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_machine_code (machine_code),
    KEY idx_machine_factory (factoryId),
    CONSTRAINT fk_machine_factory
        FOREIGN KEY (factoryId) REFERENCES m_factory(factoryId)
);

CREATE TABLE IF NOT EXISTS t_order_number (
    orderId BIGINT AUTO_INCREMENT PRIMARY KEY,
    order_no VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_order_no (order_no)
);

CREATE TABLE IF NOT EXISTS downtime_reason_master (
    code VARCHAR(50) PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    is_active TINYINT(1) NOT NULL DEFAULT 1,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS t_downtime_events (
    downtimeId BIGINT AUTO_INCREMENT PRIMARY KEY,
    machineId BIGINT NOT NULL,
    orderId BIGINT NULL,
    startTime DATETIME(6) NOT NULL,
    endTime DATETIME(6) NOT NULL,
    duration_min DECIMAL(10,2) NOT NULL,
    event VARCHAR(50) NOT NULL,
    reason_code VARCHAR(50) NULL,
    reason VARCHAR(255) NULL,
    source VARCHAR(20) NOT NULL DEFAULT 'influx',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uniq_downtime_event (machineId, startTime),
    KEY idx_downtime_machine_time (machineId, startTime),
    KEY idx_downtime_order_time (orderId, startTime),
    CONSTRAINT fk_downtime_machine
        FOREIGN KEY (machineId) REFERENCES m_machine(machineId),
    CONSTRAINT fk_downtime_order
        FOREIGN KEY (orderId) REFERENCES t_order_number(orderId),
    CONSTRAINT fk_downtime_reason
        FOREIGN KEY (reason_code) REFERENCES downtime_reason_master(code)
);

INSERT INTO downtime_reason_master (code, name)
VALUES
    ('MAT', 'Material shortage'),
    ('JAM', 'Machine jam'),
    ('SETUP', 'Setup / Changeover'),
    ('QC', 'Quality check'),
    ('PM', 'Maintenance'),
    ('BREAK', 'Operator break')
ON DUPLICATE KEY UPDATE
    name = VALUES(name),
    is_active = 1,
    updated_at = CURRENT_TIMESTAMP;
