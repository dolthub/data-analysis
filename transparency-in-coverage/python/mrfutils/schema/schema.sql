CREATE TABLE IF NOT EXISTS plans (
    plan_hash BIGINT UNSIGNED,
    reporting_entity_name VARCHAR(500),
    reporting_entity_type VARCHAR(200),
    plan_name VARCHAR(200),
    plan_id VARCHAR(10),
    plan_id_type ENUM("ein", "hios") COLLATE utf8mb4_general_ci,
    plan_market_type ENUM("group", "individual") COLLATE utf8mb4_general_ci,
    last_updated_on VARCHAR(20),
    version VARCHAR(20),
    PRIMARY KEY (plan_hash)
);

CREATE TABLE IF NOT EXISTS files (
    filename_hash BIGINT UNSIGNED,
    filename VARCHAR(500),
    url TEXT,
    PRIMARY KEY (filename_hash)
);

CREATE TABLE IF NOT EXISTS plans_files (
    plan_hash BIGINT UNSIGNED,
    filename_hash BIGINT UNSIGNED,
    PRIMARY KEY (plan_hash, filename_hash),
    FOREIGN KEY (plan_hash) REFERENCES plans(plan_hash),
    FOREIGN KEY (filename_hash) REFERENCES files(filename_hash)
);

CREATE TABLE IF NOT EXISTS codes (
    code_hash BIGINT UNSIGNED,
    billing_code_type_version VARCHAR(20),
    billing_code VARCHAR(14),
    billing_code_type ENUM("CPT","HCPCS","ICD","MS-DRG","R-DRG","S-DRG","APS-DRG","AP-DRG","APR-DRG","APC","NDC","HIPPS","LOCAL","EAPG","CDT","RC","CSTM-ALL"),
    PRIMARY KEY (code_hash)
);

CREATE TABLE IF NOT EXISTS metadata (
    metadata_hash BIGINT UNSIGNED NOT NULL,
    billing_class ENUM("professional", "institutional") COLLATE utf8mb4_general_ci,
    negotiated_type ENUM("negotiated", "derived", "fee schedule", "percentage", "per diem") COLLATE utf8mb4_general_ci,
    service_code JSON,
    expiration_date VARCHAR(20),
    additional_information TEXT,
    billing_code_modifier JSON,
    PRIMARY KEY (metadata_hash)
);

CREATE TABLE IF NOT EXISTS provider_groups (
    provider_group_hash BIGINT UNSIGNED,
    tin_type ENUM("ein", "npi") COLLATE utf8mb4_general_ci,
    tin_value VARCHAR(11),
    npi_numbers JSON,
    PRIMARY KEY (provider_group_hash)
);

CREATE TABLE IF NOT EXISTS prices (
    filename_hash BIGINT UNSIGNED NOT NULL,
    code_hash BIGINT UNSIGNED NOT NULL,
    provider_group_hash BIGINT UNSIGNED NOT NULL,
    metadata_hash BIGINT UNSIGNED NOT NULL,
    negotiated_rate DECIMAL(9,2),
    PRIMARY KEY (filename_hash, code_hash, provider_group_hash, metadata_hash, negotiated_rate),
    FOREIGN KEY (filename_hash) REFERENCES files(filename_hash),
    FOREIGN KEY (code_hash) REFERENCES codes(code_hash),
    FOREIGN KEY (provider_group_hash) REFERENCES provider_groups(provider_group_hash),
    FOREIGN KEY (metadata_hash) REFERENCES metadata(metadata_hash)
);