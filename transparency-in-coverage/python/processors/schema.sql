CREATE TABLE IF NOT EXISTS root (
    root_hash BIGINT UNSIGNED,
    reporting_entity_name VARCHAR(500),
    reporting_entity_type VARCHAR(200),
    plan_name VARCHAR(200),
    plan_id VARCHAR(10),
    plan_id_type ENUM("enum", "hios") COLLATE utf8_general_ci,
    plan_market_type ENUM("group", "individual") COLLATE utf8_general_ci,
    last_updated_on VARCHAR(20),
    version VARCHAR(20),
    filename VARCHAR(2000),
    url VARCHAR(2000),
    PRIMARY KEY (root_hash)
);

CREATE TABLE IF NOT EXISTS codes (
    code_hash BIGINT UNSIGNED,
    negotiation_arrangement ENUM("ffs", "capitation") COLLATE utf8_general_ci,
    billing_code_type_version VARCHAR(20),
    billing_code VARCHAR(14),
    billing_code_type VARCHAR(8),
    PRIMARY KEY (code_hash)
);

CREATE TABLE IF NOT EXISTS negotiated_prices (
    root_hash BIGINT UNSIGNED NOT NULL,
    code_hash BIGINT UNSIGNED NOT NULL,
    negotiated_price_hash BIGINT UNSIGNED NOT NULL,
    billing_class ENUM("professional", "institutional") COLLATE utf8_general_ci,
    service_code JSON,
    expiration_date VARCHAR(20),
    additional_information TEXT,
    billing_code_modifier JSON,
    negotiated_rate DECIMAL(9,2),
    PRIMARY KEY (negotiated_price_hash, root_hash, code_hash),
    PRIMARY KEY (negotiated_price_hash),
    FOREIGN KEY (code_hash) REFERENCES codes(code_hash),
    FOREIGN KEY (root_hash) REFERENCES root(root_hash)
);

CREATE TABLE IF NOT EXISTS provider_groups (
    provider_group_hash BIGINT UNSIGNED,
    tin_type ENUM("EIN", "NPI", "ein", "npi"),
    tin_value VARCHAR(11),
    npi_numbers JSON,
    PRIMARY KEY (provider_group_hash)
);

CREATE TABLE IF NOT EXISTS provider_groups_negotiated_prices_link (
    negotiated_price_hash BIGINT UNSIGNED,
    provider_group_hash BIGINT UNSIGNED,
    PRIMARY KEY (negotiated_price_hash, provider_group_hash),
    FOREIGN KEY (negotiated_price_hash) REFERENCES negotiated_prices(negotiated_price_hash),
    FOREIGN KEY (provider_group_hash) REFERENCES provider_groups(provider_group_hash)
);
