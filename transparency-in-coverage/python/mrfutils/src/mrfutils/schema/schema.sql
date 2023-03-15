CREATE TABLE IF NOT EXISTS file (
    id BIGINT UNSIGNED,
    filename VARCHAR(1000) NOT NULL,
    last_updated_on VARCHAR(20) NOT NULL,
    reporting_entity_name VARCHAR(200) NOT NULL,
    reporting_entity_type VARCHAR(200) NOT NULL,
    plan_name VARCHAR(200),
    plan_id_type ENUM("ein", "hios") COLLATE utf8mb4_general_ci,
    plan_id VARCHAR(11),
    plan_market_type ENUM("group", "individual") COLLATE utf8mb4_general_ci,
    version VARCHAR(30),
    url TEXT NOT NULL,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS code (
    id BIGINT UNSIGNED,
    billing_code_type_version VARCHAR(20),
    billing_code VARCHAR(14),
    billing_code_type ENUM("CPT","HCPCS","ICD","MS-DRG","R-DRG","S-DRG","APS-DRG","AP-DRG","APR-DRG","APC","NDC","HIPPS","LOCAL","EAPG","CDT","RC","CSTM-ALL"),
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS rate_metadata (
    id BIGINT UNSIGNED,
    billing_class ENUM("professional", "institutional") COLLATE utf8mb4_general_ci,
--    negotiated_type ENUM("negotiated", "derived", "fee schedule", "percentage", "per diem") COLLATE utf8mb4_general_ci,
    negotiated_type ENUM("negotiated", "fee schedule") COLLATE utf8mb4_general_ci,
    service_code JSON,
    expiration_date VARCHAR(20),
    additional_information TEXT,
    billing_code_modifier JSON,
    PRIMARY KEY (id)
);

-- Theory: for a given insurer/code/price there is a unique rate, or a
-- small number of rates

CREATE TABLE IF NOT EXISTS rate (
    id BIGINT UNSIGNED,
    code_id BIGINT UNSIGNED,
    rate_metadata_id BIGINT UNSIGNED,
    negotiated_rate DECIMAL(9,2),
    PRIMARY KEY (id),
    FOREIGN KEY (code_id) REFERENCES code(id),
    FOREIGN KEY (rate_metadata_id) REFERENCES rate_metadata(id)
);

CREATE TABLE IF NOT EXISTS tin (
    id BIGINT UNSIGNED,
    tin_type ENUM("ein", "npi") COLLATE utf8mb4_general_ci,
    tin_value VARCHAR(11),
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS npi_tin (
    npi INT UNSIGNED,
    tin_id BIGINT UNSIGNED,
    PRIMARY KEY (npi, tin_id),
    FOREIGN KEY (tin_id) REFERENCES tin(id)
);

CREATE TABLE IF NOT EXISTS tin_rate_file (
    tin_id BIGINT UNSIGNED,
    rate_id BIGINT UNSIGNED,
    file_id BIGINT UNSIGNED,
    PRIMARY KEY (rate_id, tin_id),
    FOREIGN KEY (file_id) REFERENCES file(id),
    FOREIGN KEY (rate_id) REFERENCES rate(id),
    FOREIGN KEY (tin_id) REFERENCES tin(id)
);


-- for the index files

CREATE TABLE IF NOT EXISTS toc (
    id BIGINT UNSIGNED,
    reporting_entity_name VARCHAR(200) NOT NULL,
    reporting_entity_type VARCHAR(200) NOT NULL,
    filename VARCHAR(1000) NOT NULL,
    url TEXT,
    PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS toc_plan (
    id BIGINT UNSIGNED,
    toc_id BIGINT UNSIGNED,
    plan_id VARCHAR(20),
    plan_name VARCHAR(500),
    plan_id_type ENUM("ein", "hios") COLLATE utf8mb4_general_ci,
    plan_market_type ENUM("group", "individual") COLLATE utf8mb4_general_ci,
    PRIMARY KEY (id),
    FOREIGN KEY (toc_id) REFERENCES toc(id)
);

CREATE TABLE IF NOT EXISTS toc_file (
    id BIGINT UNSIGNED,
    toc_id BIGINT UNSIGNED,
    description TEXT,
    filename VARCHAR(1000),
    url TEXT,
    PRIMARY KEY (id),
    FOREIGN KEY (toc_id) REFERENCES toc(id)
);

CREATE TABLE IF NOT EXISTS toc_plan_file (
    link BIGINT UNSIGNED,
    toc_plan_id BIGINT UNSIGNED,
    toc_file_id BIGINT UNSIGNED,
    PRIMARY KEY (link, toc_plan_id, toc_file_id),
    FOREIGN KEY (toc_plan_id) REFERENCES toc_plan(id),
    FOREIGN KEY (toc_file_id) REFERENCES toc_file(id)
);