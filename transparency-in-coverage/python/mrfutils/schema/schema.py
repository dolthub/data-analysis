SCHEMA = {
    "file": [
        "id",
        "filename",
        "last_updated_on",
        "url",
    ],
    "insurer": [
        "id",
        "reporting_entity_name",
        "reporting_entity_type",
    ],
    "file_rate": [
        "file_id",
        "rate_id",
    ],
    "code": [
        "id",
        "billing_code_type_version",
        "billing_code",
        "billing_code_type",
    ],
    "price_metadata": [
        "id",
        "billing_class",
        "negotiated_type",
        "service_code",
        "expiration_date",
        "additional_information",
        "billing_code_modifier",
    ],
    "rate": [
        "id",
        "insurer_id",
        "code_id",
        "price_metadata_id",
        "negotiated_rate",
    ],
    "npi_rate": [
        "rate_id",
        "npi",
    ],
}
