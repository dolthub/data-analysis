SCHEMA = {
    "file": [
        "filename",
        "url",
    ],
    "insurer": [
        "id",
        "reporting_entity_name",
        "reporting_entity_type",
        "last_updated_on",
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
