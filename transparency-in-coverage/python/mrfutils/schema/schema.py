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
        'plan_name',
	'plan_id_type',
	'plan_id',
	'plan_market_type',
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
    "tin": [
        "id",
        "tin_type",
        "tin_value",
    ],
    "tin_rate_file": [
        "tin_id",
        "rate_id",
        "file_id",
    ],
    "npi_tin": [
        "npi",
        "tin_id",
    ],
    "table_of_contents": [
        "plan_name",
        "plan_market_type",
        "plan_id_type",
        "plan_id",
        "file_id",
        "reporting_entity_name",
        "reporting_entity_type",
    ]
}
