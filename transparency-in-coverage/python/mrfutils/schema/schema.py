SCHEMA = {
    "file": [
        "id",
        "filename",
        "reporting_entity_name",
        "reporting_entity_type",
        "plan_name",
        "plan_id_type",
        "plan_id",
        "plan_market_type",
        "last_updated_on",
        "version",
        "url",
    ],
    "code": [
        "id",
        "billing_code_type_version",
        "billing_code",
        "billing_code_type",
    ],
    "rate_metadata": [
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
        "code_id",
        "rate_metadata_id",
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
    "toc": [
        "id",        
        "reporting_entity_name",
        "reporting_entity_type",
        "filename",
        "url",
    ],
    "toc_plan": [
        "id",
        "toc_id",
        # "toc_plan_file_link",
        "plan_id",
        "plan_name",
        "plan_id_type",
        "plan_market_type",
    ],  
    "toc_file": [
        "id",
        "toc_id",
        # "toc_plan_file_link",
        "description",
        "filename",
        "url",
    ],
    "toc_plan_file": [
        "link",
        "toc_plan_id",
        "toc_file_id",
    ],
}