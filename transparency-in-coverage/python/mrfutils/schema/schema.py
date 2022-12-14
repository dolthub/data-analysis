SCHEMA = {
    "plans": [
        "plan_hash",
        "reporting_entity_name",
        "reporting_entity_type",
        "plan_name",
        "plan_id",
        "plan_id_type",
        "plan_market_type",
        "last_updated_on",
        "version",
    ],
    "files": [
        "filename_hash",
        "filename",
        "url",
    ],
    "plans_files": [
        "plan_hash",
        "filename_hash",
    ],
    "codes": [
        "code_hash",
        # "negotiation_arrangement", # always ffs for now
        "billing_code_type_version",
        "billing_code",
        "billing_code_type",
    ],
    "prices": [
        "filename_hash",
        "code_hash",
        "price_hash",
        "billing_class",
        "negotiated_type",
        "service_code",
        "expiration_date",
        "additional_information",
        "billing_code_modifier",
        "negotiated_rate",
    ],
    "provider_groups": [
        "provider_group_hash",
        "tin_type",
        "tin_value",
        "npi_numbers",
    ],
    "prices_provider_groups": [
        "provider_group_hash",
        "price_hash",
    ]
    # "covered_services": [
    #     "root_hash_key",
    #     "in_network_hash_key",
    #     "billing_code_type_version",
    #     "description",
    #     "billing_code",
    #     "billing_code_type",
    #     "covered_services_hash_key",
    # ],
    # "bundled_codes": [
    #     "root_hash_key",
    #     "in_network_hash_key",
    #     "billing_code_type_version",
    #     "description",
    #     "billing_code",
    #     "billing_code_type",
    # ],
    # "negotiated_rates": [
    #     "root_hash_key",
    #     "in_network_hash_key",
    #     "negotiated_rates_hash_key",
    # ],
}
