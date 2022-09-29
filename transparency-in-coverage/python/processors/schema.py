SCHEMA = {
    'root':[
        'root_hash_id',
        'reporting_entity_name',
        'reporting_entity_type',
        'last_updated_on',  
        'version',
        'url',],

    'in_network':[
        'root_hash_id',
        'in_network_hash_id',
        'in_network.negotiation_arrangement',
        'in_network.name',
        'in_network.billing_code_type',
        'in_network.billing_code_type_version',
        'in_network.billing_code',
        'in_network.description',],

    'in_network.negotiated_rates':[
        'root_hash_id',
        'in_network_hash_id',
        'in_network.negotiated_rates_hash_id',
        'in_network.negotiated_rates.provider_references',],

    'in_network.negotiated_rates.negotiated_prices':[
        'root_hash_id',
        'in_network_hash_id',
        'in_network.negotiated_rates_hash_id',
        'in_network.negotiated_rates.negotiated_prices_hash_id',
        'in_network.negotiated_rates.negotiated_prices.negotiated_type',
        'in_network.negotiated_rates.negotiated_prices.negotiated_rate',
        'in_network.negotiated_rates.negotiated_prices.expiration_date',
        'in_network.negotiated_rates.negotiated_prices.service_code',
        'in_network.negotiated_rates.negotiated_prices.billing_class',
        'in_network.negotiated_rates.negotiated_prices.additional_information',
        'in_network.negotiated_rates.negotiated_prices.billing_code_modifier',],

    'in_network.negotiated_rates.provider_groups':[
        'root_hash_id',
        'in_network_hash_id',
        'in_network.negotiated_rates_hash_id',
        'in_network.negotiated_rates.provider_groups_hash_id',
        'in_network.negotiated_rates.provider_groups.npi',],

    'in_network.negotiated_rates.provider_groups.tin':[
        'root_hash_id',
        'in_network_hash_id',
        'in_network.negotiated_rates_hash_id',
        'in_network.negotiated_rates.provider_groups_hash_id',
        'in_network.negotiated_rates.provider_groups.tin_hash_id',
        'in_network.negotiated_rates.provider_groups.tin.type',
        'in_network.negotiated_rates.provider_groups.tin.value',],

    'provider_references':[
        'root_hash_id',
        'provider_references_hash_id',
        'provider_references.provider_group_id',],

    'provider_references.provider_groups':[
        'root_hash_id',
        'provider_references_hash_id',
        'provider_references.provider_groups_hash_id',
        'provider_references.provider_groups.npi',],

    'provider_references.provider_groups.tin':[
        'root_hash_id',
        'provider_references_hash_id',
        'provider_references.provider_groups_hash_id',
        'provider_references.provider_groups.tin_hash_id',
        'provider_references.provider_groups.tin.type',
        'provider_references.provider_groups.tin.value',]
}