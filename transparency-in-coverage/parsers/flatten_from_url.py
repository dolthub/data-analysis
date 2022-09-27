"""
Open questions:

1. How to make this async?
2. Is there a better way to scan through the file? Current method:
    - Check if there are codes matching our billing_code_list
    - If so, get the provider references
    - Seek to the beginning of file
    - Write the front matter
    - Then write the matching provider references
    But this seems to work pretty slowly

"""

import ijson
import json
import os
import csv
import glob
# import requests
import gzip
import time
import hashlib
from tqdm import tqdm
import io

# not clear if this saves time or not
import httpio # for seeking through streamed files

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


def write_dict_to_file(output_dir, filename, data):
    """Write dictionary to one of the files
    defined in the schema
    """
    
    file_loc = f'{output_dir}/{filename}.csv'

    fieldnames = SCHEMA[filename]
    
    if not os.path.exists(file_loc):
        with open(file_loc, 'w') as f:
            writer = csv.DictWriter(f, fieldnames = fieldnames)
            writer.writeheader()
            writer.writerow(data)
            return
    
    with open(file_loc, 'a') as f:
        writer = csv.DictWriter(f, fieldnames = fieldnames)
        writer.writerow(data)
        return


def flatten_to_file(obj, output_dir, prefix = '', **hash_ids):
    """Takes an object, turns it into a dict, and 
    writes it to file.

    We have to track the hash_ids. This requires us to loop
    through the dict once to take out the plain values,
    then loop through it again to take care of the nested 
    dicts, while passing the hash_ids down as a param.
    """

    data = {}

    for key, value in obj.items():
        
        key_id = f'{prefix}.{key}' if prefix else key

        plain_value = False

        if type(value) in [str, int, float]:
            plain_value = True

        elif type(value) == list and len(value) == 0:
            plain_value = True

        elif type(value) == list:
            if type(value[0]) in [str, int, float]:
                plain_value = True
        
        if plain_value:
            data[key_id] = value

    hash_ids[f'{prefix}_hash_id'] = hashdict(data)

    for key, value in hash_ids.items():
        data[key] = value

    for key, value in obj.items():

        key_id = f'{prefix}.{key}' if prefix else key

        dict_value = False

        if type(value) == list and value:
            if type(value[0]) in [dict]:
                dict_value = True

        if dict_value:
            for subvalue in value:
                flatten_to_file(subvalue, output_dir, key_id, **hash_ids)
                   
    write_dict_to_file(output_dir, prefix, data)


def hashdict(data_dict):
    """Get the hash of a dict (sort, convert to bytes, then hash)
    """
    sorted_dict = dict(sorted(data_dict.items()))
    return hashlib.md5(json.dumps(sorted_dict).encode('utf-8')).hexdigest()


def parse_to_file(url, billing_code_list, output_dir, overwrite = False):
    
    if os.path.exists(output_dir):
        if overwrite:
            for file in glob.glob(f'{output_dir}/*'):
                os.remove(file)
    else:
        os.mkdir(output_dir)


    print(f'Streaming from remote URL: {url}\n')
    with httpio.open(url, block_size = 2048) as r:
        # buf = io.BytesIO(r.read())

        f = gzip.GzipFile(fileobj = r) # buf)

        data = {}
        hash_ids = {}

        parser = ijson.parse(f, use_float = True)

        for prefix, event, value in parser:
            if event in ['string', 'number']:
                data[f'{prefix}'] = value

            if event == 'start_array':
                break

        data['url'] = url
        data['root_hash_id'] = hashdict(data)

        hash_ids['root_hash_id'] = data['root_hash_id']

        provider_references_list = []
        codes_found = False

        objs = ijson.items(parser, 'in_network.item', use_float = True)
        
        for obj in objs:

            # Loop through objects
            if obj['billing_code'] in billing_code_list:
                codes_found = True

                # Write the object
                flatten_to_file(obj, output_dir, prefix = 'in_network', **hash_ids)

                for negotiated_rate in obj['negotiated_rates']:
                    for provider_reference in negotiated_rate['provider_references']:
                        provider_references_list.append(provider_reference)
                        
        if not codes_found:
            return

        write_dict_to_file(output_dir, 'root', data)
        
        f.seek(0)

        objs = ijson.items(f, 'provider_references.item', use_float = True)

        s = time.time()
        for obj in objs:
            if obj['provider_group_id'] in provider_references_list:
                pass
                flatten_to_file(obj, output_dir, prefix = 'provider_references', **hash_ids)

# EXAMPLE usage

urls = ['https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company_Insurer_NFP_CSP-952-T134_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_NFP_CSP-952-T134_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_General-Electric---Kaia_CSP-980-C990_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Target_CSP-930-T171_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_The-George-Washington-University_CSP-979-T190_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Target_CSP-902-T181_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Presbyterian-Homes-of-IL_CSP-982-T189_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Watermark-Retirement-Communities_CSP-902-T166_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_BNP-Paribas_CSP-912-T139_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Laboratory-Corporation-of-America_GSP-952-C301_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Siemens-Energy--Inc-_CSP-971-C984_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Siemens-Healthineers_CSP-971-C984_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Freeport-McMoRan_CSP-954-T170_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Memorial-Sloan-Kettering--MSK-_GSP-970-C926_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Caring-Place-Health-Care-Group_CSP-912-T141_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_State-of-Arizona_CSP-979-C980_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Corning_CSP-982-C995_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Alcatel-Lucent-USA--Inc--_GSP-913-T015_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_General-Electric---Active-Employees_CSP-913-T015_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_City-of-Hialeah_CSP-989-T130_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_General-Electric---Summit-Health_CSP-915-C345_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_City-of-Baytown_CSP-930-C915_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Travis-County-Gumto_CSP-902-C193_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Turner-Corporation_CSP-910-C624_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_PricewaterhouseCoopers_CSP-911-C344_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Methodist-Health-System-Dallas_CSP-987-C816_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Winstead_CSP-903-C746_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_American-Postal-Workers-Union_GSP-959-C861_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Baylor-College-of-Medicine_CSP-983-T156_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_UNUM_CSP-903-C953_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Tishman-Speyer_CSP-966-C922_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Hubbell-Incorporated_CSP-996-C881_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_PricewaterhouseCoopers_CSP-944-C335_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Credit-Suisse_CSP-958-C378_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Credit-Suisse_CSP-958-C378_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Travis-County_CSP-850-MT84_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Valero-Services--Inc_CSP-916-C623_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Reyes-Holdings-LLC_CSP-913-T177_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_MolsonCoors_CSP-951-C814_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_MolsonCoors_CSP-950-C436_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Berry-Global_CSP-906-C673_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_BCD-Travel-Industries-Group_CSP-819-C429_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Keller-ISD---HSA-Plan_CSP-803-C765_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company_Insurer_Sitel-EosHealth_CSP-905-C181_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_General-Electric---REVA_CSP-985-C797_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Insurer_HML-16_SA_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Sitel-EosHealth_CSP-905-C181_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_New-York-University-_CSP-943-C865_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_John-Hancock_CSP-959-C958_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_General-Dynamics-aka-Electric-Boat_CSP-909-C738_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Synopsys_CSP-932-C536_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_General-Electric---Health-Solutions-Service_CSP-910-C400_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Pilot-Flying-J_CSP-932-C555_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company_Insurer_Cisco-Systems_CSP-952-C932_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Apple_CSP-954-C955_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Cisco-Systems_CSP-952-C932_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Intuit_CSP-921-C476_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_MolsonCoors_CSP-938-C431_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Archdiocese-of-New-York_CSP-935-C248_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Washington-Suburban-Sanitary-Commission--WSSC-_CSP-907-C826_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Intuit_CSP-800-C745_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Pfizer_CSP-911-C706_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_A-O-Smith_CSP-930-C340_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Princeton-University_CSP-909-C954_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Oak-Ridge-National-Lab-_CSP-914-C947_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Oak-Ridge-National-Lab-_CSP-913-C946_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Oak-Ridge-National-Lab-_CSP-911-C817_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Oak-Ridge-National-Lab-_CSP-912-C818_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Oak-Ridge-National-Lab-_CSP-915-C948_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_The-School-District-of-Escambia-County-_CSP-916-C351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Travelers_CSP-940-C250_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_HCA-Foundation-One_CSP-912-C764_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Acushnet_CSP-954-C943_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_H-T--Hackney_CSP-901-C843_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Shell-Oil-Company_CSP-907-C877_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Washington-University_GSP-911-C420_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Blackrock_CSP-809-C828_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Blackrock_CSP-810-C829_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Baylor-College-of-Medicine_CSP-909-C283_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Town-of-Oro-Valley_CSP-904-C607_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_USANA_CSP-905-C979_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Soft-Computer-Consultants_CSP-986-C885_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Shell-Oil-Company_CSP-907-C179_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_City-of-Jacksonville_CSP-914-T153_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_HCA-Doctors-on-Demand_CSP-945-C874_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Morgan-Stanley---Nina-Chopra_CSP-907-C653_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Methodist-Health-System-Dallas_GSP-928-C067_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Qualcomm_CSP-807-C755_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Motiva_CSP-950-C700_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company_Insurer_Cisco-Systems_CSP-907-MD29_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_MAMSI-Life_Insurer_OBPM---Optum-Bundle-Payment_OBPM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Cisco-Systems_CSP-907-MD29_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_Optimum-Choice--Inc-_Insurer_OBPM---Optum-Bundle-Payment_OBPM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Utah--Inc-_Insurer_OBPM---Optum-Bundle-Payment_OBPM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Ohio--Inc-_Insurer_OBPM---Optum-Bundle-Payment_OBPM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Texas--Inc-_Insurer_OBPM---Optum-Bundle-Payment_OBPM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Insurer_OBPM---Optum-Bundle-Payment_OBPM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Oregon--Inc-_Insurer_OBPM---Optum-Bundle-Payment_OBPM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Arizona--Inc-_Insurer_OBPM---Optum-Bundle-Payment_OBPM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Florida--Inc-_Insurer_OBPM---Optum-Bundle-Payment_OBPM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Georgia--Inc-_Insurer_OBPM---Optum-Bundle-Payment_OBPM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Oregon--Inc--_Insurer_OBPM---Optum-Bundle-Payment_OBPM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company_Insurer_OBPM---Optum-Bundle-Payment_OBPM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Alabama--Inc-_Insurer_OBPM---Optum-Bundle-Payment_OBPM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Illinois--Inc-_Insurer_OBPM---Optum-Bundle-Payment_OBPM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Arkansas--Inc-_Insurer_OBPM---Optum-Bundle-Payment_OBPM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Colorado--Inc-_Insurer_OBPM---Optum-Bundle-Payment_OBPM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Kentucky--Ltd-_Insurer_OBPM---Optum-Bundle-Payment_OBPM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Louisiana--Inc-_Insurer_OBPM---Optum-Bundle-Payment_OBPM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Oklahoma--Inc-_Insurer_OBPM---Optum-Bundle-Payment_OBPM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Wisconsin--Inc-_Insurer_OBPM---Optum-Bundle-Payment_OBPM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Washington--Inc-_Insurer_OBPM---Optum-Bundle-Payment_OBPM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Mississippi--Inc-_Insurer_OBPM---Optum-Bundle-Payment_OBPM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-New-Mexico--Inc-_Insurer_OBPM---Optum-Bundle-Payment_OBPM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_Neighborhood-Health-Partnership--Inc_Insurer_OBPM---Optum-Bundle-Payment_OBPM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Benefits-Plan-of-CA_Insurer_OBPM---Optum-Bundle-Payment_OBPM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Community-Plan--Inc-_Insurer_OBPM---Optum-Bundle-Payment_OBPM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-New-England--Inc-_Insurer_OBPM---Optum-Bundle-Payment_OBPM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Pennsylvania--Inc-_Insurer_OBPM---Optum-Bundle-Payment_OBPM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-the-Midlands--Inc-_Insurer_OBPM---Optum-Bundle-Payment_OBPM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_Neighborhood-Health-Partnership--Inc-_Insurer_OBPM---Optum-Bundle-Payment_OBPM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Life-Insurance-Company_Insurer_OBPM---Optum-Bundle-Payment_OBPM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-South-Carolina--Inc-_Insurer_OBPM---Optum-Bundle-Payment_OBPM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-North-Carolina--Inc-_Insurer_OBPM---Optum-Bundle-Payment_OBPM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-the-Mid-Atlantic--Inc-_Insurer_OBPM---Optum-Bundle-Payment_OBPM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Benefits-Plan-of-California_Insurer_OBPM---Optum-Bundle-Payment_OBPM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-Illinois_Insurer_OBPM---Optum-Bundle-Payment_OBPM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company_Insurer_Cisco-Systems-North-Carolina_CSP-917-C488_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_OBPM---Optum-Bundle-Payment_OBPM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Plan-of-the-River-Valley--Inc-_Insurer_OBPM---Optum-Bundle-Payment_OBPM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_OBPM---Optum-Bundle-Payment_OBPM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-the-River-Valley_Insurer_OBPM---Optum-Bundle-Payment_OBPM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Cisco-Systems-North-Carolina_CSP-917-C488_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_Optimum-Choice--Inc--and-MAMSI-Life-and-Health-Insurance-Company_Insurer_OBPM---Optum-Bundle-Payment_OBPM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Qualcomm_GSP-912-C353_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company_Insurer_Cisco-Systems_CSP-925-C956_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Cisco-Systems_CSP-925-C956_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company_Insurer_Columbia-University_CSP-992-C462_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Columbia-University_CSP-992-C462_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UMR--Inc-_TPA_REAL-APPEAL_PHCS-HD-TRAVEL-WITH-BENCHMARK-NETWORKS_-RA_0CQM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UMR--Inc-_TPA_REAL-APPEAL_THE-ALLIANCE_-RA_WOG_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UMR--Inc-_TPA_VIRTA-HEALTH-DIRECT-CONTRACT_HST_-VIR_3HSO_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UMR--Inc-_TPA_ARCELOR-MITTAL-DIALYSIS-CONTRACT_PHCS-PPO_R-_XM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UMR--Inc-_TPA_RELYMD_ZELIS-RBP_-MID_2HZL_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UMR--Inc-_TPA_FIRST-HEALTH_1H12_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company_Insurer_Maxar-Technologies-Holdings_CSP-913-C491_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company_Insurer_Cisco-Systems---Summit-Health_CSP-908-C214_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_3form--LLC-_CSP-902-C454_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_FiServ_CSP-910-C561_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_AbbVie_CSP-918-T079_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_NXP-USA_CSP-821-C941_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company_Insurer_University-of-Miami-Student-Health-Plan_GSP-901-T004_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Service-Express_CSP-903-C919_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Sabre_CSP-912-T142_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company_Insurer_Splunk_CSP-907-T174_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_City-of-Orlando_CSP-918-C977_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_City-of-Baytown_CSP-943-C981_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Sedgwick-County-_CSP-902-C929_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Salesforce--Inc_CSP-889-C890_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_WEC-Energy-Group_CSP-801-C660_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Fields-Auto-Group_CSP-935-C358_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_City-of-Round-Rock_CSP-916-T172_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Waupaca-Foundry--Inc_CSP-906-T160_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Ameriprise-Financial_CSP-928-C698_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Wisconsin-Health-Fund_CSP-909-MT10_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Tractor-Supply-Company_CSP-906-T205_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_WMATA_CSP-902-C983_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_City-of-Denton_CSP-950-C403_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Huhtamaki--Inc-_CSP-907-C875_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Maxar-Technologies-Holdings_CSP-913-C491_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Kohler_CSP-980-C606_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Miami-Beach-FOP-Health-Trust_CSP-914-C198_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Cisco-Systems---Summit-Health_CSP-908-C214_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Empower-Retirement_CSP-980-C522_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Gulliver-Schools--Inc-_CSP-939-C477_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_J-J--Keller_CSP-909-T201_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Wieden---Kennedy-Inc_CSP-901-C329_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Travis-County_CSP-901-MD37_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Florida-Municipal-Insurance-Trust-_CSP-921-C565_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Southern-Star-Central-Gas-Pipeline_CSP-901-C669_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Reyes-Holdings-LLC_CSP-959-T118_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Unified-Government-of-Wyandotte-County_CSP-983-T101_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Lower-Colorado-River-Authority_CSP-910-C939_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Citrus-County-Sheriff-s-Office_CSP-938-T182_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Goldman-Sachs_CSP-928-MD35_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Adams-County-Government_CSP-921-C889_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Splunk_CSP-907-T174_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_AbbVie_CSP-979-T186_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company_Insurer_Liberty-Mutual_CSP-809-C657_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Marsh---McLennan_CSP-906-T164_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Textron_CSP-925-C896_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Racine-County_CSP-976-T103_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company_Insurer_Eaton-Corporation_CSP-912-C872_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_City-of-Racine_CSP-976-T103_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Nephron-Pharmaceuticals-Corporation_CSP-902-T198_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_School-District-of-South-Milwaukee_CSP-997-T192_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Hearst_CSP-908-T126_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Hitachi-Astemo-Ohio-Manufacturing_CSP-906-C374_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Racine-Unified-School-District_CSP-976-T103_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Cardinal-Health_CSP-908-C925_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_General-Dynamics-aka-Electric-Boat_CSP-904-C870_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UMR--Inc-_TPA_WEST-BEND-MUTUAL-DIRECT-CONTRACTS_THE-ALLIANCE_PHCS-HD-TRAVEL-WITH-BENCHMARK-NETWORKS_-WBQ_WO_0CQM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Liberty-Mutual_CSP-809-C657_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Eaton-Corporation_CSP-912-C872_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Harley-Davidson_CSP-932-C336_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Dimensional-Investments-_CSP-914-C747_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Corning---Canton-Potsdam_CSP-902-C373_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_City-of-Tampa_CSP-904-C158_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_WPP-Group-USA_CSP-920-C853_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company_Insurer_Transforce_CSP-921-T125_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Florida--Inc-_Insurer_Tampa-Firefighters-and-Police-Officers-Benefit-Plan_CSP-904-C158_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company_Insurer_Tampa-Firefighters-and-Police-Officers-Benefit-Plan_CSP-904-C158_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Itron-Inc_CSP-932-C944_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_Neighborhood-Health-Partnership--Inc_Insurer_Tampa-Firefighters-and-Police-Officers-Benefit-Plan_CSP-904-C158_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Southern-Star-Central-Gas-Pipeline_CSP-921-T184_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company_Insurer_Samaritan-s-Purse_CSP-901-C928_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company_Insurer_Furest-Group-dba-KEEN_CSP-907-T179_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_YMCA-Employee-Benefits_CSP-942-C911_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Caesars-Enterprise-Services_CSP-976-T168_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Morgan-Stanley_CSP-902-C205_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Children-s-Hospital-of-Colorado_CSP-928-T169_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Apex-Tool-Group_CSP-935-C303_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Essity-North-America_CSP-904-C678_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Morgan-Stanley_CSP-905-C046_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company_Insurer_Genworth-Financial_CSP-907-C654_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_PNC-Financial-Services_CSP-912-C480_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Apple_CSP-911-C207_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_DENSO-Personnel-Service-America_CSP-904-T133_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Canton-Town-and-BOE_CSP-906-T203_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Insurer_Limeade_CSP-995-T175_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Sandia-National-Labs_CSP-903-C131_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Ameriprise-Financial_CSP-902-C189_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UMR--Inc-_TPA_AETNA_1H6W_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UMR--Inc-_TPA_FIRST-NATIONAL-BANK-ALASKA-DCI_BEECHSTREET-PPO-ALASKA---NEVADA-WITH-PHCS-TRAVEL_6-_T0_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Fresenius_CSP-906-C656_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Scholastic-Inc-_CSP-946-C414_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Sandia-National-Labs_CSP-989-T127_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Brown---Brown_CSP-901-C952_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Paypal_CSP-806-C682_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Rockline-Industries_CSP-982-T104_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Paypal_CSP-941-C732_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Qualcomm---Premier-ACO_CSP-934-C752_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_US-Airways-Group_CSP-962-C574_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_UnitedHealth-Group---SSMH_CSP-904-T187_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_New-York-Downtown-Hospital_GSP-906-C030_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_General-Dynamics-aka-Electric-Boat_CSP-900-MD56_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Indian-Prairie-School-District_CSP-902-T113_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_TD-Industries_CSP-951-C858_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_The-George-Washington-University_CSP-905-C137_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Hospital-for-Special-Surgery_GSP-914-C878_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Jones-Lang-Lasalle_CSP-985-C951_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company_Insurer_Columbia-University_GSP-921-C259_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Columbia-University_GSP-921-C259_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_NYU-Langone-Health-System_CSP-900-C020_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Ariens-Company_CSP-904-C774_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company_Insurer_Eaton-Corporation_CSP-963-C873_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Eaton-Corporation_CSP-963-C873_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_HCA-Vanderbuilt_CSP-900-C756_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Brookdale-Senior-Living_CSP-944-MD30_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Goldman-Sachs_CSP-900-T073_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_FedEx-Corporation_CSP-931-C997_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_FedEx-Corporation_CSP-932-C998_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Saint-Louis-University_GSP-901-MD47_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UMR--Inc-_TPA_CNIC-DCI-WITH-UNITEDHEALTHCARE-NBR_WESTERN-HEALTHCARE-ALLIANCE-W-ROCKY-MT-HEALTH_PHCS-HD-TRAVEL-WITH_-CNC_WRM_0CQM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UMR--Inc-_TPA_UNITEDHEALTHCARE-NEW-BUSINESS-RATE-DCI_-HTS_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UMR--Inc-_TPA_VAIL-RESORTS-DIRECTS-WITH-UNITEDHEALTHCARE-NBR_FIRST-CHOICE-MIDWEST_PHCS-HD-TRAVEL-WITH-BENCHMARK-NE_-V_EJ_0CQM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Dinsmore---Shohl_GSP-811-C775_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_HCA-Alternate-Facilities_CSP-942-C139_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Erie-Insurance_CSP-907-C554_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire-MPN_CSP-377-A350_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Insurer_HML-13_S3_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Oklahoma--Inc-_Insurer_HML-13_S3_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_City-of-Baytown-Altus_CSP-930-C395_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-California_INSURER_VEBA-Perform-HMO-Network-3_U5_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-California_INSURER_VEBA-Perform-HMO-Network-2_U4_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UHC-of-Oklahoma_INSURER_FULL-OR-HMO-NETWORK_FL_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-California_INSURER_FULL-OR-HMO-NETWORK_FL_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Oregon-Inc-_INSURER_FULL-OR-HMO-NETWORK_FL_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-California_INSURER_VEBA-Perform-HMO-Network-1_U3_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-California_INSURER_SV-Focus_SE_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-California_INSURER_CA-VEBA-ALLIANCE-HMO_UA_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-California_INSURER_CALPERS-SV-ALLIANCE_P5_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-California_INSURER_Signature-Value-Alliance_SA_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-California_INSURER_VALUE-NETWORK_VA_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-California_INSURER_SignatureValue-Harmony_SJ_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire-MPN_CSP-1-A350_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Delta-Airlines_CSP-938-T114_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_Oxford-Health-Insurance--Inc-_Insurer_OPH---Optum-Physical-Health_OPH-160_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_Oxford-Health-Plans--CT---Inc-_Insurer_OPH---Optum-Physical-Health_OPH-160_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_Oxford-Health-Plans--NJ---Inc-_Insurer_OPH---Optum-Physical-Health_OPH-160_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UMR--Inc-_TPA_DEXTER-APACHE-DIRECT-CONTRACT-DCI_PHCS-PPO_-DAH_7L_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UMR--Inc-_TPA_WOODWARD-COMMUNICATION-DCI_THE-ALLIANCE_PHCS-HD-FIRST-HEALTH-SHARED-SAVINGS_-G_WO_0CRL_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UMR--Inc-_TPA_CNIC-DCI-WITH-UNITEDHEALTHCARE-NBR_FIRST-CHOICE-MIDWEST_PHCS-HEALTHY-DIRECTIONS-MULTIPLAN-CH-TC3-_-CNC_EJ_C1N_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UMR--Inc-_TPA_DEXTER-APACHE-DIRECT-CONTRACT-DCI_ALLIANCE-WITH-QUALITY-PATH-PRODUCT_FIRST-HEALTH-TRAVEL-AND-MULTIPL_-DAH_ALQP_MD_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UMR--Inc-_TPA_DUKE-UNIVERSITY_Z8A_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UMR--Inc-_TPA_LAB-CARD_INTERWEST-HEALTH_PHCS-HD-TRAVEL-WITH-BENCHMARK-NETWORKS_-L_E3_0CQM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UMR--Inc-_TPA_LAB-CARD---HEALTH-COST-CONTROL-DCI_THE-CARE-NETWORK_-LHC_TCN_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UMR--Inc-_TPA_ARCHDIOCESE-OF-DENVER-WELFARE-BENEFITS-TRUST_ADCC_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UMR--Inc-_TPA_DOD-RA-WITH-UNITEDHEALTHCARE-NBR-DCI_COMPASS-ROSE-RETIREES-PLAN-C_-DOB_QRC_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Walgreens_CSP-907-C317_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_MAMSI-Life_Insurer_UHC---Embedded-Vision_UHC-Vision_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_Rocky-Mountain-HMO_Insurer_UHC---Embedded-Vision_UHC-Vision_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_Oxford-Health-Plans--Inc-_Insurer_UHC---Embedded-Vision_UHC-Vision_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_Oxford-Health-Insurance--Inc-_Insurer_UHC---Embedded-Vision_UHC-Vision_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_ALL-SAVERS-INSURANCE-COMPANY_Insurer_UHC---Embedded-Vision_UHC-Vision_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_Oxford-Health-Plans--CT---Inc-_Insurer_UHC---Embedded-Vision_UHC-Vision_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_Oxford-Health-Plans--NJ---Inc-_Insurer_UHC---Embedded-Vision_UHC-Vision_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Washington--Inc-_Insurer_UHC---Embedded-Vision_UHC-Vision_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Benefits-Plan-of-CA_Insurer_UHC---Embedded-Vision_UHC-Vision_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Life-Insurance-Company_Insurer_UHC---Embedded-Vision_UHC-Vision_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Freedom-Insurance-Company_Insurer_UHC---Embedded-Vision_UHC-Vision_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-Illinois_Insurer_UHC---Embedded-Vision_UHC-Vision_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_UHC---Embedded-Vision_UHC-Vision_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Plan-of-the-River-Valley--Inc-_Insurer_UHC---Embedded-Vision_UHC-Vision_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-the-River-Valley_Insurer_UHC---Embedded-Vision_UHC-Vision_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_Optimum-Choice-Inc-_Insurer_UHC---Embedded-Vision_UHC-Vision_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_Optimum-Choice--Inc-_Insurer_UHC---Embedded-Vision_UHC-Vision_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Texas--Inc-_Insurer_UHC---Embedded-Vision_UHC-Vision_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Oregon--Inc-_Insurer_UHC---Embedded-Vision_UHC-Vision_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Arizona--Inc-_Insurer_UHC---Embedded-Vision_UHC-Vision_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Florida--Inc-_Insurer_UHC---Embedded-Vision_UHC-Vision_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Georgia--Inc-_Insurer_UHC---Embedded-Vision_UHC-Vision_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company_Insurer_UHC---Embedded-Vision_UHC-Vision_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Illinois--Inc-_Insurer_UHC---Embedded-Vision_UHC-Vision_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-Healthcare-Insurance-Company_Insurer_UHC---Embedded-Vision_UHC-Vision_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Oklahoma--Inc-_Insurer_UHC---Embedded-Vision_UHC-Vision_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Community-Plan--Inc-_Insurer_UHC---Embedded-Vision_UHC-Vision_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-North-Carolina--Inc-_Insurer_UHC---Embedded-Vision_UHC-Vision_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_Optimum-Choice--Inc-_Insurer_Galileo-Provider-Network_GLNETWORK_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Texas--Inc-_Insurer_Galileo-Provider-Network_GLNETWORK_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Florida--Inc-_Insurer_Galileo-Provider-Network_GLNETWORK_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Georgia--Inc-_Insurer_Galileo-Provider-Network_GLNETWORK_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Illinois--Inc-_Insurer_Galileo-Provider-Network_GLNETWORK_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_MolsonCoors_CSP-904-MD87_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-696-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_Neighborhood-Health-Partnership--Inc-_Insurer_OPH-Optum-Physical-Health_OPH-195_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_Neighborhood-Health-Partnership--Inc_Insurer_OPH---Optum-Physical-Health_OPH-195_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_General-Electric---HCN_CSP-916-T086_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_City-of-McAllen_CSP-962-C503_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_Oxford-Health-Insurance--Inc-_Insurer_Metro-Network_10_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_Oxford-Health-Plans-LLC_Third-Party-Administrator_Metro-Network_10_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UMR--Inc-_TPA_MONUMENT-HEALTH-UNITEDHEALTHCARE-NEW-BUSINESS-RATE_ARCHDIOCESE-OF-DENVER-WELFARE-BENEFITS-TRUST_-MNH_ADCC_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Sandia-National-Labs_CSP-911-C835_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-863-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UMR--Inc-_TPA_CNIC-DCI-WITH-UNITEDHEALTHCARE-NBR_WISE_-CNC_WIS4_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_Optimum-Choice-Inc-_Insurer_National-Ancillary-Network_NANETWORK_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_Optimum-Choice--Inc-_Insurer_National-Ancillary-Network_NANETWORK_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Texas--Inc-_Insurer_National-Ancillary-Network_NANETWORK_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Oregon--Inc-_Insurer_National-Ancillary-Network_NANETWORK_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Arizona--Inc-_Insurer_National-Ancillary-Network_NANETWORK_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Florida--Inc-_Insurer_National-Ancillary-Network_NANETWORK_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Georgia--Inc-_Insurer_National-Ancillary-Network_NANETWORK_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company_Insurer_National-Ancillary-Network_NANETWORK_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Illinois--Inc-_Insurer_National-Ancillary-Network_NANETWORK_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-Healthcare-Insurance-Company_Insurer_National-Ancillary-Network_NANETWORK_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Oklahoma--Inc-_Insurer_National-Ancillary-Network_NANETWORK_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Community-Plan--Inc-_Insurer_National-Ancillary-Network_NANETWORK_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-North-Carolina--Inc-_Insurer_National-Ancillary-Network_NANETWORK_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UMR--Inc-_TPA_AMERICAN-WELL-DCI---POMCO_COUNTY-OF-STANISLAUS---PPO_-AW_ASNT_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Travis-County_GSP-905-MD05_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Guilford-County_GSP-904-C243_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_State-of-Arizona_GSP-952-C908_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-926-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-952-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Baylor-College-of-Medicine_GSP-992-C368_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Meharry-Medical-College_GSP-901-T084_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-429-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Murphy-USA_CSP-950-T135_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-962-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Hospital-for-Special-Surgery_CSP-917-MD58_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_HCA---IUC_CSP-903-T105_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_City-of-Lakeland_CSP-932-C694_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company_Insurer_LaClinica_GSP-904-C776_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UMR--Inc-_TPA_CNIC-DCI-WITH-UNITEDHEALTHCARE-NBR_CENTURA-NARROW-WITH-ROCKY-MOUNTAIN-HEALTH-PLANS_PHCS-HD-TRAVEL-WI_-CNC_RMCN_0CQM_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-262-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Goldman-Sachs_CSP-954-C533_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Qualcomm_CSP-921-C279_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-904-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_ICC-Industries_CSP-910-C757_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-442-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-950-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-916-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-967-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Methodist-Health-System-Dallas_GSP-914-C106_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-919-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-953-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-959-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-946-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-948-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-911-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Empower-Retirement_CSP-935-T199_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-917-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-947-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-957-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-872-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-964-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-944-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-942-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-949-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-951-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-922-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-943-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-955-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-774-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-914-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-925-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-956-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-915-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company_Insurer_Cisco-Systems_CSP-935-C711_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Cisco-Systems_CSP-935-C711_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-862-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-945-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-903-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-920-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-968-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-966-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-791-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-941-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_Oxford-Health-Insurance--Inc-_Insurer_Liberty-Network_9_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_Oxford-Health-Plans--CT---Inc-_Insurer_Liberty-Network_9_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_Oxford-Health-Plans--NJ---Inc-_Insurer_Liberty-Network_9_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_Oxford-Health-Plans-LLC_Third-Party-Administrator_Liberty-Network_9_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-961-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-927-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-912-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-923-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_Oxford-Health-Insurance--Inc-_Insurer_Freedom-Network_7_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_Oxford-Health-Plans--CT---Inc-_Insurer_Freedom-Network_7_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_Oxford-Health-Plans-LLC_Third-Party-Administrator_Freedom-Network_7_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Creighton-University_CSP-905-C871_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UMR--Inc-_TPA_REAL-APPEAL_ORLANDO-HEALTH_-RA_HOHR_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company_Insurer_Columbia-University_CSP-910-T081_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Columbia-University_CSP-910-T081_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UMR--Inc-_TPA_REAL-APPEAL_FORT-CARE-PPO_-RA_HFHC_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Children-s-Hospital-of-Colorado_GSP-901-C365_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Deere---Co---Alliance_CSP-912-T021_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UMR--Inc-_TPA_MEDICAL-COLLEGE-OF-WISCONSIN_1HWI_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UMR--Inc-_TPA_MEDICAL-COLLEGE-OF-WISCONSIN_1HWS_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-924-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UMR--Inc-_TPA_LAB-CARD_COX-HEALTH_-L_1HCX_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UMR--Inc-_TPA_ELAP_3HPE_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_HealthSCOPE-Benefits--Inc-_TPA_ELAP_3HPE_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_HealthSCOPE-Benefits--Inc-_TPA_HST-OVERLAY-WITH-PHCS-P-A-PLUS-HSB_2HMP_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UMR--Inc-_TPA_HST_2HST_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_HealthSCOPE-Benefits--Inc-_TPA_HST_2HST_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_HealthSCOPE-Benefits--Inc-_TPA_ELAP_2HEL_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_HealthSCOPE-Benefits--Inc-_TPA_GRANDVIEW-MEDHELP-DCI_HST-OVERLAY-WITH-PHCS-P-A-PLUS-HSB_-GRD_2HMP_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UMR--Inc-_TPA_ELAP-WITH-IMAGINE-HEALTH_3HIP_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_HealthSCOPE-Benefits--Inc-_TPA_ELAP-WITH-IMAGINE-HEALTH_3HIP_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UMR--Inc-_TPA_ELAP-IMAGINE---PHCS-P-A_2HIP_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_HealthSCOPE-Benefits--Inc-_TPA_DUNCASTER-INC-DCI_HST_-MIF_2HSP_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UMR--Inc-_TPA_ELAP_2HPE_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_HealthSCOPE-Benefits--Inc-_TPA_ELAP_2HPE_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_HealthSCOPE-Benefits--Inc-_TPA_RELYMD_HST-OVERLAY-WITH-PHCS-P-A-PLUS-HSB_-MID_2HMP_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Methodist-Richardson-Medical-Ctr_GSP-909-T046_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UMR--Inc-_TPA_POMCO---SUNY-UPSTATE-OBR-DCI_POMCO-SELECT-WITH-MAGNACARE-AND-PHCS-HD-TRAVEL_-UPT_MCRS_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_HealthSCOPE-Benefits--Inc-_TPA_HST-OVERLAY-WITH-HEALTHSMART-P-A-PLUS-HSB_2HHS_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UMR--Inc-_TPA_GLOBAL-STEERING-DIRECT-CONTRACT_ELAP_-GS_2HPE_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_HealthSCOPE-Benefits--Inc-_TPA_RELYMD_HST_-MID_2HST_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-698-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UMR--Inc-_TPA_LOUISIANA-MACHINERY-DIRECT-CONTRACTS_HST-PHCS-P-A_-LAM_3HSP_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UMR--Inc-_TPA_REAL-APPEAL_ELAP-FULL-RBP_-RA_3HLP_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Children-s-Hospital-of-Colorado_CSP-917-C866_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Georgia-Department-of-Community-Health_CSP-901-C418_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-913-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-969-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-670-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-274-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-736-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-728-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_University-of-Missouri---Columbia_CSP-933-C637_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_University-of-Missouri---Columbia_CSP-935-C638_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_NYU-School-of-Medicine_CSP-902-C012_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_NYU-Langone-Health-System_CSP-902-C012_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-907-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UMR--Inc-_TPA_GOLDEN-NUGGET_U8_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-906-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-910-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-662-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-687-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_HEALTHSCOPE-BENEFITS_TPA_SHO_U1_3342_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Texas-Health-Resources_GSP-901-C008_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-902-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-764-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-427-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Qualcomm---Premier-ACO_CSP-914-C783_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Qualcomm---Premier-ACO_CSP-933-C741_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-633-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Electric-Reliability-Council-of-Texas--ERCOT-_GSP-986-C655_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UMR--Inc-_TPA_GEHA-UBH-PLAN_UBH_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-783-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_University-of-Missouri---Columbia_GSP-913-C497_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_University-of-Missouri---Columbia_GSP-915-C508_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_General-Electric---Conv-Care-Adv-Prog_GSP-820-C667_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Caterpillar_CSP-911-T129_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-822-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_Qualcomm---Premier-ACO_GSP-932-C740_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Illinois--Inc-_Insurer_Illinois-Provider-Network_ILNETWORK_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company-of-New-York_Insurer_Empire_CSP-477-A351_in-network-rates.json.gz',
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_United-HealthCare-Services--Inc-_Third-Party-Administrator_NYU-Langone-Health-System_CSP-904-C020_in-network-rates.json.gz']

my_code_list = ['86328', '0001U', '97802', '99423']
my_output_dir = 'flatten'

for url in tqdm(urls):
    parse_to_file(url, billing_code_list = my_code_list, output_dir = my_output_dir, overwrite = False)

