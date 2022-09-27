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
import os
import csv
import uuid
import glob
import requests
import gzip
import time
from tqdm import tqdm

# not clear if this saves time or not
import httpio # for seeking through streamed files

SCHEMA = {
    'root':[
        'root_uuid',
        'reporting_entity_name',
        'reporting_entity_type',
        'last_updated_on',
        'version',
        'url',],

    'in_network':[
        'root_uuid',
        'in_network_uuid',
        'in_network.negotiation_arrangement',
        'in_network.name',
        'in_network.billing_code_type',
        'in_network.billing_code_type_version',
        'in_network.billing_code',
        'in_network.description',],

    'in_network.negotiated_rates':[
        'root_uuid',
        'in_network_uuid',
        'in_network.negotiated_rates_uuid',
        'in_network.negotiated_rates.provider_references',],

    'in_network.negotiated_rates.negotiated_prices':[
        'root_uuid',
        'in_network_uuid',
        'in_network.negotiated_rates_uuid',
        'in_network.negotiated_rates.negotiated_prices_uuid',
        'in_network.negotiated_rates.negotiated_prices.negotiated_type',
        'in_network.negotiated_rates.negotiated_prices.negotiated_rate',
        'in_network.negotiated_rates.negotiated_prices.expiration_date',
        'in_network.negotiated_rates.negotiated_prices.service_code',
        'in_network.negotiated_rates.negotiated_prices.billing_class',
        'in_network.negotiated_rates.negotiated_prices.additional_information',
        'in_network.negotiated_rates.negotiated_prices.billing_code_modifier',],

    'in_network.negotiated_rates.provider_groups':[
        'root_uuid',
        'in_network_uuid',
        'in_network.negotiated_rates_uuid',
        'in_network.negotiated_rates.provider_groups_uuid',
        'in_network.negotiated_rates.provider_groups.npi',],

    'in_network.negotiated_rates.provider_groups.tin':[
        'root_uuid',
        'in_network_uuid',
        'in_network.negotiated_rates_uuid',
        'in_network.negotiated_rates.provider_groups_uuid',
        'in_network.negotiated_rates.provider_groups.tin_uuid',
        'in_network.negotiated_rates.provider_groups.tin.type',
        'in_network.negotiated_rates.provider_groups.tin.value',],

    'provider_references':[
        'root_uuid',
        'provider_references_uuid',
        'provider_references.provider_group_id',],

    'provider_references.provider_groups':[
        'root_uuid',
        'provider_references_uuid',
        'provider_references.provider_groups_uuid',
        'provider_references.provider_groups.npi',],

    'provider_references.provider_groups.tin':[
        'root_uuid',
        'provider_references_uuid',
        'provider_references.provider_groups_uuid',
        'provider_references.provider_groups.tin_uuid',
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


def flatten_to_file(obj, output_dir, prefix = '', **uuids):
    """Takes an object, turns it into a dict, and 
    writes it to file
    """

    data = {}

    uuids[f'{prefix}_uuid'] = uuid.uuid4()
    
    for key, value in uuids.items():
        data[key] = value
        
    for key, value in obj.items():
        
        key_id = f'{prefix}.{key}' if prefix else key
        
        if type(value) in [str, int, float]: 
            data[key_id] = value

        elif type(value) == list:
            
            if len(value) == 0: 
                data[key_id] = None
                
            elif type(value[0]) in [str, int, float]:
                data[key_id] = value
                
                
            else:
                for subvalue in value:
                    flatten_to_file(subvalue, output_dir, key_id, **uuids)
                    
    write_dict_to_file(output_dir, prefix, data)


def parse_to_file(url, billing_code_list, output_dir, overwrite = False):
    
    if os.path.exists(output_dir):
        if overwrite:
            for file in glob.glob(f'{output_dir}/*'):
                os.remove(file)
    else:
        os.mkdir(output_dir)

    uuids = {'root_uuid':uuid.uuid4()}

    provider_references_list = []
    codes_found = False

    print(f'Streaming from remote URL: {url}\n')
    with httpio.open(url) as r:
        f = gzip.GzipFile(fileobj = r)

        objs = ijson.items(f, 'in_network.item', use_float = True)
        
        for obj in objs:

            # Loop through objects
            if obj['billing_code'] in billing_code_list:
                codes_found = True

                # Write the object
                flatten_to_file(obj, output_dir, prefix = 'in_network', **uuids)

                for negotiated_rate in obj['negotiated_rates']:
                    for provider_reference in negotiated_rate['provider_references']:
                        provider_references_list.append(provider_reference)
                        
        if not codes_found:
            return
        
        f.seek(0)

        # Once we know that there are codes,
        # get the front matter
        data = {}
        data['root_uuid'] = uuids['root_uuid']
        data['url'] = url
            
        parser = ijson.parse(f)

        for prefix, event, value in parser:
            if event in ['string', 'number']:
                data[f'{prefix}'] = value

            if event == 'start_array':
                break

        write_dict_to_file(output_dir, 'root', data)

        # Go back to the beginning of the file to scrape the provider refs
        objs = ijson.items(parser, 'provider_references.item', use_float = True)

        for obj in objs:
            if obj['provider_group_id'] in provider_references_list:
                flatten_to_file(obj, output_dir, prefix = 'provider_references', **uuids)


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
 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-Insurance-Company_Insurer_Cisco-Systems_CSP-952-C932_in-network-rates.json.gz',]

my_code_list = ['86328', '0001U', '97802', '99423']
my_output_dir = 'flatten'

for url in tqdm(urls):
    parse_to_file(url, billing_code_list = my_code_list, output_dir = my_output_dir, overwrite = False)

