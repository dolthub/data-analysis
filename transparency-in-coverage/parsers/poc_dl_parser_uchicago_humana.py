"""
A proof of concept downloader/parser for UChicago.
"""

url = 'https://developers.humana.com/Resource/DownloadPCTFile?fileType=innetwork&fileName=2022-08-25_1_in-network-rates_000000000100.csv.gz'
import csv
import requests
import gzip
import os


"""
SET OF INDICES 

{'REPORTING_ENTITY_NAME': 0,
 'REPORTING_ENTITY_TYPE': 1,
 'LAST_UPDATED_ON': 2,
 'VERSION': 3,
 'NPI': 4,
 'TIN': 5,
 'TYPE': 6,
 'NEGOTIATION_ARRANGEMENT': 7,
 'NAME': 8,
 'BILLING_CODE_TYPE': 9,
 'BILLING_CODE_TYPE_VERSION': 10,
 'BILLING_CODE': 11,
 'DESCRIPTION': 12,
 'NEGOTIATED_TYPE': 13,
 'NEGOTIATED_RATE': 14,
 'EXPIRATION_DATE': 15,
 'SERVICE_CODE': 16,
 'BILLING_CLASS': 17,
 'BILLING_CODE_MODIFIER': 18,
 'ADDITIONAL_INFO': 19,
 'BUNDLED_BILLING_CODE_TYPE': 20,
 'BUNDLED_BILLING_CODE_VERSION': 21,
 'BUNDLED_BILLING_CODE': 22,
 'BUNDLED_DESCRIPTION\n': 23}
 """

# Select the indices (idxs) you want to keep
idxs = [0, 4, 9, 11, 12]

filename = 'humana.csv'

with requests.get(url, stream = True) as r:
    f = gzip.GzipFile(fileobj = r.raw)
    
    line = next(f)
    fieldnames = line.decode().split('~')  
    fieldnames = [fieldnames[k] for k in idxs]
    
    
    # If the file doesn't exist, create it
    # and add the header
    if not os.path.exists(filename):
        with open(filename, 'w') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames = fieldnames)
            writer.writeheader()
    
    # Open the existing CSV file and begin appending
    # to the end
    with open(filename, 'a') as csvfile:
        
        writer = csv.DictWriter(csvfile, fieldnames = fieldnames)
        writer.writeheader()
        
        # Loop over the streaming file and write each row one at a time
        # This can be sped up if you batch the writing of the rows
        # and use the writer.writerows() method
        for line in f:
            
            values = line.decode().split('~')
            values = [values[k] for k in idxs]

            row = {k: v for k, v in zip(fieldnames, values)}        

            writer.writerow(row)    