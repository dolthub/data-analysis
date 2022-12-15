from mrfutils import data_import, flatten_mrf, InvalidMRF
import argparse
import logging

log = logging.getLogger('mrfutils')

parser = argparse.ArgumentParser()
parser.add_argument('-u', '--url')
parser.add_argument('-o', '--out')

args = parser.parse_args()

code_set = data_import('test/codes.csv')
npi_set = {int(x[0]) for x in data_import('test/npis.csv')}

if __name__ == '__main__':
    try:
        flatten_mrf(
            loc=url,
            out_dir='debug',
            code_set=code_set,
            npi_set=npi_set
        )
    except InvalidMRF as e:
        log.critical(e)
        pass
