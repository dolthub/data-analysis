from core import stream_json_to_csv, get_mrfs_from_index
from helpers import create_output_dir
import logging
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

logger = logging.getLogger('core')
logger.setLevel(level=logging.DEBUG)

OUTPUT_DIR = 'uhc_cesarean'

C_SECTIONS = [
	('CPT', '59510'),
	('CPT', '59514'),
	('CPT', '59515'),
]

# Try these other URLs:
# url = 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Mississippi--Inc-_Insurer_HML-75_ED_in-network-rates.json.gz'
# url = 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Texas--Inc-_Insurer_HML-75_ED_in-network-rates.json.gz'
# url = 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Ohio--Inc-_Insurer_HML-75_ED_in-network-rates.json.gz'
# url = 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Florida--Inc-_Insurer_HML-75_ED_in-network-rates.json.gz'
# url = 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Kentucky--Ltd-_Insurer_HML-75_ED_in-network-rates.json.gz'
# url = 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-09-01/2022-09-01_UnitedHealthcare-of-Louisiana--Inc-_Insurer_HML-75_ED_in-network-rates.json.gz'
# url = 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-10-01/2022-10-01_UnitedHealthcare-of-Utah--Inc-_Insurer_HML-15_S9_in-network-rates.json.gz'
url = 'https://uhc-tic-mrf.azureedge.net/public-mrf/2022-10-01/2022-10-01_UnitedHealthcare-of-Georgia--Inc-_Insurer_HML-15_S9_in-network-rates.json.gz'

create_output_dir(OUTPUT_DIR, overwrite = False)

stream_json_to_csv(url, output_dir = OUTPUT_DIR, code_filter = C_SECTIONS)