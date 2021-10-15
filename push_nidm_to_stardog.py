import requests
from requests.auth import HTTPBasicAuth
import pathlib as pal

from tqdm import tqdm


ttl_p = pal.Path('./simple2_NIDM_examples/datasets.datalad.org/openneuro/')
blaze_url = 'http://star.braindog.net:5820/nidm-openneuro/?graph = urn:graph'
headers={'Content-Type': 'text/turtle'}

files_to_upload = list(ttl_p.glob('*/nidm.ttl'))
for p in files_to_upload:
    nidm_ttl = open(p, 'rb')
    response = requests.post(url=blaze_url, data=nidm_ttl, headers=headers, auth=HTTPBasicAuth('admin', 'admin'))
    if not response.ok:
        print(f'    BAD: {p.name}: {response.status_code}')
print('Done')
