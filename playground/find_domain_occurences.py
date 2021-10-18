import json
from pathlib import Path

import pandas as pd
import requests
from requests.auth import HTTPBasicAuth

NIDM_CONTEXT = '''
PREFIX afni: <http://purl.org/nidash/afni#>
PREFIX ants: <http://stnava.github.io/ANTs/>
PREFIX bids: <http://bids.neuroimaging.io/>
PREFIX birnlex: <http://bioontology.org/projects/ontologies/birnlex/>
PREFIX crypto: <http://id.loc.gov/vocabulary/preservation/cryptographicHashFunctions#>
PREFIX datalad: <http://datasets.datalad.org/>
PREFIX dc: <http://purl.org/dc/elements/1.1/>
PREFIX dcat: <http://www.w3.org/ns/dcat#>
PREFIX dct: <http://purl.org/dc/terms/>
PREFIX dctypes: <http://purl.org/dc/dcmitype/>
PREFIX dicom: <http://neurolex.org/wiki/Category:DICOM_term/>
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
PREFIX freesurfer: <https://surfer.nmr.mgh.harvard.edu/>
PREFIX fsl: <http://purl.org/nidash/fsl#>
PREFIX ilx: <http://uri.interlex.org/base/>
PREFIX ncicb: <http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl#>
PREFIX ncit: <http://ncitt.ncit.nih.gov/>
PREFIX ndar: <https://ndar.nih.gov/api/datadictionary/v2/dataelement/>
PREFIX nfo: <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#>
PREFIX nidm: <http://purl.org/nidash/nidm#>
PREFIX niiri: <http://iri.nidash.org/>
PREFIX nlx: <http://uri.neuinfo.org/nif/nifstd/>
PREFIX obo: <http://purl.obolibrary.org/obo/>
PREFIX onli: <http://neurolog.unice.fr/ontoneurolog/v3.0/instrument.owl#>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX pato: <http://purl.obolibrary.org/obo/pato#>
PREFIX prov: <http://www.w3.org/ns/prov#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX scr: <http://scicrunch.org/resolver/>
PREFIX sio: <http://semanticscience.org/ontology/sio.owl#>
PREFIX spm: <http://purl.org/nidash/spm#>
PREFIX vc: <http://www.w3.org/2006/vcard/ns#>
PREFIX xml: <http://www.w3.org/XML/1998/namespace>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
'''

DOG_ROOT = 'http://star.braindog.net'
DOG_DB = 'nidm-openneuro'
DOG_PORT = 5820
query_url = f'{DOG_ROOT}:{DOG_PORT}/{DOG_DB}/query'
headers = {'Content-Type': 'application/sparql-query', 'Accept': 'application/sparql-results+json'}
OUT_PATH = Path(__file__).parent / '../data'


def parse_response(resp):
    _results = json.loads(resp.decode('utf-8'))
    return pd.DataFrame([{k: v['value'] for k, v in res.items()} for res in _results['results']['bindings']])


# Find all ages
data_element_query = '''
SELECT DISTINCT ?label  ?description ?source ?concept ?levels
WHERE {
    ?de a/rdfs:subClassOf* nidm:DataElement.
    OPTIONAL {?de rdfs:label ?label . } .
    OPTIONAL {?de dct:description ?description . } .
    OPTIONAL {?de nidm:sourceVariable ?source . } .
    OPTIONAL {?de nidm:isAbout ?concept . } .
    OPTIONAL {?de nidm:levels ?levels . } .
}
'''

response = requests.post(url=query_url, data=NIDM_CONTEXT + data_element_query, headers=headers,
                         auth=HTTPBasicAuth('admin', 'admin'))
de = parse_response(response.content)


# Match a number of things
def match(df, cols, keywords):
    """
    Create an index where any string in the cols matches any of the keywords
    """
    return [any([str(word).lower() in str(row[col]).lower()
                 for col in cols
                 for word in keywords])
            for rid, row in df.iterrows()]


# Diagnosis
columns = ['concept', 'description', 'label', 'source']
diag_keys = ['diagnosis', 'disorder', 'condition', 'clinical', 'medical', 'disease', 'syndrome', 'impairment', 'health',
            'control', 'typical', 'group']
diagnosis_index = match(de, columns, diag_keys)

de_diagnosis = de.loc[diagnosis_index]

# Age
age_keys = ['age', 'years', 'birth']
age_index = match(de, columns, age_keys)
de_age = de.loc[age_index]

# Sex
sex_keys = ['sex', 'gender', 'male', 'female']
sex_index = match(de, columns, sex_keys)
de_sex = de.loc[sex_index]

# Assessment
instrument_keys = ['assessment', 'response', 'test', 'instrument', 'symptom', 'observation']
instrument_index = match(de, columns, instrument_keys)
de_instrument = de.loc[instrument_index]

# No concepts
de_no_concepts = de.query('concept.isna()', engine='python')

# Unclassified
any_index = [not any(i) for i in zip(*[diagnosis_index, age_index, sex_index, instrument_index])]
de_unclassified = de.loc[any_index]

# Save the dataframes
de_diagnosis.to_csv(OUT_PATH / 'de_diagnosis.tsv', sep='\t')
de_age.to_csv(OUT_PATH / 'de_age.tsv', sep='\t')
de_sex.to_csv(OUT_PATH / 'de_sex.tsv', sep='\t')
de_instrument.to_csv(OUT_PATH / 'de_instrument.tsv', sep='\t')
de_no_concepts.to_csv(OUT_PATH / 'de_no_concepts.tsv', sep='\t')
de_unclassified.to_csv(OUT_PATH / 'de_unclassified.tsv', sep='\t')

print('Done')
