import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from elasticsearch import Elasticsearch
import requests
import json
import datetime
from retrying import retry


# Set Figure Size
plt.figure(figsize=(25, 25))


# Load DataSet
df = pd.read_csv('data/raw/owid-covid-data.csv')
df_org = df.copy()
# set properties to print all column and rows
pd.set_option('display.max_column', None)
pd.set_option('display.max_row', None)

# Get Information of DataSet
df.info()


# Fill missing continent
iso_codes = df['iso_code'].unique()
continents = df['continent'].unique()

for iso_c in iso_codes:

    # Drop Data having iso_code starting with OWID_
    if str(iso_c).startswith('OWID_'):
        index = df[df['iso_code']==iso_c].index
        df.drop(index=index, inplace=True)
    else:
        null_count = df[df['iso_code']==iso_c]['continent'].isnull().sum()
        if null_count > 0:
            print(iso_c, 'has %s null continent' % null_count)
            n_null = df[df.continent.notnull()]
            con = n_null[n_null['iso_code']==iso_c].head()['continent'].unique()
            if len(con) != 0:
                df[df['iso_code'] == iso_c]['continent'].fillna(con[0], inplace=True)
                print(iso_c, ' has filled with continent %s' % con[0])
            else:
                print(iso_c, ' has no continent')

            print(df['continent'].isnull().sum())


# Fill Missing tests_units
df['tests_units'].fillna('None', inplace=True)


# Fill Missing Numerical Fields
numeric_data = df.select_dtypes(include=['int64', 'float64'])
missing_data_rows = numeric_data[numeric_data.isnull().any(axis=1)]
missing_data_column = [feature for feature in numeric_data if numeric_data[feature].isnull().sum()>0]

# Fill NA using FFill Value or Zero Value
# df_filled_bfill = numeric_data.fillna(method='bfill').fillna(method='ffill')
df_filled_zero = numeric_data.fillna(0)
df.update(df_filled_zero)
print(df_filled_zero.isnull().sum().sum())


# Save data in CSV
df.to_csv('data/cleaned/owid-covid-data-cleaned.csv')

# Push to ES

df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
df['date'] = df['date'].dt.to_pydatetime()
df['date'] = df['date'].dt.strftime('%Y-%m-%d %H:%M:%S')

es = Elasticsearch(
    hosts=[
        'http://kube.jackson.com:30337',
        'http://kubenode01.jackson.com:30337',
        'http://kubenode02.jackson.com:30337',
        'http://kubenode03.jackson.com:30337',
    ]
)

@retry(stop_max_attempt_number=3, wait_fixed=10000)
def push_data_to_es(index, id, doc_type, body):
    res = es.index(index=index, id=id, doc_type=doc_type, body=body)
    print(res['result'])




df_dict = df.to_dict(orient="records")
for doc in df_dict:
    doc['@timestamp'] = datetime.datetime.strptime(doc['date'], '%Y-%m-%d %H:%M:%S')
    _id = doc['continent'] + doc['iso_code'] + str(int(doc['@timestamp'].timestamp()))
    index = ('covid-' + '-' + str(doc['@timestamp'].year) + '-' + str(doc['@timestamp'].month)).lower()
    print(_id)

    push_data_to_es(index=index, id=_id, doc_type='_doc', body=doc)




