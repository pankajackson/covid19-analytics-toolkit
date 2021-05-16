import  pandas as pd
from elasticsearch import Elasticsearch
import requests
import time
import json

# Load Data
df = pd.read_csv('data/owid-covid-data.csv')
df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
df['date'] = df['date'].dt.strftime('%Y-%m-%d %H:%M:%S')

print(df.dtypes)

df_dict = df.to_dict(orient="records")
print(type(df_dict))

for doc in df_dict:
    headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
    res = requests.post("http://kube.jackson.com:30331", json=json.dumps(doc), headers=headers)
    print(res.content)

    es = Elasticsearch(hosts='http://kube.jackson.com:30337')
    res = es.index(index="covid-%s" % str(doc['date']).split()[0], id=1, body=doc, doc_type='_doc')
    print(res['result'])