import pandas as pd
from elasticsearch import Elasticsearch, helpers
from datetime import datetime
from retrying import retry
import requests
import os


source_data_url = 'https://covid.ourworldindata.org/data/owid-covid-data.csv'
source_data_dir = 'data/raw/'
source_data_file = 'owid-covid-data.csv'
dest_clean_data_dir = 'data/cleaned/'


es_hosts=[
            'http://kube.jackson.com:30337',
            'http://kubenode01.jackson.com:30337',
            'http://kubenode02.jackson.com:30337',
            'http://kubenode03.jackson.com:30337',
        ]

# Get ES
def get_es_con():
    es = Elasticsearch(hosts=es_hosts)
    return es

# Download Data
@retry(stop_max_attempt_number=3, wait_fixed=10000)
def download_data(url=source_data_url):
    if not os.path.exists(os.path.abspath(source_data_dir)):
        os.makedirs(os.path.abspath(source_data_dir))
    abs_data_path = os.path.join(os.path.abspath(source_data_dir), datetime.now().strftime("%Y%m%d__%H%M") + '_' + source_data_file)
    url_content = requests.get(url).content
    file = open(abs_data_path, 'wb')
    file.write(url_content)
    file.close()
    return abs_data_path

# Load DataSet
def get_dataframe(path=download_data()):
    df = pd.read_csv(path)
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
    return df

# Save data in CSV
def save_cleaned_df(df):
    if not os.path.exists(os.path.abspath(dest_clean_data_dir)):
        os.makedirs(os.path.abspath(dest_clean_data_dir))
    abs_cleaned_data_path = os.path.join(os.path.abspath(dest_clean_data_dir), datetime.now().strftime("%Y%m%d__%H%M") + '_cleaned_' + source_data_file)
    df.to_csv(abs_cleaned_data_path)

# Fill missing continent
def get_cleaned_df(df=get_dataframe()):
    iso_codes = df['iso_code'].unique()

    # Fill missing continent
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

    # Fill NA using FFill Value or Zero Value
    df_filled_zero = numeric_data.fillna(0)
    df.update(df_filled_zero)
    save_cleaned_df(df)
    return df

@retry(stop_max_attempt_number=3, wait_fixed=10000)
def push_data_to_es(index, id, doc_type, body):
    es = get_es_con()
    res = es.index(index=index, id=id, doc_type=doc_type, body=body)
    print(res['result'])

@retry(stop_max_attempt_number=3, wait_fixed=10000)
def push_bulk_data_to_es(actions):
    es = get_es_con()
    if actions:
        res = helpers.bulk(es, actions=actions)
        return res
    return None

def get_last_updated_datetime(es=get_es_con(), iso_c=None):
    if iso_c:
        latest_data_query_body = {"sort": [{"@timestamp": {"order": "desc"}}], "query": {"match": {"iso_code": iso_c}}}
        latest_data = es.search(index="covid-*", body=latest_data_query_body, size=1)
        if len(latest_data['hits']['hits']) > 0:
            latest_date = latest_data['hits']['hits'][0]['_source']['date']
            return latest_date
    return None

def process_es_data(df=get_cleaned_df()):
    df['date'] = df['date'].dt.to_pydatetime()
    df['date'] = df['date'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df_dict = df.to_dict(orient="records")
    for doc in df_dict:
        doc['@timestamp'] = datetime.strptime(doc['date'], '%Y-%m-%d %H:%M:%S')
        _id = doc['continent'] + doc['iso_code'] + str(int(doc['@timestamp'].timestamp()))
        index = ('covid-' + '-' + str(doc['@timestamp'].year) + '-' + str(doc['@timestamp'].month)).lower()
        print(_id)
        push_data_to_es(index=index, id=_id, doc_type='_doc', body=doc)

def process_bulk_es_data(df=get_cleaned_df()):
    for iso_c, data in df.groupby('iso_code'):
        print('Processing %s' % iso_c)
        actions = []
        data['date'] = data['date'].dt.to_pydatetime()
        data['date'] = data['date'].dt.strftime('%Y-%m-%d %H:%M:%S')
        last_updated_datetime = get_last_updated_datetime(iso_c=iso_c)
        data=data[data['date']>=last_updated_datetime]
        df_dict = data.to_dict(orient="records")
        for doc in df_dict:
            doc['@timestamp'] = datetime.strptime(doc['date'], '%Y-%m-%d %H:%M:%S')
            _id = doc['continent'] + doc['iso_code'] + str(int(doc['@timestamp'].timestamp()))
            index = ('covid-' + str(doc['@timestamp'].year) + '-' + str(doc['@timestamp'].month)).lower()
            action = {
                "_index": index,
                "_type": "_doc",
                "_id": _id,
                "_source": doc
            }
            actions.append(action)
            print(doc['date'])
        res = push_bulk_data_to_es(actions=actions)
        print(res)

process_bulk_es_data()

