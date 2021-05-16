import pandas as pd
from elasticsearch import Elasticsearch, helpers
import datetime
from retrying import retry
import time


source_data_path = 'data/raw/owid-covid-data.csv'


# Load DataSet
def get_dataframe(path=source_data_path):
    df = pd.read_csv(path)
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
    return df


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
    return df


# Save data in CSV
def save_cleaned_df(df=get_cleaned_df()):
    df.to_csv('data/cleaned/owid-covid-data-cleaned.csv')

# Push to ES
def get_es_con():
    es = Elasticsearch(
        hosts=[
            'http://kube.jackson.com:30337',
            'http://kubenode01.jackson.com:30337',
            'http://kubenode02.jackson.com:30337',
            'http://kubenode03.jackson.com:30337',
        ]
    )
    return es

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



def process_es_data(df=get_cleaned_df()):

    df['date'] = df['date'].dt.to_pydatetime()
    df['date'] = df['date'].dt.strftime('%Y-%m-%d %H:%M:%S')
    df_dict = df.to_dict(orient="records")
    for doc in df_dict:
        doc['@timestamp'] = datetime.datetime.strptime(doc['date'], '%Y-%m-%d %H:%M:%S')
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
        df_dict = data.to_dict(orient="records")
        for doc in df_dict:
            doc['@timestamp'] = datetime.datetime.strptime(doc['date'], '%Y-%m-%d %H:%M:%S')
            _id = doc['continent'] + doc['iso_code'] + str(int(doc['@timestamp'].timestamp()))
            index = ('covid-' + str(doc['@timestamp'].year) + '-' + str(doc['@timestamp'].month)).lower()
            action = {
                "_index": index,
                "_type": "_doc",
                "_id": _id,
                "_source": doc
            }
            actions.append(action)
        time.sleep(5)
        res = push_bulk_data_to_es(actions=actions)
        print(res)


process_bulk_es_data()

