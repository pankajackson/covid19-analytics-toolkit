import pandas as pd
import numpy as np
from elasticsearch import Elasticsearch, helpers
from geopy.geocoders import Nominatim
from datetime import datetime
from retrying import retry
from urllib.parse import urlparse
import requests
import os
import pycountry

es_index = os.getenv('ES_INDEX', "test")
print('ES_INDEX: %s' % es_index)
owid_data_source_url = os.getenv('OWID_SOURCE_DATA_URL', "https://covid.ourworldindata.org/data/owid-covid-data.csv")
csse_data_source_baseurl = os.getenv('CSSEGISANDDATA_DATA_SOURCE_BASEURL', "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports")
source_data_dir = os.getenv('SOURCE_DATA_DIR', 'data/raw/')
dest_clean_data_dir = os.getenv('DEST_CLEAN_DATA_DIR', 'data/cleaned/')
es_hosts=os.getenv('ES_URL', [
            'http://kube.jackson.com:30337',
            'http://kubenode01.jackson.com:30337',
            'http://kubenode02.jackson.com:30337',
            'http://kubenode03.jackson.com:30337',
        ])
geo_points = {}
iso_codes = {}

# Get ES
def get_es_con():
    es = Elasticsearch(hosts=es_hosts)
    return es

def get_paths(url):
    timestamp = datetime.now().strftime("%Y%m%d__%H%M")
    file_name = os.path.basename(urlparse(url).path)
    if not os.path.exists(os.path.abspath(source_data_dir)):
        os.makedirs(os.path.abspath(source_data_dir))
    raw_file_path = os.path.join(os.path.abspath(source_data_dir), timestamp + '_' + file_name)
    if not os.path.exists(os.path.abspath(dest_clean_data_dir)):
        os.makedirs(os.path.abspath(dest_clean_data_dir))
    cleaned_file_path = os.path.join(os.path.abspath(dest_clean_data_dir), timestamp + '_cleaned_' + file_name)
    return {
        "url": url,
        "url_file_name": file_name,
        "raw_file_path": raw_file_path,
        "raw_file_name": os.path.basename(raw_file_path),
        "raw_file_dir_name": os.path.dirname(raw_file_path),
        "cleaned_file_path": cleaned_file_path,
        "cleaned_file_name": os.path.basename(cleaned_file_path),
        "cleaned_file_dir_name": os.path.dirname(cleaned_file_path)
    }


# Download Data
@retry(stop_max_attempt_number=3, wait_fixed=10000)
def download_data(url):
    print('Downloading Data %s...' % url)
    paths = get_paths(url)
    if not os.path.exists(os.path.abspath(paths['raw_file_dir_name'])):
        os.makedirs(os.path.abspath(paths['raw_file_dir_name']))
    url_content = requests.get(url).content
    file = open(paths['raw_file_path'], 'wb')
    file.write(url_content)
    file.close()
    return paths['raw_file_path']

# Load DataSet
def get_dataframe(path):
    print('Reading Data...')
    df = pd.read_csv(path)
    return df

def get_iso_code(data):
    print('Fetching ISO_CODE for %s' % str(data['country']))
    geolocator = Nominatim(user_agent="python")
    loc = geolocator.reverse("%s, %s" % (data['latitude'], data['longitude']))
    if not loc:
        print('address not found from loc and lan')
        loc = geolocator.geocode(str(data['state']) + ',' + str(data['country']))
    if not loc:
        loc = geolocator.geocode(str(data['combined']))
    if not loc:
        loc = geolocator.geocode(str(data['country']))
    if not loc:
        loc = geolocator.geocode(str(data['state']))
    try:
        location = pycountry.countries.get(alpha_2=str(loc.raw['address']['country_code']))
        iso_code = {
            'alpha_3': str(location.alpha_3).upper(),
            'alpha_2': str(location.alpha_2).upper(),
        }
        iso_codes[data['country']] = iso_code
        return iso_code
    except:
        try:
            location = pycountry.countries.search_fuzzy(str(data['country']))
        except:
            try:
                location = pycountry.countries.search_fuzzy(str(loc.address).split(',')[-1].strip())
            except:
                try:
                    location = pycountry.countries.search_fuzzy(str(data['combined']))
                except:
                    try:
                        location = pycountry.countries.search_fuzzy(str(data['state']))
                    except:
                        res = {'alpha_3': '', 'alpha_2': ''}
                        return res
    iso_code = {
        'alpha_3': str(location[0].alpha_3).upper(),
        'alpha_2': str(location[0].alpha_2).upper(),
    }
    iso_codes[data['country']] = iso_code
    return iso_code

# Save data in CSV
def save_cleaned_df(df, file_path):
    print('Saving Cleaned Data...')
    if not os.path.exists(os.path.abspath(os.path.dirname(file_path))):
        os.makedirs(os.path.abspath(os.path.dirname(file_path)))
    df.to_csv(file_path)

# Fill csse missing values
def get_cleaned_csse_data_df(df):
    print('Cleaning CSSE Data...')

    # Filling missing latitude and longitude
    if 'Lat' not in df:
        df['Lat'] = np.nan
    if 'Long_' not in df:
        df['Long_'] = np.nan
    if 'Province/State' in df and not 'Province_State' in df:
        df.rename(columns={'Province/State': 'Province_State'}, inplace=True)
    if 'Country/Region' in df and not 'Country_Region' in df:
        df.rename(columns={'Country/Region': 'Country_Region'}, inplace=True)
    if 'Last Update' in df and not 'Last_Update' in df:
        df.rename(columns={'Last Update': 'Last_Update'}, inplace=True)
    if not 'Combined_Key' in df:
        if 'Province_State' in df and 'Country_Region' in df:
            df['Combined_Key'] = df['Province_State'] + ', ' + df['Country_Region']
        if  'Country_Region' in df:
            df['Combined_Key'] = df['Country_Region']
        if 'Province_State' in df:
            df['Combined_Key'] = df['Province_State']
    geolocator = Nominatim(user_agent="python")
    for i, row in df[df['Lat'].isnull()].iterrows():
        try:
            try:
                df.at[i, 'Lat'] = geo_points[row['Combined_Key']]['latitude']
                df.at[i, 'Long_'] = geo_points[row['Combined_Key']]['longitude']
            except:
                country = str(row['Country_Region'])
                city = str(row['Province_State'])
                loc = geolocator.geocode(city + ',' + country)
                if not loc:
                    loc = geolocator.geocode(row['Combined_Key'])
                if not loc:
                    loc = geolocator.geocode(row['Province_State'])
                if not loc:
                    loc = geolocator.geocode(row['Country_Region'])
                if not loc:
                    print('Location Not Found for %s, %s' % (row['Province_State'], row['Country_Region']))

                else:
                    df.at[i, 'Lat'] = loc.latitude
                    df.at[i, 'Long_'] = loc.longitude
                    # Update Local geolocation database
                    geo_points[row['Combined_Key']] = {
                        'latitude': loc.latitude,
                        'longitude': loc.longitude
                    }
        except Exception as e:
            print(str(e))


    # Filling iso_code
    df['iso_code'] = np.nan
    for i, row in df.groupby('Country_Region'):
        try:
            df.loc[df.Country_Region == i, 'iso_code'] = iso_codes[i]['alpha_3']
        except:
            location = row.iloc[0]
            req = {
                'country': location['Country_Region'],
                'state': location['Province_State'],
                'combined': location['Combined_Key'],
                'latitude': location['Lat'],
                'longitude': location['Long_'],
            }
            iso_code = get_iso_code(req)
            if iso_code['alpha_3']:
                df.loc[df.Country_Region == i, 'iso_code'] = iso_code['alpha_3']
            else:
                print('ISO code Not Found for %s' % i)


    # Fill Missing Numerical features
    numeric_data = df.select_dtypes(include=['int64', 'float64'])
    df_filled_zero = numeric_data.fillna(0)
    df.update(df_filled_zero)

    # Fill Missing Categorical features
    categorical_data = df.select_dtypes(include=['object'])
    df_filled_unknown = categorical_data.fillna('Unknown')
    df.update(df_filled_unknown)


    return df

def get_csse_df(date=datetime.now()):
    file_format = date.strftime('%m-%d-%Y')
    csse_data_source_url = os.path.join(csse_data_source_baseurl, '%s.csv' % file_format)
    paths = get_paths(csse_data_source_url)
    if not os.path.exists(paths['cleaned_file_path']):
        if not os.path.exists(paths['raw_file_path']):
            csse_df_csv = download_data(csse_data_source_url)
        else:
            csse_df_csv = paths['raw_file_path']
        csse_df = get_dataframe(csse_df_csv)
        cleaned_csse_df = get_cleaned_csse_data_df(csse_df)
        save_cleaned_df(cleaned_csse_df, paths['cleaned_file_path'])
    else:
        cleaned_csse_df = pd.read_csv(paths['cleaned_file_path'])
    return cleaned_csse_df


# Fill  owid missing values
def get_cleaned_owid_df(df):
    print('Cleaning OWID Data...')

    # Fill missing continent
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
    iso_codes = df['iso_code'].unique()
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
        latest_data = es.search(index=es_index + '-*', body=latest_data_query_body, size=1)
        if len(latest_data['hits']['hits']) > 0:
            latest_date = latest_data['hits']['hits'][0]['_source']['date']
            return latest_date
    return None

@retry(stop_max_attempt_number=3, wait_fixed=10000)
def process_bulk_es_data(df):
    print('Processing bulk Data...')
    df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
    for iso_c, data in df.groupby('iso_code'):
        try:
            actions = []
            data['date'] = data['date'].dt.to_pydatetime()
            data['date'] = data['date'].dt.strftime('%Y-%m-%d %H:%M:%S')
            last_updated_datetime = get_last_updated_datetime(iso_c=iso_c)
            if last_updated_datetime:
                data=data[data['date']>=last_updated_datetime]
                print(last_updated_datetime)
            df_dict = data.to_dict(orient="records")
            for doc in df_dict:
                doc['@timestamp'] = datetime.strptime(doc['date'], '%Y-%m-%d %H:%M:%S')
                _id = (doc['continent'] + doc['iso_code'] + '_' + str(doc['@timestamp'])).replace(' ', '_').replace(':','-')
                index = (es_index + '-' + str(doc['@timestamp'].year) + '-' + str(doc['@timestamp'].month)).lower()
                csse_df = get_csse_df(doc['@timestamp'])
                csse_df_cont = csse_df[csse_df['iso_code'] == str(doc['iso_code']).upper()]
                if csse_df_cont.shape[0] == 0:
                    print('No data for location: ', doc['location'])
                doc['state'] = csse_df_cont.to_dict(orient="records")
                action = {
                    "_index": index,
                    "_type": "_doc",
                    "_id": _id,
                    "_source": doc
                }
                actions.append(action)
            push_bulk_data_to_es(actions=actions)
        except Exception as e:
            print(str(e))


if __name__ == '__main__':
    paths = get_paths(owid_data_source_url)

    if not os.path.exists(paths['cleaned_file_path']):
        if not os.path.exists(paths['raw_file_path']):
            owid_df_csv = download_data(owid_data_source_url)
        else:
            owid_df_csv = paths['raw_file_path']
        owid_df = get_dataframe(owid_df_csv)
        cleaned_owid_df = get_cleaned_owid_df(owid_df)
        save_cleaned_df(cleaned_owid_df, paths['cleaned_file_path'])
    else:
        cleaned_owid_df = pd.read_csv(paths['cleaned_file_path'])
    process_bulk_es_data(cleaned_owid_df)


