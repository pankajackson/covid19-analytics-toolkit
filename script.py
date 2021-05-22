import pandas as pd
import numpy as np
from elasticsearch import Elasticsearch, helpers
from datetime import datetime
from retrying import retry
import requests
import os
from urllib.parse import urlparse
import matplotlib.pyplot as plt
import seaborn as sns
from geopy.geocoders import Nominatim

data_source = 'cssegisanddata'
source_data_url = {
    "owid": os.getenv('SOURCE_DATA_URL', "https://covid.ourworldindata.org/data/owid-covid-data.csv"),
    "cssegisanddata": os.getenv('SOURCE_DATA_URL', "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/05-19-2021.csv"),
}
source_data_dir = os.getenv('SOURCE_DATA_DIR', 'data/raw/%s' % data_source)
dest_clean_data_dir = os.getenv('DEST_CLEAN_DATA_DIR', 'data/cleaned/%s' % data_source)

# Download Data
@retry(stop_max_attempt_number=3, wait_fixed=10000)
def download_data(url=source_data_url['cssegisanddata']):
    print('Downloading Data...')
    if not os.path.exists(os.path.abspath(source_data_dir)):
        os.makedirs(os.path.abspath(source_data_dir))
    source_data_file = os.path.basename(urlparse(url).path)
    abs_data_path = os.path.join(os.path.abspath(source_data_dir), datetime.now().strftime("%Y%m%d__%H%M") + '_' + source_data_file)
    url_content = requests.get(url).content
    file = open(abs_data_path, 'wb')
    file.write(url_content)
    file.close()
    return abs_data_path

# Load DataSet
def get_dataframe(path):
    print('Reading Data...')
    df = pd.read_csv(path)
    print(df.isnull().sum().sum())
    return df


def clean_csse_data(df):
    for i, row in df[df['Lat'].isnull()].iterrows():

        geolocator = Nominatim(user_agent="python")
        try:
            country = row['Country_Region']
            city = row['Province_State']
            print(country,city)
            loc = geolocator.geocode(row['Combined_Key'])
            if not loc.latitude or not loc.longitude:
                loc = geolocator.geocode(city + ',' + country)
            df.at[i, 'Lat'] = loc.latitude
            df.at[i, 'Long_'] = loc.longitude
            print('Setting LAT, LONG')

        except:
            if not row['Country_Region']:
                df.at[i, 'Country_Region'] = 'Unknown'
            if not row['Province_State']:
                df.at[i, 'Province_State'] = 'Unknown'
            print('Setting CON, STATE')
    return df

data_csv = download_data()
df = get_dataframe(data_csv)
print('Null: ', df.isnull().sum().sum())
df = clean_csse_data(df)
print('Null: ', df.isnull().sum().sum())
print(df.shape)
print(df[df['Lat'].isnull()][['Country_Region', 'Province_State']])



# geolocator = Nominatim(user_agent="my_user_agent")
# country ="Canada"
# city ="Repatriated Travellers"
# loc = geolocator.geocode(city+','+ country)
# print("latitude is :-" ,loc.latitude,"\nlongtitude is:-" ,loc.longitude)



# Plot Heatmap to show Missing values
# sns.heatmap(df.isnull())
#
#
# plt.show()