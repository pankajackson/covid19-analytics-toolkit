FROM python

WORKDIR /usr/src/app
COPY script.py ./
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

ENV SOURCE_DATA_URL 'https://covid.ourworldindata.org/data/owid-covid-data.csv'
ENV SOURCE_DATA_DIR '/data/raw/'
ENV SOURCE_DATA_FILE 'owid-covid-data.csv'
ENV DEST_CLEAN_DATA_DIR '/data/cleaned/'
ENV ES_URL 'elasticsearch:9200'

CMD [ "python", "./script.py" ]