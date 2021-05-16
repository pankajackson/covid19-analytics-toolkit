from elasticsearch import Elasticsearch, helpers
from retrying import retry

from datetime import datetime


data = [
    {
        "fname": "pankaj",
        "lname": "jackson",
        "ph": 1
    },
    {
        "fname": "pankaj1",
        "lname": "jackson1",
        "ph": 11
    },
    {
        "fname": "pankaj2",
        "lname": "jackson2",
        "ph": 12
    },
    {
        "fname": "pankaj3",
        "lname": "jackson3",
        "ph": 13
    },
]


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



actions = []
for i in data:
    action = {
        "_index": "tickets-index-1",
        "_type": "tickets",
        "_id": str(datetime.now().timestamp()),
        "_source": {
            "data": i,
            }
        }
    action["_source"]['@timestamp'] = datetime.strptime('2021-05-17 00:00:00', '%Y-%m-%d %H:%M:%S')
    actions.append(action)
    res = es.index(index=action['_index'], id=action['_id'], doc_type=action['_type'], body=action['_source'])
    print(res['result'])

# res = helpers.bulk(es, actions)
# print(res)