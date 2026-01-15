import json
import os
from typing import List

from elasticsearch import Elasticsearch, helpers
from tqdm import tqdm

"""
ES Support Matrix: https://www.elastic.co/cn/support/matrix#matrix_jvm
Use: ES 8.14.1 + JDK 21

Download JDK 21: 
wget -c https://download.oracle.com/java/21/archive/jdk-21.0.2_linux-x64_bin.tar.gz
tar -xvzf jdk-21.0.2_linux-x64_bin.tar.gz -C ~/opt/
export ES_JAVA_HOME=~/opt/jdk-21.0.2

Download ES 8.14.1:
wget -c https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-8.14.1-linux-x86_64.tar.gz
tar -xvzf elasticsearch-8.14.1-linux-x86_64.tar.gz -C ~/opt/
mv ~/opt/elasticsearch-8.14.1 ~/opt/elasticsearch

cd ~/opt/elasticsearch

add to config/elasticsearch.yml:
path.data: data
path.logs: logs
network.host: 0.0.0.0
http.port: 9200

modify config/jvm.options:
-Xms8g
-Xmx8g

./bin/elasticsearch
save the password and ca_certs path to ~/.es_config.json:
{"ca_certs":"/home/{yourname}/opt/elasticsearch/config/certs/http_ca.crt","pwd":"xxx"}
"""

DEFAULT_SETTING = {
    "number_of_shards": 1,
    "number_of_replicas": 0,
    "analysis": {
        "analyzer": {
            "std_folded": {
                "type": "custom",
                "tokenizer": "standard",
                "filter": ["lowercase", "asciifolding"],
            }
        }
    },
}


class ESClient:
    def __init__(
        self, url="https://localhost:9200", username=None, password=None, ca_certs=None
    ) -> None:
        if username and password and ca_certs:
            _basic_auth = (username, password)
            _ca_certs = ca_certs
        else:
            _home = os.environ.get("HOME")
            _c = json.load(open(os.path.join(_home, ".es_config.json")))
            _basic_auth = ("elastic", _c["pwd"])
            _ca_certs = _c["ca_certs"]

        self.client = Elasticsearch(
            url,
            ca_certs=_ca_certs,
            basic_auth=_basic_auth,
            timeout=300,
        )
        assert self.test_connection(), "ES connection failed!"

    def create_index(self, index, mappings, settings=None):
        settings = settings or DEFAULT_SETTING
        response = self.client.options(ignore_status=[400]).indices.create(
            index=index, mappings=mappings, settings=settings
        )
        return response

    def bulk_insert(self, actions: List[dict], thread_count=8):
        for idx, (success, info) in enumerate(
            tqdm(
                helpers.parallel_bulk(
                    self.client, actions, thread_count=thread_count, chunk_size=5000
                ),
                colour="green",
                ncols=100,
                desc="ES bulk insert",
                total=len(actions),
            ),
            start=1,
        ):
            if not success:
                print("A document failed:", info)
            elif idx % 100000 == 0:
                self.client.indices.refresh()
        self.client.indices.refresh()
        # return helpers.bulk(self.client, actions)

    def search(self, index, *arg, **kargs):
        return self.client.search(index=index, *arg, **kargs)

    def msearch(self, index: str, queries: List[dict], size=10):
        body = []
        for query in queries:
            body.append({"index": index})
            body.append({"query": query, "size": size})
        res = self.client.msearch(body=body)
        responses = res.body["responses"]
        return responses

    def delete_index(self, index):
        return self.client.options(ignore_status=[400, 404]).indices.delete(index=index)

    def test_connection(self):
        if self.client.ping():
            return True
        else:
            return False

    def connection_info(self):
        return self.client.info()

    def count(self, index):
        return self.client.count(index=index)


def text_usage():
    from datetime import datetime

    es = ESClient()
    # test all functions
    INDEX = "test-index"
    # print(es.delete_index(INDEX))
    mappings = {
        "properties": {
            "name": {"type": "text"},
            "age": {"type": "integer"},
            "created": {
                "type": "date",
                "format": "strict_date_optional_time||epoch_millis",
            },
        }
    }
    print(es.create_index(INDEX, mappings))
    actions = [
        {
            "_id": 2,
            "_index": INDEX,
            "name": "tom",
            "age": 17,
            "created": datetime.now(),
        },
        {
            "_id": 3,
            "_index": INDEX,
            "name": "lucy",
            "age": 15,
            "created": datetime.now(),
        },
    ]
    print(es.bulk_insert(actions))
    print(es.search(INDEX, query={"match": {"name": "tom"}}))
    print(es.delete_index(INDEX))


if __name__ == "__main__":
    text_usage()
