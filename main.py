import json
import os

from elasticsearch import Elasticsearch


def get_elasticsearch_client():
    host = os.getenv('ES_HOST', 'localhost')
    port = int(os.getenv('ES_PORT', 9200))
    scheme = 'https'
    
    username = os.getenv('ES_USERNAME', 'elastic')
    credentials = os.getenv('ES_PASSWORD')
    # headers = {
    #     'Authorization': f'Basic elastic {credentials}'
    #     }
    
    es = Elasticsearch(
            hosts=[{'host': host, 'port': port, 'scheme': scheme}],
            # headers=headers,
            verify_certs=False,
            http_auth=(username, credentials)
            )
    
    return es


def execute_query(es, index_name):
    with open('query.json', 'r') as f:
        my_query = f.read()
    query = json.loads(my_query)
    query["query"]["bool"]["filter"][1]["term"]["source.id"] = os.getenv('SOURCE_ID')
    response = es.search(index=index_name, body=query)
    return response


def main():
    es = get_elasticsearch_client()
    index_name = 'prod-08x.01506.tracardi-event-2023-10'
    response = execute_query(es, index_name)
    print(response)


if __name__ == '__main__':
    main()
