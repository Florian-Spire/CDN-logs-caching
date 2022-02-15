import pandas, gc, warnings
from elasticsearch import Elasticsearch

def connect_elasticsearch(domain, port):
    host = domain + ":" + str(port)
    es = Elasticsearch([host], timeout = 30, max_retries=10, retry_on_timeout=True)
    if es.ping():
        print('Elastic search is Connected ')
    else:
        print('Elastic search could not connect!')
    return es

def es_scroll(es, index):
    page = es.search(index=index, scroll = '1m', _source=["@timestamp", "maxage"], query={"match_all": {}}, size=10000, sort=[{"@timestamp": {"order": "asc"}}], version=False)
    sid = page['_scroll_id']
    scroll_size = len(page['hits']['total'])
    taille=scroll_size
    while (scroll_size > 0):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            page = es.scroll(scroll_id = sid, scroll = '1m', request_timeout = 30)
        # Update the scroll ID
        sid = page['_scroll_id']
        # Get the number of results that we returned in the last scroll
        scroll_size = len(page['hits']['hits'])
        taille+=scroll_size
        print(taille)
    es.clear_scroll(body={'scroll_id': sid})
    print("Taille totale : ", taille)

if __name__ == "__main__":
    es = connect_elasticsearch("192.168.100.146", 9200)
    #es_scroll(es, "batch3-2088.05.13.01").to_csv("logs_scroll_export.csv", sep=",", index=False) # CSV delimited by commas
    #print("1/2: ", len(es_scroll(es, "batch3-2088.05.13.01").index))
    es_scroll(es, "batch3-*")