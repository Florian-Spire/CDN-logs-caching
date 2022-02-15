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

def es_cache_hit_ratio(es, index):
    page = es.search(index=index, scroll = '1m', _source=["hit"], query={"match_all": {}}, size=10000, sort=[{"@timestamp": {"order": "asc"}}], version=False)
    sid = page['_scroll_id']
    print("Total number of results: ", page['hits']['total']['value'])
    total_size=0
    miss=0
    hit=0
    
    for i in range(len(page['hits']['total'])):
        total_size+=1
        if page["hits"]["hits"][i]["_source"]["hit"]== "miss":
            miss+=1
        elif page["hits"]["hits"][i]["_source"]["hit"]== "hit":
            hit+=1

    while (len(page['hits']['total']) > 0):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            page = es.scroll(scroll_id = sid, scroll = '1m', request_timeout = 30)
        # Update the scroll ID
        sid = page['_scroll_id']
        # Get the number of results that we returned in the last scroll
        for i in range(len(page['hits']['total'])):
            total_size+=1
            if page["hits"]["hits"][i]["_source"]["hit"]== "miss":
                miss+=1
            elif page["hits"]["hits"][i]["_source"]["hit"]== "hit":
                hit+=1
        print("Cache hit ratio: ", hit/(hit+miss)*100, "%")
        print("Progression: ", round(total_size/page['hits']['total']['value']*100, 2), "%\n")

    es.clear_scroll(body={'scroll_id': sid})
    print("Number of hits: ", hit)
    print("Number of miss: ", miss)
    print("Total size : ", total_size)
    print("Final cache hit ratio: ", hit/(hit+miss)*100, "%")
    return hit/(hit+miss)

if __name__ == "__main__":
    es = connect_elasticsearch("192.168.100.146", 9200)
    #es_scroll(es, "batch3-2088.05.13.01").to_csv("logs_scroll_export.csv", sep=",", index=False) # CSV delimited by commas
    #print("1/2: ", len(es_scroll(es, "batch3-2088.05.13.01").index))
    print("Cache hit ratio : ", es_cache_hit_ratio(es, "batch3-*"))