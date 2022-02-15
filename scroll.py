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
    page = es.search(index = index,
            scroll = '1m',
            body={},
            version = False,
            size = 10000)
    sid = page['_scroll_id']
    scroll_size = len(page['hits']['total'])

    # Start scrolling
    df = pandas.DataFrame()
    appended_data = []
    test=0
    while (scroll_size > 0):
        frame = pandas.DataFrame.from_dict([document['_source'] for document in page["hits"]["hits"]])
        #appended_data.append(frame)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            page = es.scroll(scroll_id = sid, scroll = '1m', request_timeout = 30)
        # Update the scroll ID
        sid = page['_scroll_id']
        # Get the number of results that we returned in the last scroll
        scroll_size = len(page['hits']['hits'])
        test+=len(frame.index)
    if len(appended_data) > 0: 
        df = pandas.concat(appended_data, ignore_index=True, sort = False)
    del appended_data
    gc.collect() 
    es.clear_scroll(body={'scroll_id': sid})
    print("Taille totale : ", test)
    return df

if __name__ == "__main__":
    es = connect_elasticsearch("192.168.100.146", 9200)
    #es_scroll(es, "batch3-2088.05.13.01").to_csv("logs_scroll_export.csv", sep=",", index=False) # CSV delimited by commas
    #print("1/2: ", len(es_scroll(es, "batch3-2088.05.13.01").index))
    print("2/2: ", len(es_scroll(es, "batch3-*").index))