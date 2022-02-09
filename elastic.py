import csv, json, pandas
from elasticsearch import Elasticsearch


def connect_elasticsearch():
    es = Elasticsearch(['192.168.100.146:9200'], timeout=30)
    if es.ping():
        print('Elastic search is Connected ')
    else:
        print('Elastic search could not connect!')
    return es

es = connect_elasticsearch()
result = es.search(index='batch3-2088.05.13.01', size=10000)
elastic_docs = result["hits"]["hits"]

docs = pandas.DataFrame()

# iterate each Elasticsearch doc in list
for num, doc in enumerate(elastic_docs):
    # get _source data dict from document
    source_data = doc["_source"]

    # get _id from document
    _id = doc["_id"]

    # create a Series object from doc dict object
    doc_data = pandas.Series(source_data, name = _id)

    # append the Series object to the DataFrame object
    docs = docs.append(doc_data)

docs.to_csv("logs_export.csv", sep=",", index=False) # CSV delimited by commas

# export Elasticsearch documents to CSV
csv_export = docs.to_csv(sep=",") # CSV delimited by commas
print ("\nCSV data:", csv_export)