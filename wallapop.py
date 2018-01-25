import random

from elasticsearch import Elasticsearch
import time, json, simplejson, urllib, requests
from constants import *



# *Nico* The id min-max for spain is: 39960000 - 40001797
RangeMin = 39962140 #39960000
RangeMax = 40001797

url = 'http://pro2.wallapop.com/shnm-portlet/api/v1/user.json/'

proxies = {
    'http': 'socks5://127.0.0.1:9150',
    'https': 'socks5://127.0.0.1:9150'
}



if __name__ == "__main__":

    # Inicializacion elastic_host
    es = Elasticsearch()
    profile = {
        "mappings": {
            "wallapop": {
                "properties": {
                    "date": {
                        "type": "date",
                        "format": "epoch_second"
                    },
                    "geopoint": {
                        "type": "geo_point",
                    },
                }
            },
        }
    }

    es.indices.create(index="wallapop", ignore=400, body=profile)
    elastic = Elasticsearch([{'host': elastic_host, 'port': elastic_port}])


    # Initialized Scraper
    for uid in range(RangeMin, RangeMax):
        try:
            request = (urllib.request.urlopen(url + str(uid) + "?").read()).decode('utf-8')
            jsonitem = json.loads(request)

            try:
                Lat = jsonitem['location']['approximatedLatitude']
                Lon = jsonitem['location']['approximatedLongitude']
                jsonitem['geopoint'] = str(str(Lat)+','+str(Lon))
            except:
                pass

            jsonitem['date'] = int(time.time())
            elastic.index(index='wallapop', doc_type='doc', body=simplejson.dumps(jsonitem))
            print(request)

        except:
            print(str(uid) + " does not exist")

        time.sleep(random.randint(0, 1))





