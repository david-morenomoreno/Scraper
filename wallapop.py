from elasticsearch import Elasticsearch
import time, json, simplejson, urllib
from constants import *



# *Nico* The id min-max for spain is: 39960000 - 40001797
RangeMin = 39960000
RangeMax = 40001797



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
            request = urllib.request.urlopen("http://pro2.wallapop.com/shnm-portlet/api/v1/user.json/" + str(uid) + "?").read()
            response = request.decode('utf-8')
            jsonitem = json.loads(response)

            try:
                Lat = jsonitem['location']['approximatedLatitude']
                Lon = jsonitem['location']['approximatedLongitude']
                jsonitem['geopoint'] = str(str(Lat)+','+str(Lon))
            except:
                pass

            jsonitem['date'] = int(time.time())
            elastic.index(index='wallapop', doc_type='doc', body=simplejson.dumps(jsonitem))
            print(response)

        except:
            print(str(uid) + " does not exist")





