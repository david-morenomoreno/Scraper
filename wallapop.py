from elasticsearch import Elasticsearch
import time, json, simplejson, urllib, random

import requests
import concurrent.futures




class Wallapop(object):

    def __init__(self):

        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=10)


        self.elastic_host = "localhost"
        self.elastic_port = 9200

        self.session = requests.Session()



        self.BASE_URL = 'http://pro2.wallapop.com/shnm-portlet/api/v1/'

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
        self.elastic = Elasticsearch([{'host': self.elastic_host, 'port': self.elastic_port}])


    def InsertElasticsearch(self,uid, jsonitem ):
        self.elastic.index(index='wallapop', doc_type='doc', body=simplejson.dumps(jsonitem))
        print("Usuario con id "+str(uid)+ " Insertado con exito")



    def User(self, uid):
        resp = self.session.get(self.BASE_URL+'user.json/' + str(uid) + "?")

        if resp.status_code == 200:
            jsonitem = json.loads(resp.text)
            jsonitem['date'] = int(time.time())

            try:
                Lat = jsonitem['location']['approximatedLatitude']
                Lon = jsonitem['location']['approximatedLongitude']
                jsonitem['geopoint'] = str(str(Lat) + ',' + str(Lon))
            except:
                pass

            self.executor.submit(self.InsertElasticsearch(uid, jsonitem))


        else:
            print("El usuario " +str(uid)+ " no existe")



if __name__ == "__main__":

    RangeMin = 490762
    RangeMax = 99999999


    wal = Wallapop()

    for uid in range(RangeMin, RangeMax):
        wal.User(uid)




