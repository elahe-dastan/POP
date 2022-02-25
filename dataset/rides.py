class RideInfo:
    def __init__(self, hit, le):
        hit = hit['_source']
        self.eta = int(hit['eta_comp_result']['smapp'])

        self.iran_day_of_week = le[hit['iran_day_of_week']]

        self.hour = hit['iran_time_of_day'].split(":")[0]
        self.minute = hit['iran_time_of_day'].split(":")[1]
        self.iran_time_of_day = int(self.hour) + int(self.minute) / 60

        self.ata = int(hit['ata_duration'])
        self.confidence = float(hit['confidence'])

        hit = hit['ride']
        self.origin_lat = hit['origin']['lat']
        self.origin_lon = hit['origin']['lon']

        self.dest_lat = hit['destination']['lat']
        self.dest_lon = hit['destination']['lon']


class Rides:
    def __init__(self, res, le):
        res = res['hits']['hits']
        self.hits = []
        for hit in res:
            self.hits.append(RideInfo(hit, le))


class RideDataset:
    def __init__(self, elastic_connection):
        # Elastic search server is configured to return 10000 results at most
        self.MAX_HIT = 10000
        self._elastic_connection = elastic_connection
        self.le = {"SATURDAY": 0,
                   "SUNDAY": 1,
                   "MONDAY": 2,
                   "TUESDAY": 3,
                   "WEDNESDAY": 4,
                   "THURSDAY": 5,
                   "FRIDAY": 6
                   }

    def fetch_rides(self, time_least, time_most):
        index = "farsanj-results-*"
        res = self._elastic_connection.search(index=index, body={
            "size": self.MAX_HIT,
            "query": {
                "bool": {
                    "must": [
                        {
                            "range": {
                                "timestamp": {
                                    "gte": time_least,
                                    "lte": time_most
                                }
                            }
                        },
                        {
                            "term": {
                                "origin_city_id": 1
                            }
                        }
                    ]
                }
            }
        })

        return Rides(res, self.le).hits
