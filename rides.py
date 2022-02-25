class RideInfo:
    def __init__(self, hit):
        hit = hit['_source']
        self.eta = hit['eta_comp_result']['smapp']

        self.iran_day_of_week = hit['iran_day_of_week']
        self.iran_time_of_day = hit['iran_time_of_day']

        self.ata = hit['ata_duration']

        hit = hit['ride']
        self.origin_lat = hit['origin']['lat']
        self.origin_lon = hit['origin']['lon']

        self.dest_lat = hit['destination']['lat']
        self.dest_lon = hit['destination']['lon']


class Rides:
    def __init__(self, res):
        res = res['hits']['hits']
        self.hits = []
        for hit in res:
            self.hits.append(RideInfo(hit))


class RideDataset:
    def __init__(self, elastic_connection):
        # Elastic search server is configured to return 10000 results at most
        self.MAX_HIT = 10000
        self._elastic_connection = elastic_connection

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
                        }
                    ]
                }
            }
        })

        return Rides(res)