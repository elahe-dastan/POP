import h3
import numpy as np
from torch.utils.data import Dataset


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
        # geo_to_h3 function returns the index in hexadecimal, I convert it to decimal
        # Our case study is just Tehran and al the indexes in Tehran start with 6177839 so we just store the remainder
        self.origin_index = int(h3.geo_to_h3(lat=self.origin_lat, lng=self.origin_lon, resolution=9),
                                16) % 617783900000000000

        self.dest_lat = hit['destination']['lat']
        self.dest_lon = hit['destination']['lon']
        # geo_to_h3 function returns the index in hexadecimal, I convert it to decimal
        # Our case study is just Tehran and al the indexes in Tehran start with 6177839 so we just store the remainder
        self.dest_index = int(h3.geo_to_h3(lat=self.dest_lat, lng=self.dest_lon, resolution=9), 16) % 617783900000000000


class Rides:
    def __init__(self, res, le):
        res = res['hits']['hits']
        self.hits = []
        self.column_data = []
        for hit in res:
            ride_info = RideInfo(hit, le)
            self.hits.append(ride_info)
            self.column_data.append([ride_info.eta, ride_info.iran_day_of_week, ride_info.iran_time_of_day, ride_info.origin_index, ride_info.dest_index, ride_info.ata])


class SampleDataset(Dataset):
    def __init__(self, rides, mean, std):
        self.rides = rides
        self.mean = mean
        self.std = std

    def __len__(self):
        return len(self.rides)

    def __getitem__(self, idx):
        ride = self.rides[idx]
        features = np.array([ride.eta, ride.iran_day_of_week, ride.iran_time_of_day, ride.origin_index, ride.dest_index])
        target = ride.ata
        normalized_features = (features - self.mean[0:-1]) / self.std[0:-1]
        normalized_target = (ride.ata - self.mean[-1]) / self.std[-1]
        return normalized_features, normalized_target


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

        self.mean = None
        self.std = None

    def fetch_rides(self, time_least, time_most, mean=None, std=None):
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
                        {"term": {"origin_city_id": 1}}
                    ]
                }
            }
        })

        rides = Rides(res, self.le)

        if mean is None:
            column_data = np.array(rides.column_data)
            self.mean = column_data.mean(axis=0)
            self.std = column_data.std(axis=0)
        else:
            self.mean = mean
            self.std = std

        dataset = SampleDataset(rides.hits, self.mean, self.std)

        if mean is None:
            return dataset, self.mean, self.std

        return dataset
