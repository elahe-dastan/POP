import pandas as pd
from clickhouse_driver import Client
import csv

# map user
# client = Client(host='172.21.16.1',
#                 user='map_user',
#                 password='6zeQUqCj4y7puMxbBCeN')

# koochooloo user
client = Client(host='172.21.16.1',
                user='elahe_dastan',
                password='hh@bSgJ#6j%XUEyUPlYB')


# '2022-12-10','2022-12-11','2022-12-12','2022-12-13','2022-12-14','2022-12-17','2022-12-18','2022-12-19','2022-12-20','2022-12-21','2022-12-24','2022-12-25','2022-12-26', '2022-12-31',
# dates = ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04']
#
# khatkesh_query = "SELECT * FROM snapp_raw_log.khatkesh_result_proto WHERE toDate(clickhouse_time, 'Iran') BETWEEN '{start_date}' AND '{finish_date}' AND toHour(clickhouse_time, 'Iran') BETWEEN {start_hour} AND {finish_hour} AND toMinute(clickhouse_time, 'Iran') BETWEEN {start_minute} AND {finish_minute}"
# for date in dates:
#     with open('../khatkesh_' + date + '_.csv', 'w') as f:
#         write = csv.writer(f)
#         for hour in range(24):
#             for min in range(0, 60, 5):
#                 print(khatkesh_query.format(start_date=date, finish_date=date, start_hour=hour, finish_hour=hour, start_minute=min, finish_minute=min+5))
#                 khatkesh = client.execute(khatkesh_query.format(start_date=date, finish_date=date, start_hour=hour, finish_hour=hour, start_minute=min, finish_minute=min+5))
#                 print("data: " + date)
#                 print("hour: " + str(hour))
#                 print("from " + str(min) + " minute to " + str(min+5) + " minute")
#                 print("number of data: " + str(len(khatkesh)))
#                 write.writerows(khatkesh)


# b = ['ride_id','driver_id','a_t_a_result.arrival_a_t_a','a_t_a_result.boarding_a_t_a','a_t_a_result.ride_a_t_a','a_t_a_result.arrival_probe_result.probe.point.lat','a_t_a_result.arrival_probe_result.probe.point.lon','a_t_a_result.arrival_probe_result.probe.timestamp','a_t_a_result.arrival_probe_result.confidence','a_t_a_result.arrival_probe_result.h3_index', 'a_t_a_result.arrival_probe_result.k_ring_level','a_t_a_result.boarding_probe_result.probe.point.lat','a_t_a_result.boarding_probe_result.probe.point.lon','a_t_a_result.boarding_probe_result.probe.timestamp','a_t_a_result.boarding_probe_result.confidence','a_t_a_result.boarding_probe_result.h3_index','a_t_a_result.boarding_probe_result.k_ring_level','a_t_a_result.final_destination_probe_result.probe.point.lat','a_t_a_result.final_destination_probe_result.probe.point.lon','a_t_a_result.final_destination_probe_result.probe.timestamp','a_t_a_result.final_destination_probe_result.confidence','a_t_a_result.final_destination_probe_result.h3_index','a_t_a_result.final_destination_probe_result.k_ring_level','a_t_a_result.destination_probe_result.probe.point.lat','a_t_a_result.destination_probe_result.probe.point.lon','a_t_a_result.destination_probe_result.probe.timestamp','a_t_a_result.destination_probe_result.confidence','a_t_a_result.destination_probe_result.h3_index','a_t_a_result.destination_probe_result.k_ring_level','a_t_a_result.extra_destination_probe_result.probe.point.lat','a_t_a_result.extra_destination_probe_result.probe.point.lon','a_t_a_result.extra_destination_probe_result.probe.timestamp','a_t_a_result.extra_destination_probe_result.confidence','a_t_a_result.extra_destination_probe_result.h3_index','a_t_a_result.extra_destination_probe_result.k_ring_level','pickup_a_d_d_result.distance','pickup_a_d_d_result.confidence','pickup_a_d_d_result.route_ratio','pickup_a_d_d_result.g_p_s_ratio','ride_a_d_d_result.distance','ride_a_d_d_result.confidence','ride_a_d_d_result.route_ratio','ride_a_d_d_result.g_p_s_ratio','total_a_d_d_confidence','in_ride_allotment','e_d_d','clickhouse_time','hash']
# print(len(b))

# rides_query = "select accepted_driver_id, created_at, passenger_id, source_lat, source_lng, destination_lat, destination_lng, argMin (eta, datediff ('minute', request_time, created_at)) as eta, provider, ata, id, city from (select distinct passenger_id, source_lat, source_lng, destination_lat, destination_lng, toDate (timestamp_time, 'Iran') as date1, timestamp_time, provider, max(eta) as eta, city_id as city, request_time from snapp_raw_log.eta_errors ee where toDate (timestamp_time, 'Iran') Between '2022-09-07' and '2022-09-07' and toHour (timestamp_time, 'Iran') Between 17 and 19 and provider = 'smapp-same-dc' and city = 1 group by source_lat, source_lng, destination_lat, destination_lng, date1, provider, city, passenger_id, timestamp_time, request_time having eta > 0) as provider_table INNER join (select created_at, passenger_id, id, origin_lng, origin_lat, destination_lng, destination_lat, round(datediff (second, min(updated_at), max(updated_at)), 2) as ata, max(estimated_distance) / 1000 as distance, (max(estimated_distance) / 1000) / (round(datediff (second, min(updated_at), max(updated_at)), 2) / 3600) as speed, accepted_driver_id from (select passenger_id, id, estimated_distance, created_at, origin_lat, origin_lng, destination_lat, destination_lng, estimated_distance, accepted_driver_id from snapp_mysql.rides_view where toDate (created_at, 'Iran') BETWEEN '2022-09-07' and '2022-09-07' and toHour (created_at, 'Iran') Between 17 and 19 and service_type = 1 and extra_destination_lng = 0) as a INNER JOIN (select DISTINCT ride_id, event_type, created_at, updated_at from snapp_mysql.ride_events where toDate (created_at, 'Iran') BETWEEN '2022-09-07' and '2022-09-07' and toHour (created_at, 'Iran') Between 17 and 19 AND event_type in (4, 5)) as b on a.id = b.ride_id group by passenger_id, created_at, origin_lng, origin_lat, destination_lng, destination_lat, id, accepted_driver_id having ata > 0 and speed >= 5 and speed <= 80) as ata_table on round(ata_table.origin_lat, 8) = round(provider_table.source_lat, 8) and round(ata_table.origin_lng, 8) = round(provider_table.source_lng, 8) and round(ata_table.destination_lat, 8) = round(provider_table.destination_lat, 8) and round(ata_table.destination_lng, 8) = round(provider_table.destination_lng, 8) and ata_table.passenger_id = provider_table.passenger_id where datediff ('minute', request_time, created_at) between -0.0001 and 5 group by created_at, passenger_id, provider, ata, id, city, accepted_driver_id, source_lat, source_lng, destination_lat, destination_lng"


# rides_query = "select accepted_driver_id, created_at, passenger_id, source_lat, source_lng, destination_lat, destination_lng, argMin (eta, datediff ('minute', request_time, created_at)) as eta, provider, ata, id, city from (select distinct passenger_id, source_lat, source_lng, destination_lat, destination_lng, toDate (timestamp_time, 'Iran') as date1, timestamp_time, provider, max(eta) as eta, city_id as city, request_time from snapp_raw_log.eta_errors ee where toDate (timestamp_time, 'Iran') Between '{start_date}' and '{finish_date}' and provider = 'smapp-same-dc' and city = 1 group by source_lat, source_lng, destination_lat, destination_lng, date1, provider, city, passenger_id, timestamp_time, request_time having eta > 0) as provider_table INNER join (select created_at, passenger_id, id, origin_lng, origin_lat, destination_lng, destination_lat, round(datediff (second, min(updated_at), max(updated_at)), 2) as ata, max(estimated_distance) / 1000 as distance, (max(estimated_distance) / 1000) / (round(datediff (second, min(updated_at), max(updated_at)), 2) / 3600) as speed, accepted_driver_id from (select passenger_id, id, estimated_distance, created_at, origin_lat, origin_lng, destination_lat, destination_lng, estimated_distance, accepted_driver_id from snapp_mysql.rides_view where toDate (created_at, 'Iran') BETWEEN '{start_date}' and '{finish_date}' and service_type = 1 and extra_destination_lng = 0) as a INNER JOIN (select DISTINCT ride_id, event_type, created_at, updated_at from snapp_mysql.ride_events where toDate (created_at, 'Iran') BETWEEN '{start_date}' and '{finish_date}' AND event_type in (4, 5)) as b on a.id = b.ride_id group by passenger_id, created_at, origin_lng, origin_lat, destination_lng, destination_lat, id, accepted_driver_id having ata > 0 and speed >= 5 and speed <= 80) as ata_table on round(ata_table.origin_lat, 8) = round(provider_table.source_lat, 8) and round(ata_table.origin_lng, 8) = round(provider_table.source_lng, 8) and round(ata_table.destination_lat, 8) = round(provider_table.destination_lat, 8) and round(ata_table.destination_lng, 8) = round(provider_table.destination_lng, 8) and ata_table.passenger_id = provider_table.passenger_id where datediff ('minute', request_time, created_at) between -0.0001 and 5 group by created_at, passenger_id, provider, ata, id, city, accepted_driver_id, source_lat, source_lng, destination_lat, destination_lng"
# for date in dates:
#     with open('../rides_' + date + '.csv', 'w') as f:
#         rides = client.execute(rides_query.format(start_date=date, finish_date=date))
#         print(date)
#         print(len(rides))
#
#         write = csv.writer(f)
# #     # write.writerow(fields)
#         write.writerows(rides)

# import pandas as pd
# import pyarrow
#
# df = pd.read_csv('../rides_test.csv')
# df.to_parquet('../rides_test.parquet')


# x = client.execute("SELECT * FROM snapp_raw_log.khatkesh_result_proto WHERE toDate(clickhouse_time, 'Iran') BETWEEN '2023-01-03' AND '2023-01-03' AND a_t_a_result.extra_destination_probe_result.confidence > 0;")
# print(x)
#
#
#
# -- Note: Unless you save your query, these tabs will NOT persist if you clear your cookies or change browsers.
#
# SELECT * FROM snapp_raw_log.ride WHERE created_date = '2023-01-03' AND ride_id = 5607673388;


# x = client.execute("SELECT * FROM snapp_mysql.ride_events WHERE toDate(created_at, 'Iran') BETWEEN '2023-01-04' AND '2023-01-04' AND ride_id= 5611345001;")
# print(x)


# dates = ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04']
#
# waiting_query = "SELECT id, waiting_price, waiting FROM snapp_mysql.rides_view WHERE toDate(created_date, 'Iran') BETWEEN '{start_date}' AND '{finish_date}' AND waiting_price == 1;"
# for date in dates:
#     with open('../waiting_' + date + '.csv', 'w') as f:
#         write = csv.writer(f)
#         print(waiting_query.format(start_date=date, finish_date=date))
#         waiting = client.execute(waiting_query.format(start_date=date, finish_date=date))
#         print("data: " + date)
#         print("number of data: " + str(len(waiting)))
#         write.writerows(waiting)

# dates = ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04']
#
# round_trip_query = "SELECT id, round_trip_price FROM snapp_mysql.rides_view WHERE toDate(created_date, 'Iran') BETWEEN '{start_date}' AND '{finish_date}' AND round_trip_price != 0;"
# for date in dates:
#     with open('../round_trip_' + date + '.csv', 'w') as f:
#         write = csv.writer(f)
#         print(round_trip_query.format(start_date=date, finish_date=date))
#         round_trip = client.execute(round_trip_query.format(start_date=date, finish_date=date))
#         print("data: " + date)
#         print("number of data: " + str(len(round_trip)))
#         write.writerows(round_trip)


# dates = ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04']
#
# cash_query = "SELECT id, passenger_must_pay_cash FROM dwh.snapp_rides WHERE toDate(created_date, 'Iran') BETWEEN '{start_date}' AND '{finish_date}' AND passenger_must_pay_cash > 0;"
# for date in dates:
#     with open('../cash_' + date + '.csv', 'w') as f:
#         write = csv.writer(f)
#         print(cash_query.format(start_date=date, finish_date=date))
#         cash = client.execute(cash_query.format(start_date=date, finish_date=date))
#         print("data: " + date)
#         print("number of data: " + str(len(cash)))
#         write.writerows(cash)


# dates = ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04']
#
# extra_destination_query = "SELECT id, extra_destination_price FROM snapp_mysql.rides_view WHERE toDate(created_date, 'Iran') BETWEEN '{start_date}' AND '{finish_date}' AND extra_destination_price != 0;"
# for date in dates:
#     with open('../extra_destination_' + date + '.csv', 'w') as f:
#         write = csv.writer(f)
#         print(extra_destination_query.format(start_date=date, finish_date=date))
#         extra_destination = client.execute(extra_destination_query.format(start_date=date, finish_date=date))
#         print("data: " + date)
#         print("number of data: " + str(len(extra_destination)))
#         write.writerows(extra_destination)


# dates = ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04']
#
# eta_query = "SELECT ride_id FROM snapp_raw_log.ride_eta WHERE toDate(created_date, 'Iran') BETWEEN '{start_date}' AND '{finish_date}' AND toHour(timestamp_time, 'Iran') BETWEEN {start_hour} AND {finish_hour};"
# for date in dates:
#     with open('../eta_' + date + '.csv', 'w') as f:
#         write = csv.writer(f)
#         for hour in range(24):
#             print(eta_query.format(start_date=date, finish_date=date, start_hour=hour, finish_hour=hour))
#             eta = client.execute(eta_query.format(start_date=date, finish_date=date, start_hour=hour, finish_hour=hour))
#             print("data: " + date)
#             print("number of data: " + str(len(eta)))
#             write.writerows(eta)


# cash
# a = client.execute("SELECT id FROM dwh.snapp_rides LIMIT 3;")
# print(a)

# fraud
# dates = ['2023-01-01', '2023-01-02', '2023-01-03', '2023-01-04']
#
# fraud_query = "SELECT ride_id, fraud, reasons FROM dolos_postgres.frauds_meta WHERE toDate(created_date, 'Iran') BETWEEN '{start_date}' AND '{finish_date}' AND fraud = 1;"
# for date in dates:
#     with open('../fraud_' + date + '.csv', 'w') as f:
#         write = csv.writer(f)
#         print(fraud_query.format(start_date=date, finish_date=date))
#         fraud = client.execute(fraud_query.format(start_date=date, finish_date=date))
#         print("data: " + date)
#         print("number of data: " + str(len(fraud)))
#         write.writerows(fraud)

# a = client.execute("DESCRIBE TABLE dolos_postgres.frauds;")
# print(a)

# a = client.execute("DESCRIBE TABLE dwh.snapp_rides;")
# print(a)

# a = client.execute("SELECT * FROM dolos_postgres.frauds WHERE created_date='2023-01-01' LIMIT 10;")
# print(a)

# a = client.execute("DESCRIBE TABLE snapp_mysql.rides;")
# print(a)


# dates = ['2023-01-01']
#
# khatkesh_query = "SELECT * FROM snapp_raw_log.khatkesh_result_proto WHERE toDate(clickhouse_time, 'Iran') BETWEEN '{start_date}' AND '{finish_date}' AND toHour(clickhouse_time, 'Iran') BETWEEN {start_hour} AND {finish_hour};"
# for date in dates:
#     with open('xyz_' + date + '.csv', 'w') as f:
#         write = csv.writer(f)
#         for hour in range(24):
#             print(khatkesh_query.format(start_date=date, finish_date=date, start_hour=hour, finish_hour=hour))
#             khatkesh = client.execute(khatkesh_query.format(start_date=date, finish_date=date, start_hour=hour, finish_hour=hour))
#             print("number of data: " + str(len(khatkesh)))
#             write.writerows(khatkesh)

a = client.execute("SELECT is_current, id, ride_id, city_id, origin_id FROM snapp_mysql.ride_locations LIMIT 10")
print(a)


# stream_snapp_mysql.ride_locations, snapp_mysql.ride_locations