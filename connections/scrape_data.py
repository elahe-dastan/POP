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

dates = ['2022-08-02', '2022-08-03', '2022-08-04', '2022-08-05', '2022-08-06', '2022-08-07', '2022-08-08',
               '2022-08-09', '2022-08-10', '2022-08-11', '2022-08-12', '2022-08-13', '2022-08-14', '2022-08-15',
               '2022-08-16', '2022-08-17', '2022-08-18', '2022-08-19', '2022-08-20', '2022-08-21', '2022-08-22',
               '2022-08-23', '2022-08-24', '2022-08-25', '2022-08-26', '2022-08-27', '2022-08-28', '2022-08-29',
               '2022-08-30', '2022-08-31', '2022-09-01', '2022-09-02', '2022-09-03', '2022-09-04', '2022-09-05',
               '2022-09-06', '2022-09-07', '2022-09-08', '2022-09-09', '2022-09-10', '2022-09-11', '2022-09-12',
               '2022-09-13', '2022-09-14', '2022-09-15', '2022-09-16', '2022-09-17', '2022-09-18', '2022-09-19',
               '2022-09-20', '2022-09-21', '2022-09-22', '2022-09-23', '2022-09-24', '2022-09-25', '2022-09-26',
               '2022-09-27', '2022-09-28', '2022-09-29', '2022-09-30', '2022-10-01', '2022-10-02', '2022-10-03',
               '2022-10-04', '2022-10-05', '2022-10-08', '2022-10-09', '2023-01-01',
               '2023-01-02', '2023-01-03', '2023-01-04']

eta_query = "SELECT * FROM snapp_mysql.rides WHERE toDate(created_at, 'Iran') BETWEEN '{start_date}' AND '{finish_date}' AND toHour(created_at, 'Iran') BETWEEN {start_hour} AND {finish_hour};"
for date in dates:
    with open('eta_' + date + '.csv', 'w') as f:
        write = csv.writer(f)
        for hour in range(24):
            print(eta_query.format(start_date=date, finish_date=date, start_hour=hour, finish_hour=hour))
            eta = client.execute(eta_query.format(start_date=date, finish_date=date, start_hour=hour, finish_hour=hour))
            print("data: " + date)
            print("number of data: " + str(len(eta)))
            write.writerows(eta)