from clickhouse_driver import Client
import csv

client = Client(host='172.21.16.1',
                user='map_user',
                password='6zeQUqCj4y7puMxbBCeN')

# a = client.execute('DESCRIBE TABLE snapp_raw_log.khatkesh_result_proto')
# print(a)
#
# b = client.execute('SELECT * FROM snapp_raw_log.khatkesh_result_proto LIMIT 1')
# print(b)

c = client.execute("SELECT * FROM snapp_raw_log.khatkesh_result_proto WHERE toDate(clickhouse_time, 'Iran') BETWEEN '2022-09-14' AND '2022-09-14' AND toHour(clickhouse_time, 'Iran') BETWEEN 17 AND 19")

with open('../khatkesh.csv', 'w') as f:
    # using csv.writer method from CSV package
    write = csv.writer(f)

    # write.writerow(fields)
    write.writerows(c)
