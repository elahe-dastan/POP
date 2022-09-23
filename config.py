import os

# ************* Clickhouse configs *************
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST") or '172.21.16.1'
CLICKHOUSE_PORT = os.getenv("CLICKHOUSE_PORT") or '9000'
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER") or 'map_user'
CLICKHOUSE_PASS = os.getenv("CLICKHOUSE_PASS") or '6zeQUqCj4y7puMxbBCeN'
