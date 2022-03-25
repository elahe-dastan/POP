from elasticsearch7 import Elasticsearch


class Elastic:
    """ElasticSearch connection"""

    _host = None
    _port = None
    _user = None
    _password = None
    _client = None

    def __init__(self, host: str, port: int = 443, user: str = "", password: str = ""):
        self._host = host
        self._port = port
        self._user = user
        self._password = password

    def connect(self) -> Elasticsearch:
        self._client = Elasticsearch(hosts=[self._host + ":" + str(self._port)],
                                     http_auth=(self._user, self._password),
                                     timeout=300)
        return self._client

    def search(self, index: str, body: dict):
        client = self.connect()
        response = client.search(
            index=index,
            body=body
        )

        return response

    def close(self) -> None:
        self._client.transport.close()
