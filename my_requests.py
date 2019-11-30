import requests
from urllib.parse import urljoin


class SessionWithBaseUrl(requests.Session):
    """
    Наследование класса requests.Session() с перегрузкой методов для добавления параметра base_url как в Guzzle.
    Параметр base_url позволяет избавиться от необходимости вводить полный адрес при запросах.
    """
    def __init__(self, url_base = None, *args, **kwargs):
        super(SessionWithBaseUrl, self).__init__(*args, **kwargs)
        self.url_base = url_base

    def request(self, method, url, **kwargs):
        modified_url = urljoin(self.url_base, url)
        return super(SessionWithBaseUrl, self).request(method, modified_url, **kwargs)
