import requests
import time
from loggers.logger import app_logger

log = app_logger()

class PagingStruct(object):
    """Class for paging struct."""

    def __init__(self, 
        main_key = ['paging'],
        next_key: str = 'next',
        previous_key: str = 'previous'):
        self.main_key = main_key
        self.next_key = next_key
        self.previous_key = previous_key


class __Client(object):
    def __init__(self,
                 host,
                 retries,
                 paging=None):
        self.host = host
        self.retries = retries
        self.paging = PagingStruct() if paging is None else paging

    @property
    def resource(self):
        raise NotImplementedError

    @property
    def api_url(self):
        raise NotImplementedError

    def exec_request(self, request_type, data, sleep_time=0, has_paging=False):
        response = self.__validate_request(request_type, data, sleep_time)
        if has_paging:
            try:
                response_data = []
                while True:
                    if not response.json().get('data'):
                        log.info('No more data to fetch. Moving forward.')
                        break
                    response_data.append(response.json())
                    read_from, read_to = (data['params']['start'],data['params']['start']+data['params']['limit'])
                    log.info(f'Records from {read_from} to {read_to} ingested OK')

                    next_page = response.json()
                    for k in self.paging.main_key:
                        next_page = next_page[k]
                    if next_page:
                        next_page = next_page.get(self.paging.next_key)
                        if not next_page:
                            break
                    else:
                        break
                    # if next_page == 500:
                    #     break
                    time.sleep(10)
                    data['params']['start'] = next_page
                    response = self.__validate_request(request_type, data, sleep_time)
                return response_data
            except ValueError as e:
                raise Exception(f'Error Reading response: {e}')
        else:
            try:
                return response.json()
            except ValueError:
                return response.iter_content(chunk_size=1000000, decode_unicode=True)

    def post(self, action, body, sleep_time=0):
        raise NotImplementedError

    def get(self, action, payload=None, sleep_time=0):
        raise NotImplementedError

    def __validate_request(self, request_type, data, sleep_time):
        for i in range(self.retries):
            response = getattr(requests, request_type.lower())(**data)
            if any(response.status_code == code for code in [429, 503]):
                log.warning('Too many api requests...')
                time.sleep(sleep_time)
                continue
            elif response.status_code not in [200, 201, 202]:
                raise requests.HTTPError('Call to API failed - '
                                         'Status code: {code} - Reason: {reason} - '
                                         'Text: {text}'
                                         .format(code=response.status_code,
                                                 reason=response.reason,
                                                 text=response.text))
            else:
                return response


class BearerTokenClient(__Client):
    def __init__(self,
                 host,
                 bearer_token,
                 retries=1,
                 paging=None):
        if not bearer_token:
            raise ValueError('Authorization was not specified.')
        super(BearerTokenClient, self).__init__(host, retries, paging)
        self.bearer_token = bearer_token

    @property
    def resource(self):
        raise NotImplementedError

    @property
    def api_url(self):
        raise NotImplementedError

    def post(self, action: str, body: dict, sleep_time: int = 0,
             extra_headers: dict = {}, has_paging: bool = False):
        if action:
            url = '{url}/{action}'.format(url=self.api_url, action=action)
        else:
            url = self.api_url
        data = dict(url=url,
                    headers={"Authorization": "Bearer " + self.bearer_token},
                    json=body)
        data['headers'].update(extra_headers)
        return self.exec_request('post', data, sleep_time, has_paging)

    def get(self, action: str = None, payload: dict = None, sleep_time: int = None,
            extra_headers: dict = {}, has_paging: bool = False):
        if action:
            url = '{url}/{action}'.format(url=self.api_url, action=action)
        else:
            url = self.api_url
        data = dict(url=url,
                    headers={"Authorization": "Bearer " + self.bearer_token},
                    params=payload)
        data['headers'].update(extra_headers)
        return self.exec_request('get', data, sleep_time, has_paging)


class KeyClient(__Client):
    def __init__(self,
                 host,
                 retries=1,
                 key=None,
                 secret=None,
                 paging=None):
        super(KeyClient, self).__init__(host, retries, paging)
        self.key = key
        self.secret = secret

    @property
    def resource(self):
        raise NotImplementedError

    @property
    def api_url(self):
        raise NotImplementedError

    def post(self, action: str, body: dict, sleep_time: int = None, has_paging: bool = False):
        if action:
            url = '{url}/{action}'.format(url=self.api_url, action=action)
        else:
            url = self.api_url
        data = dict(url=url,
                    json=body)
        if self.key and self.secret:
            data['auth'] = (self.key, self.secret)
        return self.exec_request('post', data, sleep_time, has_paging)

    def get(self, action: str, payload: dict = None, sleep_time: int = None, has_paging: bool = False):
        if action:
            url = '{url}/{action}'.format(url=self.api_url, action=action)
        else:
            url = self.api_url
        data = dict(url=url,
                    params=payload)
        if self.key and self.secret:
            data['auth'] = (self.key, self.secret)
        return self.exec_request('get', data, sleep_time, has_paging)
