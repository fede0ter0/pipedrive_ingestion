from apis.api import KeyClient, PagingStruct
from loggers.logger import app_logger

log = app_logger()

class PipeDrive(KeyClient):

    def __init__(self,paging=None):
        host = ''
        super(PipeDrive, self).__init__(host=host, key=None, secret=None,paging=paging)

    @property
    def resource(self):
        raise NotImplementedError

    @property
    def api_version(self):
        return '1'

    @property
    def api_url(self):
        return '{host}/v{version}/{resource}' \
            .format(host=self.host, version=self.api_version, resource=self.resource)

    @property
    def api_token(self):
        return 'api_token'

    def get_data(self, start):
        params = dict(
            api_token=self.api_token,
            start=start
        )

        resp_json = self.get(action=None, payload=params, sleep_time=None)

        return resp_json

class Deals(PipeDrive):

    def __init__(self):
        super().__init__(paging=PagingStruct(['additional_data','pagination'],'next_start'))

    @property
    def resource(self):
        return 'deals'

    def get_data(self, **kwargs):
        params = dict(
            api_token=self.api_token,
            start=0,
            limit=100
        )

        resp_json = self.get(action=None, payload=params, has_paging=True)
        return resp_json

class Persons(PipeDrive):

    def __init__(self):
        super().__init__(paging=PagingStruct(['additional_data','pagination'],'next_start'))

    @property
    def resource(self):
        return 'persons'

    def get_data(self,**kwargs):
        params=dict(
            api_token=self.api_token,
            filter_id=729,
            start=0,
            limit=100
        )

        resp_json = self.get(action=None, payload=params, has_paging=True)
        return resp_json
        
class Organizations(PipeDrive):

    @property
    def resource(self):
        return 'organizations'


class Pipelines(PipeDrive):

    @property
    def resource(self):
        return 'pipelines'


class Stages(PipeDrive):

    @property
    def resource(self):
        return 'stages'


class DealsTimeLine(PipeDrive):

    @property
    def resource(self):
        return 'deals/timeline'

    def get_data(self, **kwargs):
        params = dict(
            api_token=self.api_token,
            start_date=kwargs['start_date'],
            interval=kwargs['interval'],
            amount=kwargs['amount'],
            field_key=kwargs['field_key']
        )

        resp_json = self.get(action=None, payload=params, sleep_time=None)

        return resp_json