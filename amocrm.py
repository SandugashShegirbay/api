import logging
import time
from schema import Schema, Or
from my_request import SessionWithBaseUrl


class amoCRM:
    __last_error = 'Good job!!!'
    __is_auth = False
    logging.basicConfig(filename="sample.log", filemode='w', level=logging.INFO)

    # accepts data for authorization and performs primary authorization
    def __init__(self, __user_login, __user_hash, __subdomain):
        self.__user_login, self.__user_hash, self.__subdomain = __user_login, __user_hash, __subdomain
        self.__session = SessionWithBaseUrl(url_base='https://{}.amocrm.ru/api/v2/'.format(__subdomain))
        self.__auth()

    # authorization method
    def __auth(self):
        __data = {
            'USER_LOGIN': self.__user_login,
            'USER_HASH': self.__user_hash}
        __user_auth = self.__session.post('/private/api/auth.php', data=__data, params={'type': 'json'})
        self.status_code = __user_auth.status_code

        if __user_auth.status_code == 200:
            logging.info("Authorized")
            self.__is_auth = True

        else:
            logging.error("Not authorized")
            self.__last_error = "Not authorized"

    # returns authorization status
    def get_auth_status(self):
        return self.__is_auth

    # makes a request on the server; other methods makes a request through it
    def __request(self, method, path, params=None, headers=None):

        # Проверка статуса авторизации. Если False, повторная авторизация
        if not self.__is_auth:
            logging.info("Try authorized a second time")
            self.__auth()
        # Выполнение запроса
        resp = self.__session.request(method, path, params=params, headers=headers)
        # Если успешно, возврат объекта Response
        if resp.status_code != 401:
            logging.info("Request work")
            return resp
        # Если ошибка и у сервиса имеется зеркало
        else:
            logging.info("Try authorized across mirror")
            self.__session = SessionWithBaseUrl(url_base='https://{}.z1.amocrm.ru/api/v2'.format(self.__subdomain))
            # Авторизация на зеркале
            self.__auth()
            # Проверка авторизации. Если ошибка, возврат возврат объекта Response с кодом 401
            if not self.__is_auth:
                self.__last_error = "Not authorized"
                logging.error("This is error(I tried authorized many times, check your data)")
                return resp
            # Выполнение запроса к зеркалу
            resp = self.__session.request(method, path, params=params, headers=headers)
            return resp

    # Return errors
    def get_error_message(self):
        return self.__last_error

    # method for checking data structure
    def __schema(self, requests, par):
        schems = {
            'events': {'value_before': Or(list, dict),
                       'value_after': Or(list, dict),
                       'object': Schema({'entity': Or(int, str), 'id': Or(int, str)}, ignore_extra_keys=True),
                       },
            'leads': {'id': Or(int, str),
                      'account_id': Or(int, str),
                      'closed_at': Or(int),
                      'company': Or(dict),
                      'created_at': Or(int),
                      'created_by': Or(int),
                      'main_contact': Or(dict),
                      'name': Or(str),
                      'pipeline_id': Or(int),
                      'responsible_user_id': Or(int),
                      'sale': Or(int),
                      'status_id': Or(int),
                      'updated_at': Or(int),
                      'custom_fields': Or(dict, list),
                      'contacts': Or(dict)

                      },
            'contacts': {'id': Or(int, str),
                         'account_id': Or(int, str),
                         'company': Or(dict),
                         'created_at': Or(int),
                         'created_by': Or(int),
                         'name': Or(str),
                         'responsible_user_id': Or(int),
                         'updated_at': Or(int),
                         'custom_fields': Or(list, dict),
                         'leads': Or(dict)

                         },
            'companies': {'id': Or(int, str),
                          'account_id': Or(int, str),
                          'created_at': Or(int),
                          'created_by': Or(int),
                          'name': Or(str),
                          'responsible_user_id': Or(int),
                          'updated_at': Or(int),
                          'custom_fields': Or(list, dict),
                          'leads': Or(dict),
                          'contacts': Or(dict)

                          },
            'tasks': {'id': Or(int, str),
                      'account_id': Or(int, str),
                      'created_at': Or(int),
                      'created_by': Or(int),
                      'responsible_user_id': Or(int),
                      'updated_at': Or(int),
                      'task_type': Or(int),
                      'element_id': Or(int),
                      'element_type': Or(int),
                      'result': Or(dict),
                      'text': Or(str),
                      'is_completed': Or(bool),
                      'duration': Or(int),
                      'complete_till_at': Or(int)
                      },
            'notes': {'id': Or(int, str),
                      'account_id': Or(int, str),
                      'created_at': Or(int),
                      'created_by': Or(int),
                      'responsible_user_id': Or(int),
                      'updated_at': Or(int),
                      'note_type': Or(int),
                      'element_id': Or(int),
                      'element_type': Or(int),
                      'params': Or(dict),
                      'text': Or(str)}
        }
        # receives a list and checks each dict for integrity
        for request in requests:
            schema = Schema(schems[par], ignore_extra_keys=True)
            # if in the list at least one damaged element the method returns false
            if not schema.is_valid(request):
                logging.error('integrity check failed')
                __last_error = 'Integrity_check_error'
                return False
            else:
                logging.info('integrity check was successful')
                return True

    # this method returns all answers, the method calls itself until all answers are returned since only 500 elements will be returned in one request
    def __for_recurcions(self, url, limit_offset, params={}, items=[]):
        params = {'limit_rows': 500, 'limit_offset': limit_offset}
        is_schema = False
        request = self.__request('get', url, params=params)
        if request.status_code == 200:
            # checks the status of the code to find out if the answer is empty .if not empty calls the method recursively, until the answer is an empty list
            if request.status_code != 204:
                results = request.json().get('_embedded', {}).get('items', False)

                if not results:
                    __last_error = url + ' Response or items are not'
                    logging.error(url + ' Response or items are not')
                    results = []

                items.append(results)
                # calls the method recursively to get all the data
                items += self.__for_recurcions(url=url, limit_offset=limit_offset+500, items=items)
        # if the request was not successful, it assigns the last error to the variable and an empty list is returned because the conditions were not met and etems was initially empty        
        else:
            logging.error("request error")
            self.__last_error = 'Request error'
        return items

    # helper method for getting the number of pages of an event
    def __get_events_total_pages(self, headers, params=None):
        # make a request to get the number of pages of the event
        count_request = self.__request('get', '/ajax/events/count/', params=params, headers=headers)
        if count_request.status_code != 200:
            logging.error("Total Pages Request error.Try again")
            __last_error = 'Total Pages Request error'
            count_request = {}
        return count_request.json().get('pagination', {}).get('total', False)

    # method for getting events, accepts all possible parameters
    def get_events(self, event_type=None, useFilter=None, filter_date_from=None, filter_date_to=None):
        page_result = {}
        params = {}
        events = []
        headers = {'X-Requested-With': 'XMLHttpRequest'}
        if useFilter is not None:
            params['useFilter'] = useFilter

        if filter_date_from is not None:
            params['filter_date_from'] = filter_date_from

        if filter_date_to is not None:
            params['filter_date_to'] = filter_date_to

        if event_type is not None:
            params['event_type'] = event_type

        total_pages = self.__get_events_total_pages(headers, params=params)

        if not total_pages:
            __last_error = "Pagination or total are not"
            logging.error("Pagination or total are not")
            total_pages = 0

        for i in range(1, total_pages + 1):
            params['PAGEN_1'] = i
            event_request = self.__request('get', '/ajax/events/list', headers=headers, params=params)

            # after every 4th request,  take a break for 1 second so that the server does not crash
            if i % 4 == 0:
                time.sleep(1)

            # check a status
            if event_request.status_code == 200:
                page_result = event_request.json().get('response', {}).get('items', False)

                if not page_result:
                    __last_error = "Events response are not"
                    logging.error("Events response are not")
                    page_result = []

            events += page_result

        # calls a method for checking integrity
        is_schema = self.__schema(events, 'events')
        if is_schema:
            return events, is_schema
        else:
            for i in range(1, total_pages + 1):
                params['PAGEN_1'] = i
                event_request = self.__request('get', '/ajax/events/list', headers=headers, params=params)

                # after every 4th request,  take a break for 1 second so that the server does not crash
                if i % 4 == 0:
                    time.sleep(1)

                if event_request.status_code == 200:
                    page_result = event_request.json().get('response', {}).get('items', False)

                if not page_result:
                    __last_error = 'Events Response or items are not'
                    logging.error('Events Response or items are not')
                    page_result = []

                events += page_result
                is_schema = self.__schema(events, 'events')
                if not is_schema:
                    logging.info('integrity check failed so returns an empty list')
                    return [], is_schema
                return events, is_schema

    # method for getting leads, accepts all possible parameters
    def get_leads(self, id=None, query=None, responsible_user_id=None, _with=None,
                  status=None, create_from=None, create_to=None, modify_from=None, modify_to=None, tasks=None, active=None):
        params = {}
        if id is not None:
            params['id[]'] = id

        if query is not None:
            params['query'] = query

        if responsible_user_id is not None:
            params['responsible_user_id[]'] = responsible_user_id

        if _with is not None:
            params['with'] = _with

        if status is not None:
            params['status[]'] = status

        if create_from is not None:
            params['filter[date_create][from]'] = create_from

        if create_to is not None:
            params['filter[date_create][to]'] = create_to

        if modify_from is not None:
            params['filter[date_modify][from]'] = modify_from

        if create_to is not None:
            params['filter[date_modify][to]'] = modify_to

        if tasks is not None:
            params['filter[tasks]'] = tasks

        if active is not None:
            params['filter[active]'] = active
        # calls the method recursively to get all the data
        results = sum(self.__for_recurcions('leads', 0, params), [])
        # checks the integrity of the data for this calls the method __schema
        is_schema = self.__schema(results, 'leads')
        # if the test fails for the first time, it will try to request a new data
        if not is_schema:
            results = self.__for_recurcions('leads', 0, params)
            # if after the second attempt the check fails then returns an empty list
            if not is_schema:
                return [], is_schema
            return results, is_schema
        return results, is_schema

    # method for getting contacts, accepts all possible parameters
    def get_contacts(self, id=None, responsible_user_id=None, query=None):
        params = {}
        if id is not None:
            params['id[]'] = id

        if responsible_user_id is not None:
            params['responsible_user_id[]'] = responsible_user_id

        if query is not None:
            params['query'] = query

        results = sum(self.__for_recurcions('contacts', 0, params), [])
        is_schema = self.__schema(results, 'contacts')

        if not is_schema:
            results = self.__for_recurcions('contacts', 0, params)

            if not is_schema:
                return [], is_schema
            return results, is_schema
        return results, is_schema

    # method for getting companies, accepts all possible parameters
    def get_companies(self, id=None, query=None, responsible_user_id=None):
        params = {}
        if id is not None:
            params['id[]'] = id

        if query is not None:
            params['query'] = query

        if responsible_user_id is not None:
            params['responsible_user_id[]'] = responsible_user_id

        results = sum(self.__for_recurcions('companies', 0, params), [])
        is_schema = self.__schema(results, 'companies')

        if not is_schema:
            results = self.__for_recurcions('companies', 0, params)

            if not is_schema:
                return [], is_schema
            return results, is_schema
        return results, is_schema

    # method for getting tasks, accepts all possible parameters
    def get_tasks(self, id=None, element_id=None, responsible_user_id=None, type=None,
                  create_from=None, create_to=None, modify_from=None, modify_to=None, pipe=None, status=None,
                  created_by=None, task_type=None, tasks=[], one_time=0):
        params = {}
        if id is not None:
            params['id'] = id

        if element_id is not None:
            params['element_d'] = element_id

        if responsible_user_id is not None:
            params['responsible_user_id'] = responsible_user_id

        if type is not None:
            params['type'] = type

        if create_from is not None:
            params['filter[date_create][from]'] = create_from

        if create_to is not None:
            params['filter[date_create][to]'] = create_to

        if modify_from is not None:
            params['filter[date_modify][from]'] = modify_from

        if modify_to is not None:
            params['filter[date_modify][to]'] = modify_to

        if pipe is not None:
            params['filter[pipe][]'] = pipe

        if status is not None:
            params['filter[status][]'] = status

        if created_by is not None:
            params['filter[created_by][]'] = created_by

        if task_type is not None:
            params['filter[task_type][]'] = task_type

        results = sum(self.__for_recurcions('tasks', 0, params), [])
        is_schema = self.__schema(results, 'tasks')

        if not is_schema:
            results = self.__for_recurcions('tasks', 0, params)

            if not is_schema:
                return [], is_schema
            return results, is_schema
        return results, is_schema

    # method for getting notes, accepts all possible parameters
    def get_notes(self, type=None, require=None, id=None, element_id=None,
                  note_type=None, if_modified_sinse=None):
        params = {}
        if type is not None:
            params['type[]'] = type

        if require is not None:
            params['require'] = require

        if id is not None:
            params['id[]'] = id

        if element_id is not None:
            params['element_id'] = element_id

        if note_type is not None:
            params['note_type'] = note_type

        if if_modified_sinse is not None:
            params['if-modified-sinse'] = if_modified_sinse

        results = sum(self.__for_recurcions('notes', 0, params), [])

        is_schema = self.__schema(results, 'notes')

        if not is_schema:
            results = self.__for_recurcions('notes', 0, params)

            if not is_schema:
                return [], is_schema
            return results, is_schema
        return results, is_schema
