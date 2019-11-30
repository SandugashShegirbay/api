import time
from my_requests import SessionWithBaseUrl
import requests
import hashlib
import csv
from pathlib import Path
import logging
import pandas as pd
import numpy
import matplotlib.pyplot as plt


class Sipuni:
    p = Path("results/")
    p.mkdir(parents=True, exist_ok=True)
    __last_error = 'all right'
    logging.basicConfig(filename='sample.log', filemode='w', level=logging.INFO)
    __date_analys = ''
    __user_analys = ''
    __type_analys = ''
    __all_call_time = ''
    __schema_analys = ''
    __not_talk_time = ''
    __talk_time = ''

    def __init__(self, __user, __hash):
        self.__user, self.__hash = __user, __hash
        self.__session = SessionWithBaseUrl(url_base='https://sipuni.com/api/statistic/')

    # the method that makes the request and return response. other methods calls this    
    def __request(self, path, params=None, hash_array=None):
        hash_array.append(self.__user)
        hash_array.append(self.__hash)
        hesh = hashlib.md5(bytes('+'.join(hash_array), 'utf-8')).hexdigest()
        params['user'] = self.__user,
        params['hash'] = hesh
        resp = self.__session.request('post', path, data=params)
        return resp

    # Gets a conversation record list using csv
    def get_call_recordings_list(self, anonymous='', firstTime='', fromm='', fromNumber='', state='', to='',
                                 toAnswer='', toNumber='', tree='', typee=''):
        hash_array = [anonymous, firstTime, fromm, fromNumber, state, to, toAnswer, toNumber, tree, typee]
        params = {
            'anonymous': anonymous,
            'firstTime': firstTime,
            'from': fromm,
            'fromNumber': fromNumber,
            'state': state,
            'to': to,
            'toAnswer': toAnswer,
            'toNumber': toNumber,
            'tree': tree,
            'type': typee
        }
        call_list_request = self.__request('export', params=params, hash_array=hash_array)
        # if the status code 200 writes the result to a file and returns a response otherwise returns an empty list
        if (call_list_request.status_code == 200):
            fn = 'call_list.csv'
            filepath = self.p / fn
            with filepath.open('w+', encoding='utf-8') as f:
                f.write(call_list_request.text)
            return call_list_request.text.split('\n')
        else:
            logging.info("Export Request doesn't work")
            self.__last_error = 'Export Request error'
            return [], False

    # returns a list of staff members; checks a response status and returns either a list of employees or an empty list    
    def get_list_of_employees(self):
        list_of_employees = self.__request('operators', {}, [])
        if (list_of_employees.status_code == 200):
            fn = 'employee_list.csv'
            filepath = self.p / fn
            with filepath.open('w+', encoding='utf-8') as f:
                f.write(list_of_employees.text)
            return list_of_employees.text.split('\n')
        else:
            logging.info("Operators Request doesn't work")
            self.__last_error = 'Operators Request error'
            return [], False

    # gets the conversation recording file as a parameter takes the recording id which in turn is taken from the csv list and writes everything to the mp3 file the file name is the recording id    
    def get_call_recordings(self, idd):
        params = {'id': idd}
        hash_array = [idd]
        call_recordings = self.__request('record', params, hash_array)
        if call_recordings.status_code == 200:
            fn = '{}.mp3'.format(idd)
            filepath = self.p / fn
            with filepath.open('wb') as f:
                f.write(call_recordings.content)
                # f.close()
            return 'You can find the mp3 file in the results folder file name is', fn
        else:
            logging.info("Record Request doesn't work")
            self.__last_error = 'Record Request error'
            return [], False
    # here we analyze the data obtained earlier
    def analytics(self, data, headers):
        # first translate it to data frame
        df = pd.DataFrame(data, columns=headers)
        # get rid of unnecessary columns
        df = df.drop(['Кто ответил', 'Оценка', 'ID записи', 'Метка', 'Теги', 'Запись существует', 'Состояние перезвона',
                      'Время перезвона', 'ID заказа звонка', 'Откуда', 'Куда'], axis=1)
        # replace all data with a number
        df = df.replace('Входящий', 1)
        df = df.replace('Исходящий', 0)
        df = df.replace('Внутренний', 2)
        df = df.replace('исходящая', 0)

        df = df.replace('Отдел продаж', 1)
        df = df.replace('Отдел тех. поддержки', 2)
        df = df.replace('Распределение', 3)

        df = df.replace('', 0)
        df = df.replace('Не отвечен', 1)
        df = df.replace('Отвечен', 0)
        df['Длительность звонка'] = df['Длительность звонка'].astype(int)
        df['Длительность разговора'] = df['Длительность разговора'].astype(int)
        df['Время ответа'] = df['Время ответа'].astype(int)
        df['Новый клиент'] = df['Новый клиент'].astype(int)
        # and time for date format
        df['Время'] = pd.to_datetime(df.Время)
        df['weekday'] = df.Время.dt.weekday_name
        '''Приступая к этой задаче, 
        я подумала о том,
        на какие важные вопросы я могу ответить,
        проанализировав эти данные. Вот несколько вариантов:
        '''
        
        print('в какое время самый частый звонок?')
        print(df.Время.dt.hour.value_counts())

        print("В какой день недели больше звонков?")
        print(df.Время.dt.weekday_name.value_counts())

        print('В какой день недели наиболее неотвеченных звонков?')
        print(df.groupby(['weekday'])['Статус'].aggregate('mean'))

        print('Кто больше всех оставляет звонок без ответа,клиент и админ?(Исходящий:0,Входящий:1,Внутренний:2)')
        print(df.groupby(['type'])['Статус'].aggregate('mean'))

        print('какой тип телефонного звонка имеет больше минут разговора?')
        print(df.groupby(['type'])['Длительность разговора'].aggregate('mean'))

        print('От каких типов звонков больше новых клиентов?')
        print(df.groupby(['type'])['Новый клиент'].aggregate('mean'))

        print('В какой день недели клиенты чаще всего звонят')
        print(df.groupby(['weekday'])['type'].aggregate('mean'))

        print('По какой схеме время ответа занимает больше времени?')
        print(df.groupby(['Схема'])['Время ответа'].aggregate('mean'))

        print('График на год. В каком месяце было много звонков?')
        print(df.Время.dt.month.value_counts().sort_index().plot())

        self.__date_analys = df.Время.dt.month.value_counts()
        self.__user_analys = df['Новый клиент'].value_counts()
        self.__type_analys = df['type'].value_counts()
        self.__all_call_time = df['Длительность звонка'].describe()
        self.__schema_analys = df['Схема'].value_counts()
        self.__not_talk_time = df['Время ответа'].describe()
        self.__talk_time = df['Длительность разговора'].describe()

    def get_date_analys(self):
        return self.__date_analys

    def get_user_analys(self):
        return self.__user_analys

    def get_type_analys(self):
        return self.__type_analys

    def get_not_talk_time(self):
        return self.__not_talk_time

    def get_all_call_time(self):
        return self.__all_call_time

    def get_schema_analys(self):
        return self.__schema_analys

    def get_talk_time(self):
        return self.__talk_time

    def get_error_message(self):
        return self.__last_error
