from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta

import uuid

import requests
import json
import xmltodict

import warnings
warnings.simplefilter("ignore")

import pandas as pd
import numpy as np 

import datetime
from dateutil.relativedelta import relativedelta

import pendulum

date_now = datetime.datetime.now().date()

from functools import lru_cache

from sqlalchemy import create_engine 

# from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.generic_transfer import GenericTransfer
from airflow.providers.postgres.operators.postgres import PostgresOperator

import importlib

import modules.api_info
importlib.reload(modules.api_info)

# ________________________________________

from modules.api_info import var_encrypt_var_app_client_id
from modules.api_info import var_encrypt_var_app_secret
from modules.api_info import var_encrypt_var_secret_key

from modules.api_info import var_encrypt_url_sbis
from modules.api_info import var_encrypt_url_sbis_unloading

from modules.api_info import var_encrypt_var_db_user_name
from modules.api_info import var_encrypt_var_db_user_pass

from modules.api_info import var_encrypt_var_db_host
from modules.api_info import var_encrypt_var_db_port

from modules.api_info import var_encrypt_var_db_name
from modules.api_info import var_encrypt_var_db_name_for_upl

from modules.api_info import var_encrypt_var_db_for_upl_schema
from modules.api_info import var_encrypt_var_db_for_upl_schema_inter
from modules.api_info import var_encrypt_var_db_for_upl_schema_service_toolkit
from modules.api_info import var_encrypt_var_db_schema
from modules.api_info import var_encrypt_var_to_docs_upl
from modules.api_info import var_encrypt_var_db_for_upl_schema_to
from modules.api_info import var_encryptvar_API_sbis
from modules.api_info import var_encrypt_API_sbis_pass

from modules.api_info import f_decrypt, load_key_external


var_app_client_id = f_decrypt(var_encrypt_var_app_client_id, load_key_external()).decode("utf-8")
var_app_secret = f_decrypt(var_encrypt_var_app_secret, load_key_external()).decode("utf-8")
var_secret_key = f_decrypt(var_encrypt_var_secret_key, load_key_external()).decode("utf-8")

url_sbis = f_decrypt(var_encrypt_url_sbis, load_key_external()).decode("utf-8")
url_sbis_unloading = f_decrypt(var_encrypt_url_sbis_unloading, load_key_external()).decode("utf-8")

var_db_user_name = f_decrypt(var_encrypt_var_db_user_name, load_key_external()).decode("utf-8")
var_db_user_pass = f_decrypt(var_encrypt_var_db_user_pass, load_key_external()).decode("utf-8")

var_db_host = f_decrypt(var_encrypt_var_db_host, load_key_external()).decode("utf-8")
var_db_port = f_decrypt(var_encrypt_var_db_port, load_key_external()).decode("utf-8")

var_db_name = f_decrypt(var_encrypt_var_db_name, load_key_external()).decode("utf-8")

var_db_name_for_upl = f_decrypt(var_encrypt_var_db_name_for_upl, load_key_external()).decode("utf-8")
var_db_for_upl_schema = f_decrypt(var_encrypt_var_db_for_upl_schema, load_key_external()).decode("utf-8")
var_db_for_upl_schema_inter = f_decrypt(var_encrypt_var_db_for_upl_schema_inter, load_key_external()).decode("utf-8")
var_db_for_upl_schema_service_toolkit = f_decrypt(var_encrypt_var_db_for_upl_schema_service_toolkit, load_key_external()).decode("utf-8")
var_db_for_upl_schema_to = f_decrypt(var_encrypt_var_db_for_upl_schema_to, load_key_external()).decode("utf-8")


var_db_schema = f_decrypt(var_encrypt_var_db_schema, load_key_external()).decode("utf-8")
var_to_docs_upl = f_decrypt(var_encrypt_var_to_docs_upl, load_key_external()).decode("utf-8")


API_sbis = f_decrypt(var_encryptvar_API_sbis, load_key_external()).decode("utf-8")
API_sbis_pass = f_decrypt(var_encrypt_API_sbis_pass, load_key_external()).decode("utf-8")


local_tz = pendulum.timezone("Europe/Moscow")


default_arguments = {
    'owner': 'evgenijgrinev',
}


with DAG(
    'upl_excel_daily',
    schedule_interval='0 0 * * *',
    # schedule_interval='@once',
    catchup=False,
    default_args=default_arguments,
    start_date=pendulum.datetime(2024,7,1, tz=local_tz),
) as dag:


    def def_sbis_employees():
        
        sbis_empl = pd.read_excel('Z:/sbis_employees/Сотрудники.xlsx')
        
        @lru_cache
        def regex_filter(val):
            if val:
                mo = re.search("схема", val.lower())
                if mo:
                    return True
                else:
                    return False
            else:
                return False
        
        sbis_empl_fil = sbis_empl[sbis_empl["Полный путь до подразделения"].apply(regex_filter)]
        print('len(agents_by_schema):', len(sbis_empl_fil["Полный путь до подразделения"].unique()))
        
        sbis_empl_fil["schema"] = sbis_empl_fil["Полный путь до подразделения"].apply(lambda x: re.findall(r'схема\s+\d+/.\d+|схема\s+\d+', x.lower())[0])
        
        table_name = 'sbis_employees_excel'
        my_conn = create_engine(f"postgresql+psycopg2://{var_db_user_name}:{var_db_user_pass}@{var_db_host}:{var_db_port}/softum")
        try: 
            my_conn.connect()
            # print('connect')
            my_conn = my_conn.connect()
            sbis_empl_fil.to_sql(name=f'{table_name}', con=my_conn, index = False, schema=var_db_schema, if_exists="replace")
            print(f'{table_name} success!')
            my_conn.close()
        except:
            print(f'{table_name} failed')
        print('________________________')
    
    def def_agent_award_schema():
    
        df_scheme_common = pd.read_excel('Z:/agent_award/agent_award_schema/Схемы мотивации для Агентов.xlsx', header=1)
        df_scheme_common["Наименование"] = df_scheme_common["Наименование"].apply(lambda x: x.lower())
        df_scheme_common = df_scheme_common.rename(columns={
            "Наименование": "schema",
            'Вознаграждение за новых клиентов СБИС': 'sbis_new',
            'Вознаграждение за продления/расширения клиентов СБИС': 'sbis_ext',
            'Вознаграждение ККТ': 'kkt',
            'Вознаграждение доп оборудование Оборудование': 'dop',
            'Вознаграждение ТО': 'to',
            'Вознаграждение Гарант': 'garant',
            'Вознаграждение 1С': 'one_c',
            'Вознаграждение ОФД': 'ofd',
        })
        
        table_name = 'agent_scheme_common_motivation_excel'
        my_conn = create_engine(f"postgresql+psycopg2://{var_db_user_name}:{var_db_user_pass}@{var_db_host}:{var_db_port}/softum")
        try: 
            my_conn.connect()
            # print('connect')
            my_conn = my_conn.connect()
            df_scheme_common.to_sql(name=f'{table_name}', con=my_conn, index = False, schema=var_db_schema, if_exists="replace")
            print(f'{table_name} success!')
            my_conn.close()
        except:
            print(f'{table_name} failed')
        print('________________________')


    # __________________________________________________________________________________
    sbis_employees = PythonOperator(
        task_id='sbis_employees',
        python_callable=def_sbis_employees
    )
    
    agent_award_schema = PythonOperator(
        task_id='agent_award_schema',
        python_callable=def_agent_award_schema
    )
    # __________________________________________________________________________________


sbis_employees >> agent_award_schema




