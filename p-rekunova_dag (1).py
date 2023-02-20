import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
from datetime import timedelta
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


TOP_1M_DOMAINS = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'
TOP_1M_DOMAINS_FILE = 'top-1m.csv'


def get_data():
    top_doms = requests.get(TOP_1M_DOMAINS, stream=True)
    zipfile = ZipFile(BytesIO(top_doms.content))
    top_data = zipfile.read(TOP_1M_DOMAINS_FILE).decode('utf-8')

    with open(TOP_1M_DOMAINS_FILE, 'w') as f:
        f.write(top_data)


def get_zone():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    dom_zone = []
    for i in top_data_df['domain']:
        dom_zone.append([i, i.split('.')[-1]])    
    dom_zone = pd.DataFrame(dom_zone, columns=['domain', 'zone'])
    top_data_df = top_data_df.merge(dom_zone, how='inner', on='domain')
    top_data_zone = top_data_df.groupby('zone', as_index=False) \
        .agg({'domain':'count'}) \
        .sort_values('domain', ascending=False) \
        .head(10)
    with open('top_data_zone.csv', 'w') as f:
        f.write(top_data_zone.to_csv(index=False, header=False))
        
                
def get_len():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    dom_len = []
    for i in top_data_df['domain']:
        dom_len.append([i, len(i)])
    dom_len_df = pd.DataFrame(dom_len, columns=['domain', 'dom_len'])
    top_data_df = top_data_df.merge(dom_len_df, how='inner', on='domain')
    top_data_length = top_data_df.sort_values('domain') \
        .drop_duplicates(subset=['dom_len']) \
        .sort_values('dom_len', ascending=False) \
        .head(10)
    with open('top_data_length.csv', 'w') as f:
        f.write(top_data_length.to_csv(index=False, header=False))
        
def airflow_rank():
    top_data_df = pd.read_csv(TOP_1M_DOMAINS_FILE, names=['rank', 'domain'])
    top_data_airflow = top_data_df[top_data_df['domain'].str.startswith('airflow.')]       
    with open('top_data_airflow.csv', 'w') as f:
        f.write(top_data_airflow.to_csv(index=False, header=False))    


def print_data(ds): # передаем глобальную переменную airflow
    with open('top_data_zone.csv', 'r') as f:
        all_data = f.read()
    with open('top_data_length.csv', 'r') as f:
        all_data_len = f.read()
    with open('top_data_airflow.csv', 'r') as f:
        aiflow_data = f.read()
    date = ds

    print(f'Top 10 zones for date {date}')
    print(all_data)

    print(f'Top longest domais for date {date}')
    print(all_data_len)

    print(f'Airflow rank for date {date}')
    print(aiflow_data)
    
    
default_args = {
    'owner': 'p-rekunova',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 19),
    'schedule_interval': '0 0 * * *'
}
dag = DAG('p-rekunova_lesson2', default_args=default_args)




t1 = PythonOperator(task_id='get_data',
                    python_callable=get_data,
                    dag=dag)

tzone = PythonOperator(task_id='get_zone',
                    python_callable=get_zone,
                    dag=dag)

tlen = PythonOperator(task_id='get_len',
                        python_callable=get_len,
                        dag=dag)

taf = PythonOperator(task_id='airflow_rank',
                        python_callable=airflow_rank,
                        dag=dag)

t5 = PythonOperator(task_id='print_data',
                    python_callable=print_data,
                    dag=dag)



t1 >> [tzone, tlen, taf] >> t5
