from datetime import datetime, timedelta
import pendulum
import yfinance as yf

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

default_args = {
    'owner' : 'SQL',
    'retries' : 5,
    'retry_delay' : timedelta(minutes = 2)
}

def scrape_info(stock_symbol):
    stock_info = yf.Ticker(stock_symbol)
    if stock_info.info.get('industry',-1)==-1:
        stock_info.info['industry'] = 'Not Defined'
    if stock_info.info.get('sector',-1)==-1:
        stock_info.info['sector'] = 'Not Defined'
    pg_hook = PostgresHook(postgres_conn_id = 'postgres_localhost',schema = 'airflow')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute('''insert into stock_info_data(stock_id, industry, sector) values(%s,%s,%s)''',(stock_info.info['symbol'],stock_info.info['industry'],stock_info.info['sector']))
    connection.commit()
    cursor.close()
    connection.close()

dag = DAG(dag_id = 'stock_info_pipeline_v_1.1',default_args = default_args, schedule = '@yearly', start_date = pendulum.datetime(2023,6,21,tz = 'Asia/Calcutta'))

stocks = ['SAIL.NS','TATASTEEL.NS','JSWSTEEL.NS','NESTLEIND.NS','MARICO.NS','HINDALCO.NS','JUBLFOOD.NS','TATACONSUM.NS','VBL.NS','TCS.NS','TECHM.NS','TATAELXSI.NS','SONATSOFTW.NS','ROUTE.NS','ZYDUSLIFE.NS','GLAXO.NS','BIOCON.NS','SYNGENE.NS','CIPLA.NS','BHARTIARTL.NS','INDUSTOWER.NS','RELIANCE.NS','AXISBANK.NS','SBIN.NS']

create_task = PostgresOperator(
    task_id = 'create_table',
    postgres_conn_id = 'postgres_localhost',
    sql = """create table if not exists stock_info_data(
            stock_id character varying,
            industry character varying,
            sector character varying
    )""",
    dag = dag
)
for stock in stocks:
    scrape_task = PythonOperator(
        task_id = f'scrape_{stock}_info',
        python_callable = scrape_info,
        op_kwargs = {'stock_symbol':stock},
        dag = dag
    )
    create_task>>scrape_task
