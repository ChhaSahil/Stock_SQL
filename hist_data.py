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

def scrape_data(stock_symbol):
    stock_data = yf.Ticker(stock_symbol)
    stock_hist = stock_data.history()
    pg_conn = PostgresHook(postgres_conn_id = 'postgres_localhost', schema = 'airflow')
    connection = pg_conn.get_conn()
    cursor = connection.cursor()
    for index, row in stock_hist.iterrows():
        cursor.execute('''insert into stock_price_data2(stock_id,datetime,stock_name,open_price,close_price,high_price,low_price,volume)
                    values(%s,%s,%s,%s,%s,%s,%s,%s)''',(stock_data.info['symbol'],str(index.date()),stock_data.info['longName'],row['Open'],row['Close'],row['High'],row['Low'],row['Volume']))
        connection.commit()
    cursor.close()
    connection.close()

dag = DAG(dag_id='stock_data_pipeline_v_1.2',default_args=default_args,schedule = '@yearly',start_date=pendulum.datetime(2023,6,19,tz = 'Asia/Calcutta'))

stocks = ['SAIL.NS','TATASTEEL.NS','JSWSTEEL.NS','NESTLEIND.NS','MARICO.NS','HINDALCO.NS','JUBLFOOD.NS','TATACONSUM.NS','VBL.NS','TCS.NS','TECHM.NS','TATAELXSI.NS','SONATSOFTW.NS','ROUTE.NS','ZYDUSLIFE.NS','GLAXO.NS','BIOCON.NS','SYNGENE.NS','CIPLA.NS','BHARTIARTL.NS','INDUSTOWER.NS','RELIANCE.NS','AXISBANK.NS','SBIN.NS']

create_task = PostgresOperator(
        task_id = 'create_table',
        postgres_conn_id = 'postgres_localhost',
        sql = """create table if not exists stock_price_data2(
            stock_id character varying,
            datetime character varying,
            stock_name character varying,
            open_price character varying,
            close_price character varying,
            high_price character varying,
            low_price character varying,
            volume character varying
            )"""
    )

for stock in stocks:
    scrape_task = PythonOperator(
        task_id = f'scrape_{stock}_data',
        python_callable = scrape_data,
        op_kwargs={'stock_symbol':stock},
        dag = dag
    )
    create_task>>scrape_task