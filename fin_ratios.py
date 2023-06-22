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

def scrape_fin_fundamentals(stock_symbol):
    fin_fun = yf.Ticker(stock_symbol)
    
    if fin_fun.info.get('trailingPE',-1)==-1:
        fin_fun.info['trailingPE'] = 'Not Defined'
    if fin_fun.info.get('bookValue',-1)==-1:
        fin_fun.info['bookValue'] = 'Not Defined'
    if fin_fun.info.get('marketCap',-1)==-1:
        fin_fun.info['marketCap'] = 'Not Defined'
    if fin_fun.info.get('returnOnEquity',-1)==-1:
        fin_fun.info['returnOnEquity'] = 'Not Defined'
    if fin_fun.info.get('dividendRate',-1)==-1:
        fin_fun.info['dividendRate'] = 'Not Defined'
    if fin_fun.info.get('trailingEps',-1)==-1:
        fin_fun.info['trailingEps'] = 'Not Defined'
    if fin_fun.info.get('debtToEquity',-1)==-1:
        fin_fun.info['debtToEquity'] = 'Not Defined'
    print(fin_fun.info['trailingPE'])
    pg_conn = PostgresHook(postgres_conn_id = 'postgres_localhost', schema = 'airflow')
    connection = pg_conn.get_conn()
    cursor = connection.cursor()
    cursor.execute('''insert into stock_fundamentals(stock_id,PE_Ratio_TTM,Book_Value,EPS,Debt_to_Equity,Dividend_Rate,ROE,Mrkt_Cap)
                    values(%s,%s,%s,%s,%s,%s,%s,%s)''',(fin_fun.info['symbol'],fin_fun.info['trailingPE'],fin_fun.info['bookValue'],fin_fun.info['trailingEps'],fin_fun.info['debtToEquity'],fin_fun.info['dividendRate'],fin_fun.info['returnOnEquity'],fin_fun.info['marketCap']))
    connection.commit()
    cursor.close()
    connection.close()

dag = DAG(dag_id = 'stock_fundamentals_pipeline_v_1.1',default_args = default_args, schedule = '@yearly', start_date = pendulum.datetime(2023,6,21,tz = 'Asia/Calcutta'))

stocks = ['SAIL.NS','TATASTEEL.NS','JSWSTEEL.NS','NESTLEIND.NS','MARICO.NS','HINDALCO.NS','JUBLFOOD.NS','TATACONSUM.NS','VBL.NS','TCS.NS','TECHM.NS','TATAELXSI.NS','SONATSOFTW.NS','ROUTE.NS','ZYDUSLIFE.NS','GLAXO.NS','BIOCON.NS','SYNGENE.NS','CIPLA.NS','BHARTIARTL.NS','INDUSTOWER.NS','RELIANCE.NS','AXISBANK.NS','SBIN.NS']

create_task = PostgresOperator(
    task_id = 'create_table',
    postgres_conn_id = 'postgres_localhost',
    sql = """create table if not exists stock_fundamentals(
            stock_id character varying,
            PE_Ratio_TTM character varying,
            Book_Value character varying,
            EPS character varying,
            Debt_to_Equity character varying,
            Dividend_Rate character varying,
            ROE character varying,
            Mrkt_Cap character varying
    )""",
    dag = dag
)
for stock in stocks:
    scrape_task = PythonOperator(
        task_id = f'scrape_{stock}_info',
        python_callable = scrape_fin_fundamentals,
        op_kwargs = {'stock_symbol':stock},
        dag = dag
    )
    create_task>>scrape_task
