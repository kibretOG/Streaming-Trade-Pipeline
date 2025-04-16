from airflow import DAG
import yliveticker
from datetime import datetime
from airflow.operators.python import PythonOperator
from confluent_kafka import Producer

default_args = {
    'owner': 'kibret_og',
    'start_date': datetime.now()
}


def stream_to_kafka():
    import json
    conf = {
        'bootstrap.servers' : 'broker:29092'
    }
    producer = Producer(conf)
    
    def on_new_msg(ws, msg):
        try:
            producer.produce('trade_occurance_raw', value=json.dumps(msg).encode('utf-8'))
            producer.flush()
            print('successful ingestion')
        except Exception as e:
            print(f'An error occured: {e}')
        # ws.close()
    yliveticker.YLiveTicker(
        on_ticker=on_new_msg,
        ticker_names=[
            "JPY=X"
        ]
    )
    # producer.flush()

    
with DAG('pull_trade_data',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_to_kafka
    )
