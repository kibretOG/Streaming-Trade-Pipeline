import yliveticker
from datetime import datetime
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'kibret_og',
    'start_date': datetime.now()
}


def stream_to_kafka():
    from kafka import KafkaProducer
    import json
    producer = KafkaProducer(bootstrap_server, )
   
    def on_new_msg(ws, msg):
        producer.send('trade_occurance', json.dumps(msg).encode('utf-8'))

    yliveticker.YLiveTicker(
        on_ticker=on_new_msg,
        ticker_names=[
            "BTC=X", "^GSPC", "^DJI", "^IXIC", "^RUT",
            "CL=F", "GC=F", "SI=F", "EURUSD=X", "^TNX",
            "^VIX", "GBPUSD=X", "JPY=X", "BTC-USD",
            "^CMC200", "^FTSE", "^N225"
        ]
    )
    
with DAG('pull_trade_data',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_to_kafka
    )
