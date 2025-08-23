import uuid
import json
import time
import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from kafka import KafkaProducer
import requests

default_args = {
    'owner': 'sumit mahajan',
    'start_date': datetime(2025, 8, 5, 10, 0)
}

# ✅ Function to get user data from API
def get_data():
    try:
        res = requests.get("https://randomuser.me/api/")
        logging.info(f"Raw API response: {res.text}")  # 👈 DEBUG: Log raw API data

        res_json = res.json()
        results = res_json.get('results', [])

        if not results:
            raise ValueError("No results found in API response")

        return results[0]
    except Exception as e:
        logging.error(f"[get_data] Error fetching from API: {e}")
        raise  # Let Airflow know task failed

# ✅ Function to format the API data
def format_data(res):
    data = {}
    location = res['location']
    data['id'] = str(uuid.uuid4())  # UUID needs to be a string
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{location['street']['number']} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data

# ✅ Kafka streaming logic
def stream_data():
    producer = None
    try:
        producer = KafkaProducer(
            bootstrap_servers=['broker:29092'],
            max_block_ms=5000
        )
        logging.info("[stream_data] Kafka producer connected")

        curr_time = time.time()

        while True:
            if time.time() > curr_time + 60:  # Stop after 60 sec
                break

            try:
                raw_data = get_data()
                formatted = format_data(raw_data)

                logging.info(f"[stream_data] Sending user: {formatted['username']}")

                producer.send('users_created', json.dumps(formatted).encode('utf-8'))

                time.sleep(3)  # Optional: Slow down requests
            except Exception as e:
                logging.error(f"[stream_data] Error while processing: {e}")
                continue

    finally:
        if producer:
            logging.info("[stream_data] Flushing & closing Kafka producer")
            producer.flush()
            producer.close()

# ✅ Define DAG
with DAG(
    'user_automation',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )
