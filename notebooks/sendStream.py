"""Generates a stream to Kafka from a time series csv file.
"""

import argparse
import csv
import json
import sys
import time
from dateutil.parser import parse
from confluent_kafka import Producer
import socket
from datetime import date, timedelta
import pandas as pd
import warnings

warnings.filterwarnings("ignore")

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg.value()), str(err)))
    else:
        print("Message produced: %s" % (str(msg.value())))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('filename', type=str,
                        help='Time series csv file.')
    parser.add_argument('topic', type=str,
                        help='Name of the Kafka topic to stream.')
    parser.add_argument('--speed', type=float, default=1, required=False,
                        help='Speed up time series by a given multiplicative factor.')
    args = parser.parse_args()

    topic = args.topic
    p_key = args.filename

    conf = {'bootstrap.servers': "broker:9092",
            'client.id': socket.gethostname()}
    producer = Producer(conf)

    df = pd.read_csv(args.filename)
    df['date'] = pd.to_datetime(df['date'])
    columns = list(df.columns)
    columns.remove('date')
    
    start_date = df['date'].min()
    event_date = start_date
    i = 30

    while True:

        try:
            print(event_date)
            event_data = df.loc[df['date'] == event_date] 


            event_data.drop(columns='date', inplace=True)
            event_data_json = event_data.to_json(orient='records')
            jresult = f'{{"event_date": "{str(event_date.date())}", "data": {event_data_json} }}'
            producer.produce(topic, key=p_key, value=jresult, callback=acked)
            producer.flush()
            event_date = event_date +timedelta(days=1)
            time.sleep(30)

        except Exception as e:
            print(e.message)
            sys.exit()


if __name__ == "__main__":
    main()
