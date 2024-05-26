import joblib
from processor import Processor
from kafka import KafkaConsumer, KafkaProducer
from pymongo import MongoClient
from mongo_test import get_database
from sklearn.feature_extraction.text import CountVectorizer
import json
from datetime import datetime

pipeline = joblib.load('models/log_reg_pipeline.pkl')

# text processor
processor = Processor()

# kafka consumer
consumer = KafkaConsumer('review-events', group_id='processingGroup', bootstrap_servers=['localhost:9092'])

# mongoDB database
db = get_database()
collection = db['reviews']

def launch():
    # execute
    for message in consumer:
        message = message.value.decode('utf-8')
        message =json.loads(message)
        print(message)
        res = pipeline.predict([message['body']])
        if res[0] == 1:
            sentiment = 'Positive'
        else:
            sentiment = 'Negative'
        item = {
            "userId": message['userId'],
            "movieId": str(message['movieId']),
            "body" : message['body'],
            "attitude" : sentiment,
            "timestamp": datetime.now(),
        }
        print(item)
        collection.insert_many([item])

if __name__ == "__main__":
    pipeline = launch()