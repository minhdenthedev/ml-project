from kafka import KafkaProducer
from random import randint
import json
import datetime
from time import sleep
from kafka.admin import KafkaAdminClient
import numpy as np

admin_client = KafkaAdminClient(
	bootstrap_servers = 'localhost:9092'
)

topics = ['rate-events', 'visited-events', 'search-events', 'review-events']
admin_client.delete_topics(topics=topics)

actions = ['rate', 'visited', 'search', 'review']
keywords = ['hello', 'hi', 'goodbye']

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)


while True:
    userId = randint(1, 943)
    movieId = np.clip(np.random.normal(500, 5, 1)[0], 498, 502)
    action_id = randint(0,3)
    current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    data = {
        'userId' : userId,
        'movieId' : movieId,
        'action' : actions[action_id],
        'timestamp' : current_time
    }
    if data['action'] == 'search':
    	keyword_id = int(np.clip(np.random.normal(2, 1, 1)[0], 0, 2))
    	data['keyword'] = keywords[keyword_id]
    elif data['action'] == 'rate':
    	data['rate'] = randint(1, 5)
    producer.send(data['action'] + '-events', data)
    print(data)	
