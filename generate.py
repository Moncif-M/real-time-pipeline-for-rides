from datetime import datetime,timezone
import time
import uuid
import json 
import random
from kafka import KafkaProducer
def generate_location():
    return f'Point({random.uniform(-74.01,-73.95)} {random.uniform(40.70,40.80)})'

producer = KafkaProducer(bootstrap_servers = 'localhost:9092',
                         value_serializer=lambda v : json.dumps(v).encode('utf-8'))

while True :
    ride = {
        'ride_id' : str(uuid.uuid4()),
        'timestamp' : datetime.now(timezone.utc).isoformat(),
        'pickup_location': generate_location(),
        'dropoff_location' : generate_location(),
        'passanger count' : random.randint(1,20),
        'price': round(random.uniform(20,100)),
        'driver_id': random.randint(1,4)
    }
    producer.send('rides', ride).get(timeout=60)


