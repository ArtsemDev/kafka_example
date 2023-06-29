from confluent_kafka import Consumer
from ujson import load, loads

with open('config.json', 'r', encoding='utf-8') as file:
    config = load(file)


consumer = Consumer(config.get('default') | config.get('consumer'))
# consumer = Consumer({**config.get('default') **config.get('consumer')})
consumer.subscribe([config.get('topic')])
while True:
    msg = consumer.poll(1.0)
    if msg is None:
        pass
    elif msg.error():
        print(msg.error())
    else:
        print(loads(msg.value().decode()))
