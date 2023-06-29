from confluent_kafka import Producer
from ujson import load, dumps


with open('config.json', 'r', encoding='utf-8') as file:
    config = load(file)


producer = Producer(config.get('default'))


def message(err, msg):
    if err is not None:
        print(err)
    else:
        print(f'{msg.topic()} {msg.key()} {msg.value()}')


from time import sleep


for i in range(10):
    sleep(1)
    data = {'user_id': i}
    producer.produce(config.get('topic'), dumps(data), callback=message)
    producer.poll(1)
    producer.flush()
