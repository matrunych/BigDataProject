from kafka.producer import KafkaProducer
import json
import requests

producer = KafkaProducer(bootstrap_servers=["34.66.220.182:9092", "34.134.122.115:9092", "34.122.15.255:9092"],
                         api_version=(0, 10, 2),
                         value_serializer=lambda value: json.dumps(value).encode('utf-8'))

r = requests.get('http://stream.meetup.com/2/rsvps', stream=True)
r.encoding = 'utf-8'

for i in r.iter_lines(decode_unicode=True):
    if i:
        producer.send("events", value=json.loads(i))
