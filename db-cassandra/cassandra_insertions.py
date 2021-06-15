import cassandra
from kafka import KafkaConsumer
import json
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from datetime import datetime

consumer = KafkaConsumer('events', bootstrap_servers=["34.66.220.182:9092", "34.134.122.115:9092",
                                                      "34.122.15.255:9092"],
                         api_version=(0, 10, 2),
                         value_deserializer=lambda value: json.loads(value.decode('utf-8')),
                         auto_offset_reset='earliest',
                         consumer_timeout_ms=1000000)

auth_provider = PlainTextAuthProvider("cassandra", "password")
cluster = Cluster(['34.69.185.95'], auth_provider=auth_provider)
session = cluster.connect("all_events")

for message in consumer:
    event_id = message.value['event']['event_id']
    event_name = message.value['event']['event_name']
    event_time = datetime.utcfromtimestamp(message.value['event']['time'] / 1000).strftime('%Y-%m-%d %H:%M:%S')
    group_id = str(message.value['group']['group_id'])
    group_name = message.value['group']['group_name']
    group_country = message.value['group']['group_country']
    group_city = message.value['group']['group_city']
    group_topics = message.value['group']['group_topics']
    topics_names = []
    for topic in group_topics:
        topics_names.append(topic['topic_name'])

    session.execute(
        "INSERT INTO all_countries ( group_country ) VALUES ( %s ) IF NOT EXISTS ",
        (group_country,))

    session.execute(
        """
        INSERT INTO cities_of_counries (group_country, group_city)
        VALUES (%s, %s)
        IF NOT EXISTS
        """,
        (group_country, group_city,))

session.execute(
    """
    INSERT INTO event_by_id (event_id, event_name, event_time, group_topics,
     group_name, group_country, group_city)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    IF NOT EXISTS
    """,
    (event_id, event_name, event_time, topics_names,
     group_name, group_country, group_city,))

session.execute(
    """
    INSERT INTO groups_by_city (group_city, group_name, group_id)
    VALUES (%s, %s, %s)
    IF NOT EXISTS
    """,
    (group_city, group_name, group_id,))

session.execute(
    """
    INSERT INTO event_by_group (event_id, event_name, event_time, group_topics,
     group_name, group_country, group_city, group_id)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    IF NOT EXISTS
    """,
    (event_id, event_name, event_time, topics_names,
     group_name, group_country, group_city, group_id,))
