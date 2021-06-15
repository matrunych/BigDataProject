import json
import threading

from write_file import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_list, struct
import time
import functools
from collections import Counter

one_hour = 60 * 60
six_hours = one_hour * 7
three_hours = one_hour * 4
spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


def batch():
    tm = time.time()
    events_at_each_US_state(tm)
    newly_created_events(tm)
    events_at_each_US_state(tm)


def get_time(tm, diff):
    return time.strftime('%H:%M', time.localtime(tm - diff)), time.strftime('%H:%M', time.localtime(tm))


# spark is an existing SparkSession
def get_df(tm, hours):
    to_read, to_read1 = get_time(tm, 0)
    to_read = int(to_read.split(':')[0])
    lst1 = []
    for i in range(hours, 0, -1):
        lst1.append(str(to_read - i) + '.json')
    file = open('temp.json', 'w')
    for i in lst1:
        txt = upload_from_bucket('temporary_data/' + i)
        file.write(txt)
    file.close()
    return spark.read.json('temp.json')

# Displays the content of the DataFrame to stdout
def newly_created_events(tm):
    df = get_df(tm, 6)
    df.groupby("")
    df = df.filter((tm - six_hours < df["mtime"]) & (df["mtime"] < tm - one_hour)).select("country").groupby(
        "country").count()
    lst = df.collect()
    start, end = get_time(tm, three_hours)
    for i in range(len(lst)):
        lst[i] = {lst[i][0]: lst[i][1]}
    rs = {'start': start, 'end': end, 'statistics': lst}
    upload_to_bucket('events_per_country/res.json', json.dumps(rs))
    return json.dumps(rs)

def events_at_each_US_state(tm):
    df = get_df(tm, 3)

    df.groupby("")
    df = df.filter((tm - three_hours < df["mtime"]) & (df["mtime"] < tm - one_hour))
    df = df.filter(df['country'] == 'us')
    df = df.select(
        "group_city", 'group_name').groupby("group_city").agg(collect_list(struct('group_name')))
    start, end = get_time(tm, three_hours)
    rs = {'start': start, 'end': end, 'statistics': df.collect()}
    for i in range(len(rs['statistics'])):
        for k in range(len(rs['statistics'][i])):
            for el in range(len(rs['statistics'][i][k])):
                try:
                    rs['statistics'][i][k][el] = list(rs['statistics'][i][k][el])[0]
                except TypeError:
                    pass
        rs['statistics'][i] = {rs['statistics'][i][0]: rs['statistics'][i][1]}
    upload_to_bucket('groups_in_city/res.json', json.dumps(rs))
    return json.dumps(rs)

def most_popular_topic_for_each_country(tm):

    df = get_df(tm, 6)
    df.groupby("")

    df = df.filter((tm - three_hours < df["mtime"]) & (df["mtime"] < tm - one_hour)).select('group_topics', 'country') \
        .groupby('country').agg(collect_list('group_topics').alias('topics'))
    rs = [row.asDict() for row in df.collect()]
    for row in rs:
        cur = []
        for topics in row['topics']:
            cur += topics
        el = Counter(cur).most_common(1)[0]
        row['topics'] = {el[0]: el[1]}
    start, end = get_time(tm, six_hours)
    rs = {'start': start, 'end': end, 'statistics': [{x['country']: x['topics']} for x in rs]}
    upload_to_bucket('topic_by_country/res.json', json.dumps(rs))
    return json.dumps(rs)


if __name__ == '__main__':
    while (True):
        batch()
        time.sleep(60*60*6)
