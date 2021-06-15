from flask import Flask
from write_file import upload_from_bucket
from main import *
app = Flask(__name__)


@app.route('/')
def hello_world():
    return ''


@app.route('/events_per_country')
def events_per_country():

    return newly_created_events(time.time())


@app.route('/groups_in_city')
def groups_in_city():

    return events_at_each_US_state(time.time())


@app.route('/topic_by_country')
def topic_by_country():

    return most_popular_topic_for_each_country(time.time())

if __name__ == '__main__':
    app.run()