from flask import Flask, jsonify
from cassandra_manager import Cassandra

app = Flask(__name__)


@app.route('/list_all_countries', methods=["GET"])
def list_all_countries():
    res = jsonify(db.list_all_countries())
    res.status_code = 200
    return res


@app.route('/list_all_cities_by_country/<string:country>', methods=["GET"])
def cities_for_country(country):
    res = jsonify(db.cities_for_country(country))
    res.status_code = 200
    return res


@app.route('/event_details_by_id/<string:event_id>', methods=["GET"])
def event_details_by_id(event_id):
    res = jsonify(db.event_details_by_id(event_id))
    res.status_code = 200
    return res


@app.route('/list_groups_by_city/<string:city>', methods=["GET"])
def list_groups_by_city(city):
    res = jsonify(db.list_groups_by_city(city))
    res.status_code = 200
    return res


@app.route('/event_details_by_group/<string:group>', methods=["GET"])
def event_details_by_group(group):
    res = jsonify(db.event_details_by_group(group))
    res.status_code = 200
    return res



if __name__ == '__main__':
    db = Cassandra(user='cassandra', password='password', ip_address='34.69.185.95')
    app.run(debug=False)
