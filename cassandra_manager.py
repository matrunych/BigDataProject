import json
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider


class Cassandra:
    def __init__(self, user, password, ip_address):
        auth_provider = PlainTextAuthProvider(user, password)
        self.cluster = Cluster([ip_address], auth_provider=auth_provider)
        self.session = self.cluster.connect("all_events")

    def list_all_countries(self):
        res = self.session.execute("SELECT * FROM  all_countries;")
        rows = [r for r in res]
        return rows

    def cities_for_country(self, country):
        res = self.session.execute("SELECT group_city FROM cities_of_counries WHERE group_country = %s;", (country,))
        rows = [r for r in res]
        return rows

    def event_details_by_id(self, id):
        res = self.session.execute("SELECT event_name, event_time, group_topics, group_name, group_city,"
                                   " group_country FROM event_by_id "
                                   "WHERE event_id = %s;", (id,))

        rows = [{"event_name": r.event_name,
                 "event_time": r.event_time,
                 "topics": r.group_topics,
                 "group_name": r.group_name,
                 "group_city": r.group_city,
                 "group_country": r.group_country} for r in res]
        return rows

    def list_groups_by_city(self, city):
        res = self.session.execute("SELECT group_city, group_name, group_id FROM groups_by_city "
                                   "WHERE group_city = %s;", (city.capitalize(),))
        rows = [{"group_city": r.group_city,
                 "group_name": r.group_name,
                 "group_id": r.group_id} for r in res]
        return rows

    def event_details_by_group(self, group):
        res = self.session.execute("SELECT event_name, event_time, group_topics, group_name, group_city,"
                                   " group_country FROM event_by_group "
                                   "WHERE group_id = %s;", (group,))
        rows = [{"event_name": r.event_name,
                 "event_time": r.event_time,
                 "topics": r.group_topics,
                 "group_name": r.group_name,
                 "group_city": r.group_city,
                 "group_country": r.group_country} for r in res]

        return rows
