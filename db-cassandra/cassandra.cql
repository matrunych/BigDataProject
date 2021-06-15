CREATE
KEYSPACE all_events WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};


CREATE TABLE all_events.all_countries
(
    group_country text,
    primary key (group_country)
)
WITH comment = 'Q1';


CREATE TABLE all_events.cities_of_counries
(
    group_country text,
    group_city text,
    primary key ((group_country), group_city)
)
WITH comment = 'Q2';


CREATE TABLE all_events.event_by_id
(
    event_id text,
    event_name text,
    event_time text,
    group_topics list<text>,
    group_name text,
    group_country text,
    group_city text,
    primary key (event_id)
)
WITH comment = 'Q3';

CREATE TABLE all_events.groups_by_city
(
    group_city text,
    group_name text,
    group_id text,
    primary key ((group_city), group_id)
)
WITH comment = 'Q4';

CREATE TABLE all_events.event_by_group
(
    event_id text,
    event_name text,
    event_time text,
    group_topics list<text>,
    group_name text,
    group_country text,
    group_city text,
    group_id text,
    primary key (group_id)
)
WITH comment = 'Q5';

