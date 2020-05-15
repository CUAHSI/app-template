"""
File: cuahsi_search_app.py
Description: A simple word search of the GeoDeepDive corpus for CUAHSI
             related terms. Search terms are defined in the config.yaml
             as "search_terms."

             The purpose of this application is to discover research products
             that have formally referenced CUAHSI services as well as those
             that have informally mentioned them. The goal is to better
             identify the breadth and depth of the use of CUAHSI services and
             tools throughout science.
"""

import json
import yaml
import psycopg2
from psycopg2.extensions import AsIs
from psycopg2.extras import NamedTupleCursor


class Matches:
    """
    Stores GDD match info
    """
    def __init__(self):
        self.matches = {}
        self.base_url = 'https://geodeepdive.org/api/articles?id='

    def insert(self, docid, term):

        # insert the document record
        if docid not in self.matches.keys():
            self.matches[docid] = {'url': f'{self.base_url}{docid}'}

        # insert the term match
        if term in self.matches[docid].keys():
            # increment existing match
            self.matches[docid][term] += 1
        else:
            # insert new match
            self.matches[docid][term] = 1

    def json(self):
        return json.dumps(self.matches, indent=4)


# read in credentials and configuration
with open('../credentials.yml', 'r') as credential_yaml:
    credentials = yaml.load(credential_yaml, Loader=yaml.Loader)

with open('../config.yml', 'r') as config_yaml:
    config = yaml.load(config_yaml, Loader=yaml.Loader)

# Connect to Postgres
connection = psycopg2.connect(
    dbname=credentials['postgres']['database'],
    user=credentials['postgres']['user'],
    host=credentials['postgres']['host'],
    port=credentials['postgres']['port'])

cursor = connection.cursor(cursor_factory=NamedTupleCursor)

# read all sentences from our NLP example database.
cursor.execute("SELECT * FROM %(app_name)s_sentences_nlp352;",
               {"app_name": AsIs(config["app_name"])},)

# initialize the object for storing document matches
matches = Matches()

for c in cursor:

    for idx, pos in enumerate(c.poses):
        # look for match

        if c.words[idx].lower() in config['search_terms']:
            matches.insert(c.docid, c.words[idx])

# write results to the output directory
with open("../output/matches.json", "w") as f:
    f.write(matches.json())
