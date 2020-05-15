"""
File: example_app.py
Description: Example app utilizing the GeoDeepDive infrastructure and products.
    This will look at the produced NLP table and print a list of proper nouns which
    are modified by adjectives, along with the sentence id in which they occur.
Assumes: make setup-local has been run (so that the example database is populated)
"""

import json
import yaml
import psycopg2
from psycopg2.extensions import AsIs
from psycopg2.extras import NamedTupleCursor


class Matches:
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

# key: proper_noun, value: (adjective, sentence_id)
proper_nouns_with_adj = {}

# read all sentences from our NLP example database.
cursor.execute("SELECT * FROM %(app_name)s_sentences_nlp352;",
               {"app_name": AsIs(config["app_name"])},)


# list of matches
matches = Matches()

for c in cursor:

    for idx, pos in enumerate(c.poses):
        # look for match

        if c.words[idx].lower() in config['terms']:
            matches.insert(c.docid, c.words[idx])

#        if pos == "NNP" or pos == "NNPS":
#            if c.words[idx] in config['terms']:
#                matches.insert(c.docid, c.words[idx])


# write results to the output directory
with open("../output/matches.json", "w") as f:
    f.write(matches.json())
