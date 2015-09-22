__author__ = 'Andres Ramirez'
from pymongo import MongoClient
import twitter

import sys

reload(sys)
sys.setdefaultencoding('utf8')

client = MongoClient()
client = MongoClient('localhost', 27017)

db=client.twitter
tweets=db['tweets']

CONSUMER_KEY = ''
CONSUMER_SECRET = ''
OAUTH_TOKEN = ''
OAUTH_TOKEN_SECRET = ''

auth = twitter.oauth.OAuth(OAUTH_TOKEN, OAUTH_TOKEN_SECRET,CONSUMER_KEY, CONSUMER_SECRET)

twitter_api = twitter.Twitter(auth=auth)


query="eurobasket" ####SELECCIONA AQUI TU CONSULTA
count=10 ###SELECCIONA EL NÚMERO DE TWEETS POR CONSULTA
nqueries=100 ###SELECCIONA EL NÚMERO DE QUERIES LANZADOS A LA API

search_results = twitter_api.search.tweets(q=query, count=count)
statuses=search_results['statuses']

for i in range(len(statuses)):
    tweets.insert_one(statuses[i]).inserted_id


for _ in range(nqueries):
    try: next_results = search_results['search_metadata']['next_results']
    except KeyError, e: break
    kwargs = dict([kv.split('=') for kv in next_results[1:].split("&")])
    search_results = twitter_api.search.tweets(**kwargs)
    statuses=search_results['statuses']
    for i in range(len(statuses)):
        try:
            tweets.insert_one(statuses[i]).inserted_id
        except:
            print("There is a problem with MongoDB")

