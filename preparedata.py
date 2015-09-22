# -*- coding: utf-8 -*-
__author__ = 'Andres Ramirez'

from alchemyapi import AlchemyAPI
from pymongo import MongoClient
import arff
import json
import sys

reload(sys)
sys.setdefaultencoding('utf8')

client = MongoClient()
client = MongoClient('localhost', 27017)
alchemyapi = AlchemyAPI()

db=client.twitter
tweets=db.tweets
filtros={'localizacion_usuario':'user.location',
         'geolocalizado':'user.geo_enabled',
         'idioma_usuario':'user.lang',
         'usuario_verificado':'user.verified',
         'retweets':'retweet_count',
         'favorito':'favorited',
         'fecha':'created_at'}

campos_usuario={'idioma':'user.lang',
                'localizacion':'user.location',
                'followers':'user.followers_count'}

campos_tweet={'texto':'text',
              'retweets':'retweet_count',
              'favorito':'favorited',
              'fecha':'created_at'}

filter={filtros['idioma_usuario']:'es', filtros['usuario_verificado']:True}

projection={'_id': False,
            campos_usuario['idioma']:True,
            campos_usuario['followers']:True,
            campos_tweet['texto']:True,
            campos_tweet['retweets']:True,
            campos_tweet['favorito']:True}


data=tweets.find(filter,projection=projection)

dataset=[]
attribute_types=[]
for tweet in data:
        tweet=json.dumps(tweet,indent=1)
        tweet=json.loads(tweet)

        attributes=[]
        data=[]
        try:
            data.append(tweet['user']['lang'])
            atribute=('user_language',['ar','ca','de','el','en','en-gb','es','fi','fr','he','id','it','ja','ko','nl','no','pl','pt','ru','sv','tr','zh-cn'])
            attributes.append(atribute)
        except:
            pass

        try:
            data.append(tweet['user']['location'])
            atribute=('user_location','STRING')
            attributes.append(atribute)
        except:
            pass

        try:
            data.append(tweet['user']['followers_count'])
            atribute=('user_followers','NUMERIC')
            attributes.append(atribute)
        except:
            pass

        try:
            data.append(tweet['retweet_count'])
            atribute=('retweeted','NUMERIC')
            attributes.append(atribute)
        except:
            pass

        try:
            data.append(tweet['favorited'])
            atribute=('favorited',['True','False'])
            attributes.append(atribute)
        except:
            pass

        try:
            data.append(tweet['created_at'])
            atribute=('creation_date','STRING')
            attributes.append(atribute)
        except:
            pass


        #for item in variables:
            #f.write('%s\t' % item)

        try:
            sentiment=alchemyapi.sentiment('html', tweet['text'])
            try:
                if sentiment['docSentiment']['type']=='neutral':
                    #f.write('%s\n' % 0)
                    data.append(0)
                    atribute=('tweet_sentiment','REAL')
                    attributes.append(atribute)
                else:
                    #f.write('%s\n' % sentiment['docSentiment']['score'])
                    data.append(sentiment['docSentiment']['score'])
                    atribute=('tweet_sentiment','REAL')
                    attributes.append(atribute)
            except:
                data.append(0)
                atribute=('tweet_sentiment','REAL')
                attributes.append(atribute)

        except:
            pass

        attribute_types.append(attributes)
        dataset.append(data)

tweets_dataset={'description':'Tweets','relation':'Tweet fields','attributes':attribute_types[0],'data':dataset}

with open('tweets.arff','w') as f:
    f.write(arff.dumps(tweets_dataset))
    f.close()