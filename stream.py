import tweepy
import json
import pymongo
from pymongo import MongoClient
import datetime
import bitly_api
import requests
import time
from celery import Celery

BROKER_URL = 'mongodb://localhost:27017/jobs'
celery = Celery('EOD_TASKS', broker=BROKER_URL)
celery.config_from_object('celeryconfig')

class MyStreamListener(tweepy.StreamListener):

    def __init__(self, bitly_connection, database, count):
        self.bitly_connection = bitly_connection
        self.db = database
        self.count = count

    def on_error(self, status):
        print(status)

    def on_data(self, data):
        decoded = json.loads(data)
        # print(decoded)
        if 'entities' in decoded and decoded['retweeted'] == False:
            urls = decoded['entities']['urls']
            for url in urls:
                shortUrl = url['expanded_url']
                try:
                    userinfo = self.bitly_connection.info(link=shortUrl)
                    clicks = self.bitly_connection.link_clicks(link=shortUrl)
                    countries = self.bitly_connection.link_countries(link=shortUrl)
                    encoders_count = self.bitly_connection.link_encoders_count(link=shortUrl)['count']
                    referring_domains = self.bitly_connection.link_referring_domains(link=shortUrl)
                except bitly_api.BitlyError as er:
                    continue
                self.db.bitly_urls.insert_one({
                    "id_str": decoded['id_str'],
                    "text": decoded['text'],
                    "created_at": decoded['created_at'],
                    "coordinates": decoded['coordinates'],
                    "hashtags": decoded['entities']['hashtags'],
                    "user_mentions": decoded['entities']['user_mentions'],
                    "user": {
                        "id_str": decoded["user"]["id_str"],
                        "screen_name": decoded["user"]["screen_name"],
                        "friends_count": decoded["user"]["friends_count"],
                        "followers_count": decoded["user"]["followers_count"],
                        "listed_count": decoded["user"]["listed_count"],
                        "statuses_count": decoded["user"]["statuses_count"],
                        "lang": decoded["user"]["lang"],
                        "location": decoded["user"]["location"],
                        "time_zone": decoded["user"]["time_zone"],
                        "utc_offset": decoded["user"]["utc_offset"]
                    },
                    "shortened_url": url["expanded_url"],
                    "userinfo": userinfo,
                    "clicks": clicks,
                    "countries": countries,
                    "encoders_count": encoders_count,
                    "referring_domains": referring_domains
                })
                print(self.count)
                self.count += 1
        return True

def getKeys():
    inputFile = open('details.txt', 'r')
    consumer_key = inputFile.readline().strip().split(" ")[2]
    consumer_secret = inputFile.readline().strip().split(" ")[2]
    access_token = inputFile.readline().strip().split(" ")[2]
    access_token_secret = inputFile.readline().strip().split(" ")[2]
    bitly_access_token = inputFile.readline().strip().split(" ")[2]
    inputFile.close()
    return (consumer_key, consumer_secret, access_token, access_token_secret, bitly_access_token)

@celery.task
def getTweets():
    consumer_key, consumer_secret, access_token, access_token_secret, bitly_access_token = getKeys()
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(auth)
    bitly_connection = bitly_api.Connection(access_token=bitly_access_token)
    
    client = MongoClient()
    db = client.tweets
    myStreamListener = MyStreamListener(bitly_connection, db, 0)
    myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)

    track = ['bit ly', 'bitly']
   
    connected = False
    while not connected:
        try:
            myStream.filter(track=track)
            connected = True
        except Exception as err:
            print("Network problem")
            time.sleep(2)
            pass

if __name__ == '__main__':
    getTweets()
