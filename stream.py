import tweepy
import json
import pymongo
from pymongo import MongoClient
import datetime
import bitly_api

fp = open('output2.txt', 'w', encoding='utf8')
client = MongoClient()
db = client.tweets

track = ['bit ly', 'bitly']
count = 0

class MyStreamListener(tweepy.StreamListener):

    def on_error(self, status):
        print(status)

    def on_data(self, data):
        global count
        decoded = json.loads(data)
        # print(decoded)
        if 'entities' in decoded and decoded['retweeted'] == False:
            urls = decoded['entities']['urls']
            for url in urls:
                shortUrl = url['expanded_url']
                try:
                    userinfo = bitly_connection.info(link=shortUrl)
                    clicks = bitly_connection.link_clicks(link=shortUrl)
                    countries = bitly_connection.link_countries(link=shortUrl)
                    encoders_count = bitly_connection.link_encoders_count(link=shortUrl)['count']
                    referring_domains = bitly_connection.link_referring_domains(link=shortUrl)
                except bitly_api.BitlyError as er:
                    continue
                fp.write(url['expanded_url'] + '\n')
                db.bitly_urls.insert_one({
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
                print(count)
                count += 1
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

def getTweets():
    consumer_key, consumer_secret, access_token, access_token_secret, bitly_access_token = getKeys()
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(auth)
    global bitly_connection
    bitly_connection = bitly_api.Connection(access_token=bitly_access_token)
    
    myStreamListener = MyStreamListener()
    myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)
    
    myStream.filter(track=track)

if __name__ == '__main__':
    getTweets()
