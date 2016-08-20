import tweepy
import json
import pymongo
from pymongo import MongoClient
import datetime


fp = open('output2.txt', 'w', encoding='utf8')
client = MongoClient()
db = client.tweets

track = ['bit ly', 'bitly']
count = 0
i = 0

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
                fp.write(url['expanded_url'] + '\n')
                db.bitly_urls.insert_one({"text": decoded['text'], "url": url['expanded_url'], "source" : decoded['source'], "place":decoded['place'], "retweet_count": decoded['retweet_count'], "created_at":decoded['created_at'],"downloaded_at": datetime.datetime.utcnow(), "user": {"name":decoded['user']['name'],"screen_name":decoded['user']['screen_name'], "description": decoded['user']['description'],"statuses_count": decoded['user']['statuses_count'], "followers_count": decoded['user']['followers_count'],"verified":decoded['user']['verified'],"time_zone":decoded['user']['time_zone'],"location":decoded['user']['location'],"language":decoded['user']['lang'],"friends_count":decoded['user']['friends_count'],"favourites_count":decoded['user']['favourites_count'],"created_at":decoded['user']['created_at']}})
                print(count)
                count += 1
        return True

def getKeys():
    inputFile = open('details.txt', 'r')
    consumer_key = inputFile.readline().strip().split(" ")[2]
    consumer_secret = inputFile.readline().strip().split(" ")[2]
    access_token = inputFile.readline().strip().split(" ")[2]
    access_token_secret = inputFile.readline().strip().split(" ")[2]
    inputFile.close()
    return (consumer_key, consumer_secret, access_token, access_token_secret)

def getTweets():

    consumer_key, consumer_secret, access_token, access_token_secret = getKeys()
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(auth)
    
    myStreamListener = MyStreamListener()
    myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)
    
    myStream.filter(track=track)

if __name__ == '__main__':
    getTweets()
