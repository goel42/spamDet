import tweepy
import json

fp = open('output0.txt', 'w', encoding='utf8')
track = ['bit ly', 'bitly']
count = 0
i = 0

class MyStreamListener(tweepy.StreamListener):

    def on_error(self, status):
        print(status)

    def on_data(self, data):
        global count
        decoded = json.loads(data)
        if 'entities' in decoded and decoded['retweeted'] == False:
            urls = decoded['entities']['urls']
            for url in urls:
                fp.write(url['expanded_url'] + '\n')
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
