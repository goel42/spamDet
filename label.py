import requests
import json
import pymongo
import bitly_api
import time
import traceback

count = 0

def setup():
    bitly_access_token, google_access_token = getKeys()
    bitly_connection = bitly_api.Connection(access_token = bitly_access_token)
    
    mongoclient = pymongo.MongoClient('127.0.0.1', 27017)
    db = mongoclient.tweets
    findLong(bitly_connection, db, google_access_token)

def findLong(bitly_connection, db, google_access_token):
    cursor = db.bitly_urls.find(no_cursor_timeout = True, modifiers={"$snapshot": True})
    record_count = 0
    for record in cursor:
        if "google_safe_browsing" in record:
            continue
        exception = True
        while exception:
            try:
                long_url = bitly_connection.expand(shortUrl=record["shortened_url"])[0]['long_url']
                print(long_url)
                result = label(google_access_token, long_url)
                record_count += 1
                print(record_count)
                record["google_safe_browsing"] = result
                db.bitly_urls.save(record)
                exception = False
            except Exception as err:
                print(traceback.format_exc())
                time.sleep(2)
                pass
    cursor.close()

def label(google_access_token, long_url):
    URL = "https://safebrowsing.googleapis.com/v4/threatMatches:find?key={key}"
    URL = URL.format(key=google_access_token)

    request_body = {
        "client": {
            "clientId": "iiita",
            "clientVersion": "1.0.0"
        },
        "threatInfo": {
            "threatTypes": ["MALWARE", "SOCIAL_ENGINEERING", "UNWANTED_SOFTWARE", "POTENTIALLY_HARMFUL_APPLICATION", "THREAT_TYPE_UNSPECIFIED"],
            "platformTypes": ["ANY_PLATFORM"],
            "threatEntryTypes": ["URL"],
            "threatEntries": [
                {"url": long_url}
            ]
        }
    }
    request_body = json.dumps(request_body)
    response = requests.post(URL, data=request_body)
    print(response.status_code)
    print(response.text, end="")
    is_spam = False
    if len(response.text) > 4:
        global count
        count += 1
        is_spam = True
    print(count)
    return is_spam

def getKeys():
    inputFile = open('details.txt', 'r')
    for i in range(4):
        inputFile.readline()
    bitly_access_token = inputFile.readline().strip().split(" ")[2]
    google_access_token = inputFile.readline().strip().split(" ")[2]
    inputFile.close()
    return (bitly_access_token, google_access_token)

if __name__ == '__main__':
    setup()

