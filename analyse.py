from pymongo import MongoClient

mongoclient = MongoClient('127.0.0.1', 27017)
db = mongoclient.tweets
cursor = db.bitly_urls.find()

def main():
    find_clicks()

def find_clicks():
    clicks = dict()
    for record in cursor:
        shortened_url = record["shortened_url"]
        click_count = record["clicks"]
        clicks[shortened_url] = click_count
    write_to_file('TweetsWithClicks', clicks)

def find_userinfo():
    user = dict()
    for record in cursor:
        friends_count = record["user"]["friends_count"]
        statuses_count = record["user"]["statuses_count"]
        followers_count = record["user"]["followers_count"]
        screen_name = record["user"]["screen_name"]
        user[screen_name] = (friends_count, statuses_count, followers_count)
    write_to_file('userInfo', user)


def find_languages():
    count_lang = dict()
    for record in cursor:
        lang = record["user"]["lang"]
        count_lang[lang] = count_lang.get(lang, 0) + 1
    write_to_file('languages', count_lang)

def find_time():
    count_time = dict()
    for record in cursor:
        created_at = record["created_at"].split(" ")
        time = created_at[3].split(":")[0]

        count_time[time] = count_time.get(time, 0) + 1

    
def find_dates():
    count_dates = dict()
    for record in cursor:
        created_at = record["created_at"].split(" ")
        date = created_at[2]
        if date in count_dates:
            count_dates[date] += 1
        else:
            count_dates[date] = 1
    print(count_dates)

def write_to_file(filename, dictionary):
    fp = open('Result/' + filename + '.txt', 'w')

    for record in dictionary:
        print(record + " " + str(dictionary[record]), file=fp)
    fp.close()

if __name__ == '__main__':
    main()
    cursor.close()

