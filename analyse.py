from pymongo import MongoClient
import traceback
import operator

mongoclient = MongoClient('127.0.0.1', 27017)
db = mongoclient.tweets
cursor = db.bitly_urls.find()

def main():
    find_time(isSpam=True)


def find_encoders_count(isSpam=False, topTen=False):
    encoders = dict()
    for record in cursor:
        if isSpam == True:
            if 'google_safe_browsing' not in record or record['google_safe_browsing'] == False:
                continue
        shortened_url = record["shortened_url"]
        encoders_count = record["encoders_count"]
        encoders[shortened_url] = encoders_count
    
    write_to_file('URLsByEncoders', encoders, isSpam, topTen)

def find_total_domains(isSpam = False, topTen=False):
    total_domains = dict()
    for record in cursor:
        if isSpam == True:
            if 'google_safe_browsing' not in record or record['google_safe_browsing'] == False:
                continue
              
        domains = record["referring_domains"]
        for domain in domains:
            try:
                domain_name = domain["domain"]
                domain_clicks = domain["clicks"]
                total_domains[domain_name] = total_domains.get(domain_name,0) \
                        + domain_clicks
            except KeyError as kerr:
                domain_name = domain["referrer_app"]
                domain_clicks = domain["clicks"]
                total_domains[domain_name] = total_domains.get(domain_name,0) \
                        + domain_clicks

    write_to_file('ClicksByDomains', total_domains, isSpam, topTen)


def find_total_countries(isSpam = False):
    total_clicks = dict()
    for record in cursor:
        if isSpam == True:
            if 'google_safe_browsing' not in record or record['google_safe_browsing'] == False:
                continue
        countries = record["countries"]
        for country in countries:
            country_name = country["country"]
            country_clicks = country["clicks"]
            total_clicks[country_name] = total_clicks.get(country_name, 0) \
                    + country_clicks

    write_to_file('ClicksByCountries', total_clicks, isSpam)

def find_clicks(isSpam = False, topTen=False):
    clicks = dict()
    for record in cursor:
        if isSpam == True:
            if 'google_safe_browsing' not in record or record['google_safe_browsing'] == False:
                continue
        shortened_url = record["shortened_url"]
        click_count = record["clicks"]
        clicks[shortened_url] = click_count
    
    write_to_file('TweetsWithClicks', clicks, isSpam, topTen)

def find_userinfo():
    user = dict()
    for record in cursor:
        friends_count = record["user"]["friends_count"]
        statuses_count = record["user"]["statuses_count"]
        followers_count = record["user"]["followers_count"]
        screen_name = record["user"]["screen_name"]
        user[screen_name] = (friends_count, statuses_count, followers_count)
    write_to_file('userInfo', user)


def find_languages(isSpam=False):
    count_lang = dict()
    for record in cursor:
        if isSpam == True:
            if 'google_safe_browsing' not in record or record['google_safe_browsing'] == False:
                continue
        lang = record["user"]["lang"]
        count_lang[lang] = count_lang.get(lang, 0) + 1
    write_to_file('TweetsLanguages', count_lang, isSpam)

def find_time(isSpam=False):
    count_time = dict()
    for record in cursor:
        if isSpam == True:
            if 'google_safe_browsing' not in record or record['google_safe_browsing'] == False:
                continue
        created_at = record["created_at"].split(" ")
        time = created_at[3].split(":")[0]

        count_time[time] = count_time.get(time, 0) + 1
    write_to_file('TweetsByTime', count_time, isSpam)

    
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

def write_to_file(filename, dictionary, isSpam=False, topTen=False):
    if isSpam:
        filename = 'Spam' + filename
    if topTen:
        filename = 'Top' + filename
        dictionary = sorted(dictionary.items(), key=operator.itemgetter(1))[-1:-11:-1]

    fp = open('Result/' + filename + '.txt', 'w')

    for record in dictionary:
        if topTen:
            print(record[0] + " " + str(record[1]), file=fp)
        else:
            print(record + " " + str(dictionary[record]), file=fp)
    fp.close()

if __name__ == '__main__':
    main()
    cursor.close()

