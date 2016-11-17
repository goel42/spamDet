from pymongo import MongoClient
import traceback

mongoclient = MongoClient('127.0.0.1', 27017)
db = mongoclient.tweets
cursor = db.bitly_urls.find()

def main():
    find_encoders_count()


def find_encoders_count():
    encoders = dict()
    for record in cursor:
        shortened_url = record["shortened_url"]
        encoders_count = record["encoders_count"]
        encoders[shortened_url] = encoders_count
    write_to_file('TweetsWithEncoders', encoders)

def find_total_domains():
    total_domains = dict()
    for record in cursor:
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
                

    write_to_file('ClicksByDomains', total_domains)


def find_total_clicks():
    total_clicks = dict()
    for record in cursor:
        countries = record["countries"]
        for country in countries:
            country_name = country["country"]
            country_clicks = country["clicks"]
            total_clicks[country_name] = total_clicks.get(country_name, 0) \
                    + country_clicks
    write_to_file('ClicksByCountries', total_clicks)

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

