from pymongo import MongoClient
import numpy

mongoclient = MongoClient('127.0.0.1', 27017)
db = mongoclient.tweets
cursor = db.bitly_urls.find()

dataset = dict()

def main():
    find_clicks()
    find_encoders_count()
    find_countries_clicks()
    find_domain_clicks()

    #dataset['http://bit.ly/2bjLLU3'] = [False, 31, 1, 10.333333333333334, 12.498888839501783, 31.0, 0.0]


    fp = open('features.arff', 'w')
    for key in dataset:
        try:
            for i in range(6, 2, -1):
                print("{0:.4}".format(dataset[key][i]), file=fp, end=",")
            for i in range(2, 0, -1):
                print(dataset[key][i], end=",", file=fp)
            print(dataset[key][0], file=fp)
        except IndexError as ierr:
            continue
    fp.close()


def find_domain_clicks():
    cursor.rewind()
    for record in cursor:
        if record["shortened_url"] not in dataset:
            continue
        short_url = record["shortened_url"]
        domains = record["referring_domains"]
        domain_clicks = []
        for domain in domains:
            domain_clicks.append(domain["clicks"])
        if len(dataset[short_url]) == 5:
            if len(domain_clicks) == 0:
                dataset[short_url].append([0, 0])
            else:
                dataset[short_url].append(numpy.mean(domain_clicks))
                dataset[short_url].append(numpy.sqrt(numpy.var(domain_clicks)))


def find_countries_clicks():
    cursor.rewind()
    for record in cursor:
        if record["shortened_url"] not in dataset:
            continue
        short_url = record["shortened_url"]
        countries = record["countries"]
        country_clicks = []
        for country in countries:
            country_clicks.append(country["clicks"])
        if len(dataset[short_url]) == 3:
            if len(country_clicks) == 0:
                dataset[short_url].append([0, 0])
            else:
                dataset[short_url].append(numpy.mean(country_clicks))
                dataset[short_url].append(numpy.sqrt(numpy.var(country_clicks)))

def find_encoders_count():
    cursor.rewind()
    for record in cursor:
        if record["shortened_url"] not in dataset:
            continue
        short_url = record["shortened_url"]
        encoders_count = record["encoders_count"]
        if len(dataset[short_url]) == 2:
            dataset[short_url].append(encoders_count)
        else:
            dataset[short_url][2] = encoders_count

def find_clicks():
    for record in cursor:
        if 'google_safe_browsing' not in record:
            continue
        short_url = record["shortened_url"]
        clicks = record["clicks"]
        safe_browsing = record["google_safe_browsing"]
        if clicks >= 2:
            dataset[short_url] = [safe_browsing, clicks]

if __name__ == '__main__':
    main()
    cursor.close()


