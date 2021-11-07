import time
import sqlite3
import tweepy
import nltk
import re
import config_keys
from nltk.corpus import stopwords
from sqlite3 import Error
from oauth2client.service_account import ServiceAccountCredentials

auth = tweepy.OAuthHandler(consumer_key=config_keys.consumer_key, consumer_secret=config_keys.consumer_secret)
auth.set_access_token(config_keys.access_token_1, config_keys.access_token_2)
api = tweepy.API(auth)

# WOEIDs for the following regions: worldwide, USA, Canada, UK, Australia (English-speaking countries with highest Twitch traffic)
loc_ids = (1, 23424977, 23424775, 23424975, 23424748)

sql_path = "trends.sqlite"


def create_connection(path):
    connection = None
    try:
        connection = sqlite3.connect(path)
        print("Connection to SQLite database successful!")
        return connection
    except Error as e:
        print(f"The error '{e}' occurred.")


def execute_query(connection, query, vals=None):
    cursor = connection.cursor()
    try:
        if vals:
            cursor.execute(query, vals)
        else:
            cursor.execute(query)
        connection.commit()
    except Error as e:
        print(f"The error '{e}' occurred.")


def execute_read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as e:
        print(f"The error '{e}' occurred.")


def get_top_trending():
    out = []  # list of dictionaries including keywords (no default value) and tweet volume (defaults to None)
    word_list = []  # list of keywords; used to remove duplicates
    # loops through locations in the following order: worldwide, USA, Canada, UK, Australia
    for loc in loc_ids:
        term_counter = 0
        fetched_trending = [
            {'keyword': item['name'], 'volume': item['tweet_volume'] if item['tweet_volume'] is not None else 0,
             'loc': loc} for item in api.get_place_trends(loc)[0]['trends']]
        sorted_trending = sorted(fetched_trending, key=lambda d: d['volume'], reverse=True)
        # loops through trending keywords in descending order of volume
        for f in sorted_trending:
            term = f['keyword']
            if term not in word_list and term_counter < 12 and f['loc'] == loc:
                out.append(f)
                word_list.append(term)
                term_counter += 1
    return out


# timestamp should be passed as an integer unix time
def get_associated_words(term=''):
    word_counts = {}  # will be in the format below:
    # {word (string): count (integer)}
    tweets = api.search_tweets(q=term, lang='en', result_type='popular', count=100, tweet_mode='extended')
    for tweet in tweets:
        text = tweet.full_text
        text = text.lower()
        text = re.sub('((www\.[^\s]+)|(https?://[^\s]+))', '', text)  # remove URLs
        text = re.sub('[\t\n\r\f\v]', ' ', text)  # remove non-space whitespace
        text = re.sub(r'#([^\s]+)', r'\1', text)  # remove the # in #hashtag
        text = re.sub('\\\'', '', text)  # remove apostrophes
        text = re.sub('[^a-z\s]', ' ', text)  # remove odd characters and numbers
        text = re.sub('[\s]{2,}', ' ', text)  # remove multiple spaces
        text = re.sub('^[ ]+|[ ]+$', '', text)  # remove spaces at the beginning and end of tweets
        for w in text.split():
            if w != '' and w != term and w not in stopwords.words('english'):
                if w not in word_counts.keys():
                    word_counts[w] = 1
                else:
                    word_counts[w] += 1
    return word_counts


# time is stored as Unix time
create_words_table = """
CREATE TABLE IF NOT EXISTS associated_words (
timestamp INTEGER NOT NULL,
keyword TEXT NOT NULL,
associated_word TEXT NOT NULL 
);
"""

create_words_index = "CREATE INDEX IF NOT EXISTS idx_text ON associated_words (keyword)"

connection = create_connection(sql_path)

execute_query(connection, create_words_table)
execute_query(connection, create_words_index)

# time is stored as Unix time
create_trends_table = """
CREATE TABLE IF NOT EXISTS trends (
timestamp INTEGER NOT NULL,
volume INTEGER,
keyword TEXT
);
"""

create_index = "CREATE INDEX IF NOT EXISTS idx_text ON trends (keyword)"

execute_query(connection, create_trends_table)
execute_query(connection, create_index)

while True:
    unix_time = time.time()
    trending = get_top_trending()
    for t in trending:
        insert_keywords = "INSERT INTO trends (timestamp, volume, keyword) VALUES (?, ?, ?);"
        execute_query(connection, insert_keywords, (int(unix_time), t['volume'], t['keyword']))

        insert_associated_words = "INSERT INTO associated_words (timestamp, keyword, associated_word) VALUES (?, ?, ?);"
        words = get_associated_words(t['keyword'])
        for word, count in words.items():
            if count >= 5:
                execute_query(connection, insert_associated_words, (int(unix_time), t['keyword'], word))
    print('Sleeping')
    time.sleep(60*5)  # can only request new trends once every five minutes
