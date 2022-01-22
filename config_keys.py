import json

FILE = open('config.json')
KEYS = json.load(FILE)

# back-end
CONSUMER_KEY = KEYS['CONSUMER_KEY']
CONSUMER_SECRET = KEYS['CONSUMER_SECRET']
ACCESS_TOKEN_1 = KEYS['ACCESS_TOKEN_1']
ACCESS_TOKEN_2 = KEYS['ACCESS_TOKEN_2']

# front-end
flask_secret = KEYS['flask_secret']
twitch_client_id = KEYS['twitch_client_id']
twitch_client_secret = KEYS['twitch_client_secret']
twitch_access_token = KEYS['twitch_access_token']

FILE.close()
