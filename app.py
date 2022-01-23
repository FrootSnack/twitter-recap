import dataclasses
import sqlite3
from datetime import datetime
from datetime import timedelta

import requests
from flask import Flask
from flask import flash
from flask import redirect
from flask import render_template
from flask import request

import config_keys

app = Flask(__name__)


def to_stream_time(stream_start, timestamp):
    return str(timedelta(seconds=int(timestamp)-int(stream_start)))


app.jinja_env.globals.update(to_stream_time=to_stream_time)

app.config["SECRET_KEY"] = config_keys.FLASK_SECRET

client_id = config_keys.TWITCH_CLIENT_ID
client_secret = config_keys.TWITCH_CLIENT_SECRET
access_token = config_keys.TWITCH_ACCESS_TOKEN


@dataclasses.dataclass
class VOD:
    id: int
    user_name: str
    title: str
    start_time: datetime
    end_time: datetime


@app.route("/")
def home():
    return render_template("home.html")


@app.route('/replay/<video_id>')
def recap_page(video_id):
    if not validate_token():
        regenerate_token()

    try:
        vod = get_vod(video_id)
    except ValueError:
        return redirect("/")

    return render_template("recap_page.html", vod=vod)


@app.route('/validate', methods=['POST'])
def validate():
    try:
        vod_id = get_vod(request.form.get("search-box", "null").split("/")[-1]).id
        return redirect(f"/replay/{vod_id}")
    except ValueError:
        flash("Error in input")
        return redirect("/")


@dataclasses.dataclass
class Trend:
    start_time: int
    end_time: int
    volume: int
    keyword: int


@app.route('/trends/', methods=['GET'])
def get_associated_trends():
    start_time = request.args["start_time"]
    end_time = request.args["end_time"]
    user_name = request.args["user_name"]

    associated_trends = []

    con = sqlite3.connect("trends.sqlite")

    cur = con.cursor()

    for row in cur.execute("""
        SELECT trends.* FROM trends WHERE (timestamp BETWEEN ? AND ?) AND trends.keyword in 
        (SELECT keyword FROM associated_words WHERE associated_word = LOWER(?) and associated_words.timestamp = trends.timestamp) 
        ORDER BY timestamp ASC;
    """, (start_time, end_time, user_name)):
        timestamp = row[0]
        volume = row[1]
        keyword = row[2]

        concat = False
        for trend in associated_trends:
            if trend.keyword == keyword and trend.end_time + 40*60 > timestamp:
                #print(f"Trend {keyword} already existed within 40 minutes, concatenating")
                trend.end_time = timestamp
                concat = True
                break
        if not concat:
            #print(f"New trend {keyword} added at timestamp {timestamp}")
            associated_trends.append(
                Trend(start_time=timestamp, end_time=timestamp, volume=volume, keyword=keyword)
            )


    cur.close()
    con.close()
    return render_template("trends_box.html", stream_start=start_time, associated_trends=associated_trends)


def validate_token():
    response = requests.get('https://id.twitch.tv/oauth2/validate', headers={"Authorization": f"Bearer {access_token}"})
    return response.status_code == 200


def get_vod(video_id):
    response = requests.get(f'https://api.twitch.tv/helix/videos?id={video_id}',
                            headers={
                                "Client-ID": client_id,
                                "Authorization": f"Bearer {access_token}"})
    if response.status_code != 200:
        raise ValueError(f"Request returned status code {response.status_code}")
    data = response.json()["KEYS"][0]

    start_time = datetime.strptime(data["created_at"], "%Y-%m-%dT%H:%M:%SZ")
    duration = timedelta(
        days=get_prec(data["duration"], "d"),
        hours=get_prec(data["duration"], "h", "d"),
        minutes=get_prec(data["duration"], "m", "h"),
        seconds=get_prec(data["duration"], "s", "m")
    )

    vod = VOD(
        id=video_id,
        title=data["title"],
        start_time=start_time,
        end_time=start_time + duration,
        user_name=data["user_name"]
    )

    return vod


def get_prec(source, stop, start=None):
    if start is None or start not in source:
        start_index = -1
    else:
        start_index = source.index(start)

    if stop not in source:
        return 0
    else:
        end_index = source.index(stop)
        return int(source[start_index + 1:end_index])


def regenerate_token():
    global access_token
    response = requests.post(f'https://id.twitch.tv/oauth2/token?client_id={client_id}&client_secret={client_secret}&grant_type=client_credentials')
    data = response.json()
    access_token = data["access_token"]
    print(f"REGENERATED ACCESS TOKEN, PLEASE CHANGE!! NEW ACCESS TOKEN: \"{access_token}\"")


if __name__ == '__main__':
    app.run()
