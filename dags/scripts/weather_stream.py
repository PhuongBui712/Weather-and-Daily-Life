import requests
import uuid
import json
from datetime import datetime, timedelta
from time import sleep
import time 
import pytz

class UUIDEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, uuid.UUID):
            # if the obj is uuid, we simply return the value of uuid
            return obj.hex
        return json.JSONEncoder.default(self, obj)
    
Saigon_timezone = pytz.timezone('Asia/Saigon')

realtime_url = "https://api.tomorrow.io/v4/weather/realtime?location=10.7829647,106.670745&apikey=Ez2hSkCFqMrvsXGs56HWnk7eWcKGwGP8"
daytime_url = "https://api.tomorrow.io/v4/timelines?apikey=Ez2hSkCFqMrvsXGs56HWnk7eWcKGwGP8"

payload = {
    "location": "10.7829647,106.670745",
    "fields": ["temperature", "cloudBase", "cloudCeiling", "cloudCover", "dewPoint", "freezingRainIntensity", "humidity", "precipitationProbability", "pressureSurfaceLevel", "rainIntensity", "sleetIntensity", "snowIntensity", "temperatureApparent", "uvHealthConcern", "uvIndex", "visibility", "weatherCode", "windDirection", "windGust", "windSpeed"],
    "units": "metric",
    "timesteps": ["15m"],
    "startTime": "nowMinus24h",
    "endTime": "now",
    "timezone": "auto"
}
headers = {
    "accept": "application/json",
    "Accept-Encoding": "gzip",
    "content-type": "application/json"
}

def get_weather():
    headers = {"accept": "application/json"}
    response = requests.get(realtime_url, headers=headers)
    response_data = response.json()

    return response_data

def format_weather(res):
    location ={}
    location['latitude'] = res['location']['lat']
    location['longitude'] = res['location']['lon']

    dt_obj = datetime.now(Saigon_timezone)
    dt = dt_obj.strftime("%Y-%m-%d %H:%M:%S.%f%z")

    weather = {}
    weather['time'] = dt
    weather['cloud_base'] = res['data']['values']['cloudBase']
    weather['cloud_ceiling'] = res['data']['values']['cloudCeiling']
    weather['cloud_cover'] = res['data']['values']['cloudCover']
    weather['dew_point'] = res['data']['values']['dewPoint']
    weather['freezing_rain_intensity'] = res['data']['values']['freezingRainIntensity']
    weather['humidity'] = res['data']['values']['humidity']
    weather['precipitation_probability'] = res['data']['values']['precipitationProbability']
    weather['pressure_surface_level'] = res['data']['values']['pressureSurfaceLevel']
    weather['rain_intensity'] = res['data']['values']['rainIntensity']
    weather['sleet_intensity'] = res['data']['values']['sleetIntensity']
    weather['snow_intensity'] = res['data']['values']['snowIntensity']
    weather['temperature'] = res['data']['values']['temperature']
    weather['temperature_apparent'] = res['data']['values']['temperatureApparent']
    weather['uv_health_concern'] = res['data']['values']['uvHealthConcern']
    weather['uv_index'] = res['data']['values']['uvIndex']
    weather['visibility'] = res['data']['values']['visibility']
    weather['weather_code'] = res['data']['values']['weatherCode']
    weather['wind_direction'] = res['data']['values']['windDirection']
    weather['wind_gust'] = res['data']['values']['windGust']
    weather['wind_speed'] = res['data']['values']['windSpeed']

    weather.update(location)

    return weather

def get_next_crawling_time():
    now = datetime.now(Saigon_timezone)
    next_time = now + timedelta(minutes=15)
    next_time = datetime(next_time.year, next_time.month, next_time.day,
                         next_time.hour, (next_time.minute // 15) * 15, 1,
                         tzinfo=Saigon_timezone)

    return next_time

def live_weather():
    from kafka import KafkaProducer
    import time
    import logging
    
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    while True:
        try:
            response_data = get_weather()
            response_weather = format_weather(response_data)
            producer.send('weather', json.dumps(response_weather).encode('utf-8'))

        except Exception as e:
            logging.error(f'An error occured: {e}')
            break

        next_time = get_next_crawling_time()
        if datetime.now(Saigon_timezone).minute != next_time.minute:
            sleep((next_time + timedelta(minutes=7) - datetime.now(Saigon_timezone)).seconds)

