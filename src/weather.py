import requests
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import os
import sys
import time 

# Define the URL
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

dataframes = []
def call_api_and_save(realtime_url, dataframes):
    try:
        headers = {"accept": "application/json"}
        response = requests.get(realtime_url, headers=headers)
        response_data = response.json()
        print(response_data)
        # Assuming the response contains a 'data' field with relevant information
        if 'data' in response_data:
            # Convert response data to dataframe
            new_data = pd.DataFrame(response_data)
            
            # Append dataframe to the list
            dataframes.append(new_data)
            
            # Concatenate all dataframes in the list
            combined_dataframe = pd.concat(dataframes, ignore_index=True)
            
            # Save combined dataframe to CSV
            combined_dataframe.to_csv('weather_data_1-3.csv', index=False)
            
            print("Data saved successfully.")
        else:
            print("No data found in response.")
    except Exception as e:
        print("Error occurred:", e)

def call_api_every_x_seconds(x, realtime_url, dataframes):
    while True:
        call_api_and_save(realtime_url, dataframes)
        time.sleep(x)

def call_api_everday(daytime_url):
    response = requests.post(daytime_url, json=payload, headers=headers)
    response_data = response.json()
    new_data = pd.DataFrame(response_data)
    #separate each start time to a row
    new_data = new_data.explode('data')
    #separate each data to a column
    new_data = pd.concat([new_data.drop(['data'], axis=1), new_data['data'].apply(pd.Series)], axis=1)

    new_data.to_csv('../data/weather data/weather_data_2-3.csv', index=False)

def main():
    #call_api_every_x_seconds(60, realtime_url, dataframes)
    call_api_everday(daytime_url)
