from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
import time
from datetime import datetime, timedelta
import requests
import os
import pytz
from time import sleep
import json

from scripts.weather_stream import get_next_crawling_time

model_url = 'http://157.230.44.40:5000/process_traffic_status'

CMT8_Pham_Van_Hai= "http://giaothong.hochiminhcity.gov.vn/expandcameraplayer/?camId=63ae7cfcbfd3d90017e8f422&camLocation=C%C3%A1ch%20M%E1%BA%A1ng%20Th%C3%A1ng%20T%C3%A1m%20%E2%80%93%20Ph%E1%BA%A1m%20V%C4%83n%20Hai&camMode=camera&videoUrl=https://d2zihajmogu5jn.cloudfront.net/bipbop-advanced/bipbop_16x9_variant.m3u8"
CMT8_Bac_Hai = "http://giaothong.hochiminhcity.gov.vn/expandcameraplayer/?camId=63ae798abfd3d90017e8f255&camLocation=C%C3%A1ch%20M%E1%BA%A1ng%20Th%C3%A1ng%20T%C3%A1m%20-%20B%E1%BA%AFc%20H%E1%BA%A3i&camMode=camera&videoUrl=https://d2zihajmogu5jn.cloudfront.net/bipbop-advanced/bipbop_16x9_variant.m3u8"
CMT8_Truong_Son = "http://giaothong.hochiminhcity.gov.vn/expandcameraplayer/?camId=63ae79aabfd3d90017e8f26a&camLocation=C%C3%A1ch%20M%E1%BA%A1ng%20Th%C3%A1ng%20T%C3%A1m%20-%20Tr%C6%B0%E1%BB%9Dng%20S%C6%A1n&camMode=camera&videoUrl=https://d2zihajmogu5jn.cloudfront.net/bipbop-advanced/bipbop_16x9_variant.m3u8"
CMT8_To_Hien_Thanh = "http://giaothong.hochiminhcity.gov.vn/expandcameraplayer/?camId=63ae7966bfd3d90017e8f240&camLocation=C%C3%A1ch%20M%E1%BA%A1ng%20Th%C3%A1ng%20T%C3%A1m%20-%20T%C3%B4%20Hi%E1%BA%BFn%20Th%C3%A0nh&camMode=camera&videoUrl=https://d2zihajmogu5jn.cloudfront.net/bipbop-advanced/bipbop_16x9_variant.m3u8"
CMT8_Hoa_Hung = "http://giaothong.hochiminhcity.gov.vn/expandcameraplayer/?camId=631955e7c9eae60017a1c30a&camLocation=C%C3%A1ch%20M%E1%BA%A1ng%20Th%C3%A1ng%20T%C3%A1m%20%E2%80%93%20H%C3%B2a%20H%C6%B0ng&camMode=camera&videoUrl=https://d2zihajmogu5jn.cloudfront.net/bipbop-advanced/bipbop_16x9_variant.m3u8"

cameras = [CMT8_Pham_Van_Hai, CMT8_Bac_Hai, CMT8_Truong_Son, CMT8_To_Hien_Thanh, CMT8_Hoa_Hung]

Saigon_timezone = pytz.timezone('Asia/Saigon')

def image_processing (img):
    result = requests.post(model_url, files={'image': img})
    result = result.json()
    return result


def ScreenShot_Traffic_Camera():
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    camera_data = []
    cam_locations = ['Pham Van Hai', 'Bac Hai', 'Truong Son', 'To Hien Thanh', 'Hoa Hung']

    for index, (cam, ith, cam_location) in enumerate(zip(cameras, range(1,len(cameras)+1), cam_locations)):
        if ith != 1:
            driver.execute_script(f"window.open('about:blank','{str(ith)}ndtab');")
            driver.switch_to.window(f"{str(ith)}ndtab")

        driver.get(cam)
        driver.set_window_size(1000, 800)
        driver.save_screenshot("cam" + str(ith) +".png")
        with open("cam" + str(ith) +".png", "rb") as image_file:
            image_bytes = image_file.read()
        camera =  image_processing (image_bytes)
        camera['location'] = cam_location
        camera_data.append(camera)

        os.remove("cam" + str(ith) +".png")
    
    return camera_data

def format_traffic(res, time):
    traffic_json = {}
        
    traffic_json['time'] = time
    traffic_json['person'] = res['object_counts'].get('person', 0)
    traffic_json['bicycle'] = res['object_counts'].get('bicycle', 0)
    traffic_json['motorcycle'] = res['object_counts'].get('motorcycle', 0)
    traffic_json['car'] = res['object_counts'].get('car', 0)
    traffic_json['stop_sign'] = res['object_counts'].get('stop sign', 0)
    traffic_json['traffic_light'] = res['object_counts'].get('traffic light', 0)
    traffic_json['truck'] = res['object_counts'].get('truck', 0)
    traffic_json['bus'] = res['object_counts'].get('bus', 0)
    traffic_json['potted_plant'] = res['object_counts'].get('potted plant', 0)
    traffic_json['traffic_status'] = res.get('traffic_status', 'Medium')
    traffic_json['location'] = res.get('location', 'None')
    return traffic_json
    
def live_traffic():
    from kafka import KafkaProducer
    import time
    import logging
    
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    while True:

        try:
            response_data = ScreenShot_Traffic_Camera()

            for cam in response_data:
                dt_obj = datetime.now(Saigon_timezone)
                dt = dt_obj.strftime("%Y-%m-%d %H:%M:%S.%f%z")
                formated_data = format_traffic(cam, dt)
                producer.send('traffic', json.dumps(formated_data).encode('utf-8'))

        except Exception as e:
            logging.error(f'An error occured: {e}')
            break 

        next_time = get_next_crawling_time()
        if datetime.now(Saigon_timezone).minute != next_time.minute:
            sleep((next_time + timedelta(minutes=7) - datetime.now(Saigon_timezone)).seconds)