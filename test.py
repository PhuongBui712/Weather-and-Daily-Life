from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
import time
from datetime import datetime
import requests
import os
#import schedule
import pytz
import json

n = 1 
def image_processing (img):
    url = 'http://157.230.44.40:5000/process_traffic_status'
    result = requests.post(url, files={'image': img})
    result = result.json()
    return result


def ScreenShot_Traffic_Camera(cam1, cam2, cam3, cam4, cam5):
   
    
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)


    
    driver.get(cam1)
    driver.set_window_size(1000, 800)
    driver.save_screenshot("pvh.png")
    with open("pvh.png", "rb") as image_file:
        image_bytes = image_file.read()
    camera1 =  image_processing (image_bytes)
    
    driver.execute_script("window.open('about:blank','2ndtab');")
    driver.switch_to.window("2ndtab")
    driver.get(cam2)
    driver.set_window_size(1000, 800)
    driver.save_screenshot("bh.png")
    with open("bh.png", "rb") as image_file:
        image_bytes = image_file.read()
    camera2 =  image_processing (image_bytes )

    
    driver.execute_script("window.open('about:blank','3rdtab');")
    driver.switch_to.window("3rdtab")
    driver.get(cam3)
    driver.set_window_size(1000, 800)
    driver.save_screenshot("ts.png")
    with open("ts.png", "rb") as image_file:
        image_bytes = image_file.read()   
    camera3 =  image_processing (image_bytes )


    driver.execute_script("window.open('about:blank','4thtab');")
    driver.switch_to.window("4thtab")
    driver.get(cam4)
    driver.set_window_size(1000, 800)
    driver.save_screenshot("tht.png")
    with open("tht.png", "rb") as image_file:
        image_bytes = image_file.read()   
    camera4 = image_processing (image_bytes)

    


    driver.execute_script("window.open('about:blank','5thtab');")
    driver.switch_to.window("5thtab")
    driver.get(cam5)
    driver.set_window_size(1000, 800)
    driver.save_screenshot("hh.png")
    with open("hh.png", "rb") as image_file:
        image_bytes = image_file.read() 
    camera5 = image_processing (image_bytes)


    

    driver.quit()
    return camera1, camera2, camera3, camera4, camera5
   
def save_json_file (n): 
    processed_list = []
    processed_list = ScreenShot_Traffic_Camera(CMT8_Pham_Van_Hai, CMT8_Bac_Hai, CMT8_Truong_Son, CMT8_To_Hien_Thanh, CMT8_Hoa_Hung)
    merged_all =  json.dumps (processed_list)
    file_path = str(n) + "_" + "traffic_camera.json"
    with open(file_path, "w") as json_file:
       json.dump(merged_all, json_file)
CMT8_Pham_Van_Hai= "http://giaothong.hochiminhcity.gov.vn/expandcameraplayer/?camId=63ae7cfcbfd3d90017e8f422&camLocation=C%C3%A1ch%20M%E1%BA%A1ng%20Th%C3%A1ng%20T%C3%A1m%20%E2%80%93%20Ph%E1%BA%A1m%20V%C4%83n%20Hai&camMode=camera&videoUrl=https://d2zihajmogu5jn.cloudfront.net/bipbop-advanced/bipbop_16x9_variant.m3u8"
CMT8_Bac_Hai = "http://giaothong.hochiminhcity.gov.vn/expandcameraplayer/?camId=63ae798abfd3d90017e8f255&camLocation=C%C3%A1ch%20M%E1%BA%A1ng%20Th%C3%A1ng%20T%C3%A1m%20-%20B%E1%BA%AFc%20H%E1%BA%A3i&camMode=camera&videoUrl=https://d2zihajmogu5jn.cloudfront.net/bipbop-advanced/bipbop_16x9_variant.m3u8"
CMT8_Truong_Son = "http://giaothong.hochiminhcity.gov.vn/expandcameraplayer/?camId=63ae79aabfd3d90017e8f26a&camLocation=C%C3%A1ch%20M%E1%BA%A1ng%20Th%C3%A1ng%20T%C3%A1m%20-%20Tr%C6%B0%E1%BB%9Dng%20S%C6%A1n&camMode=camera&videoUrl=https://d2zihajmogu5jn.cloudfront.net/bipbop-advanced/bipbop_16x9_variant.m3u8"
CMT8_To_Hien_Thanh = "http://giaothong.hochiminhcity.gov.vn/expandcameraplayer/?camId=63ae7966bfd3d90017e8f240&camLocation=C%C3%A1ch%20M%E1%BA%A1ng%20Th%C3%A1ng%20T%C3%A1m%20-%20T%C3%B4%20Hi%E1%BA%BFn%20Th%C3%A0nh&camMode=camera&videoUrl=https://d2zihajmogu5jn.cloudfront.net/bipbop-advanced/bipbop_16x9_variant.m3u8"
CMT8_Hoa_Hung = "http://giaothong.hochiminhcity.gov.vn/expandcameraplayer/?camId=631955e7c9eae60017a1c30a&camLocation=C%C3%A1ch%20M%E1%BA%A1ng%20Th%C3%A1ng%20T%C3%A1m%20%E2%80%93%20H%C3%B2a%20H%C6%B0ng&camMode=camera&videoUrl=https://d2zihajmogu5jn.cloudfront.net/bipbop-advanced/bipbop_16x9_variant.m3u8"

while True:
    save_json_file (n)
    time.sleep(60)
    n = n + 1
    break