from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
import time
from datetime import datetime
import os
import schedule
import pytz

options = Options()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

def ScreenShot_to_Url (cam1, cam2, cam3, cam4, cam5):
    utc_plus_7 = datetime.now().astimezone(pytz.timezone('Asia/Bangkok'))
    year= utc_plus_7.strftime("%Y")
    month = utc_plus_7.strftime("%m")
    day = utc_plus_7.strftime("%d")
    hour = utc_plus_7.strftime("%H")
    minute = utc_plus_7.strftime("%M")
    sec = utc_plus_7.strftime("%S")
    


    
    driver.get(cam1)
    element = driver.find_element(By.ID, "ext-element-1")
    driver.set_window_size(1000, 800)
    folder_name = os.path.join("Pham Van Hai", year + "_"  + month + "_"  + day + "_"  + hour + "_"  + minute + "_"  + sec  + ".png")
    driver.save_screenshot(folder_name)
    

    
    driver.execute_script("window.open('about:blank','2ndtab');")
    driver.switch_to.window("2ndtab")
    driver.get(cam2)
    element = driver.find_element(By.ID, "ext-element-1")
    driver.set_window_size(1000, 800)
    folder_name = os.path.join("Bac Hai", year + "_"  + month + "_"  + day + "_"  + hour + "_"  + minute + "_"  + sec  + ".png")
    driver.save_screenshot(folder_name)


    
    driver.execute_script("window.open('about:blank','3rdtab');")
    driver.switch_to.window("3rdtab")
    driver.get(cam3)
    element = driver.find_element(By.ID, "ext-element-1")
    driver.set_window_size(1000, 800)
    folder_name = os.path.join("Truong Son", year + "_"  + month + "_"  + day + "_"  + hour + "_"  + minute + "_"  + sec  + ".png")
    driver.save_screenshot(folder_name)



    driver.execute_script("window.open('about:blank','4thtab');")
    driver.switch_to.window("4thtab")
    driver.get(cam4)
    element = driver.find_element(By.ID, "ext-element-1")
    driver.set_window_size(1000, 800)
    folder_name = os.path.join("To Hien Thanh", year + "_"  + month + "_"  + day + "_"  + hour + "_"  + minute + "_"  + sec  + ".png")
    driver.save_screenshot(folder_name)

    


    driver.execute_script("window.open('about:blank','5thtab');")
    driver.switch_to.window("5thtab")
    driver.get(cam5)
    element = driver.find_element(By.ID, "ext-element-1")
    driver.set_window_size(1000, 800)
    folder_name = os.path.join("Hoa Hung", year + "_"  + month + "_"  + day + "_"  + hour + "_"  + minute + "_"  + sec  + ".png")
    driver.save_screenshot(folder_name)
    
   

    driver.quit()

   
   
CMT8_Pham_Van_Hai= "http://giaothong.hochiminhcity.gov.vn/expandcameraplayer/?camId=63ae7cfcbfd3d90017e8f422&camLocation=C%C3%A1ch%20M%E1%BA%A1ng%20Th%C3%A1ng%20T%C3%A1m%20%E2%80%93%20Ph%E1%BA%A1m%20V%C4%83n%20Hai&camMode=camera&videoUrl=https://d2zihajmogu5jn.cloudfront.net/bipbop-advanced/bipbop_16x9_variant.m3u8"
CMT8_Bac_Hai = "http://giaothong.hochiminhcity.gov.vn/expandcameraplayer/?camId=63ae798abfd3d90017e8f255&camLocation=C%C3%A1ch%20M%E1%BA%A1ng%20Th%C3%A1ng%20T%C3%A1m%20-%20B%E1%BA%AFc%20H%E1%BA%A3i&camMode=camera&videoUrl=https://d2zihajmogu5jn.cloudfront.net/bipbop-advanced/bipbop_16x9_variant.m3u8"
CMT8_Truong_Son = "http://giaothong.hochiminhcity.gov.vn/expandcameraplayer/?camId=63ae79aabfd3d90017e8f26a&camLocation=C%C3%A1ch%20M%E1%BA%A1ng%20Th%C3%A1ng%20T%C3%A1m%20-%20Tr%C6%B0%E1%BB%9Dng%20S%C6%A1n&camMode=camera&videoUrl=https://d2zihajmogu5jn.cloudfront.net/bipbop-advanced/bipbop_16x9_variant.m3u8"
CMT8_To_Hien_Thanh = "http://giaothong.hochiminhcity.gov.vn/expandcameraplayer/?camId=63ae7966bfd3d90017e8f240&camLocation=C%C3%A1ch%20M%E1%BA%A1ng%20Th%C3%A1ng%20T%C3%A1m%20-%20T%C3%B4%20Hi%E1%BA%BFn%20Th%C3%A0nh&camMode=camera&videoUrl=https://d2zihajmogu5jn.cloudfront.net/bipbop-advanced/bipbop_16x9_variant.m3u8"
CMT8_Hoa_Hung = "http://giaothong.hochiminhcity.gov.vn/expandcameraplayer/?camId=631955e7c9eae60017a1c30a&camLocation=C%C3%A1ch%20M%E1%BA%A1ng%20Th%C3%A1ng%20T%C3%A1m%20%E2%80%93%20H%C3%B2a%20H%C6%B0ng&camMode=camera&videoUrl=https://d2zihajmogu5jn.cloudfront.net/bipbop-advanced/bipbop_16x9_variant.m3u8"

def job():
    ScreenShot_to_Url (CMT8_Pham_Van_Hai, CMT8_Bac_Hai, CMT8_Truong_Son, CMT8_To_Hien_Thanh, CMT8_Hoa_Hung)

schedule.every(15).seconds.do(job)

while True:  
    schedule.run_pending()
    time.sleep(30)
