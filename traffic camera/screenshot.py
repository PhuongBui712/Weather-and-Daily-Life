from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import time
from datetime import datetime
import os

def ScreenShot_to_Url (cam1, cam2, cam3, cam4, cam5):
    year= datetime.now().strftime("%Y")
    month = datetime.now().strftime("%m")
    day = datetime.now().strftime("%d")
    hour = datetime.now().strftime("%H")
    minute = datetime.now().strftime("%M")
    sec = datetime.now().strftime("%S")
    webBrowser = webdriver.Chrome()

    
    webBrowser.get(cam1)
    element = webBrowser.find_element(By.ID, "ext-element-1")
    webBrowser.set_window_size(1000, 800)
    folder_name = os.path.join("Pham Van Hai", year + "_"  + month + "_"  + day + "_"  + hour + "_"  + minute + "_"  + sec  + ".png")
    webBrowser.save_screenshot(folder_name)
    

    
    webBrowser.execute_script("window.open('about:blank','2ndtab');")
    webBrowser.switch_to.window("2ndtab")
    webBrowser.get(cam2)
    element = webBrowser.find_element(By.ID, "ext-element-1")
    webBrowser.set_window_size(1000, 800)
    folder_name = os.path.join("Bac Hai", year + "_"  + month + "_"  + day + "_"  + hour + "_"  + minute + "_"  + sec  + ".png")
    webBrowser.save_screenshot(folder_name)


    
    webBrowser.execute_script("window.open('about:blank','3rdtab');")
    webBrowser.switch_to.window("3rdtab")
    webBrowser.get(cam3)
    element = webBrowser.find_element(By.ID, "ext-element-1")
    webBrowser.set_window_size(1000, 800)
    folder_name = os.path.join("Truong Son", year + "_"  + month + "_"  + day + "_"  + hour + "_"  + minute + "_"  + sec  + ".png")
    webBrowser.save_screenshot(folder_name)



    webBrowser.execute_script("window.open('about:blank','4thtab');")
    webBrowser.switch_to.window("4thtab")
    webBrowser.get(cam4)
    element = webBrowser.find_element(By.ID, "ext-element-1")
    webBrowser.set_window_size(1000, 800)
    folder_name = os.path.join("To Hien Thanh", year + "_"  + month + "_"  + day + "_"  + hour + "_"  + minute + "_"  + sec  + ".png")
    webBrowser.save_screenshot(folder_name)

    


    webBrowser.execute_script("window.open('about:blank','5thtab');")
    webBrowser.switch_to.window("5thtab")
    webBrowser.get(cam5)
    element = webBrowser.find_element(By.ID, "ext-element-1")
    webBrowser.set_window_size(1000, 800)
    folder_name = os.path.join("Hoa Hung", year + "_"  + month + "_"  + day + "_"  + hour + "_"  + minute + "_"  + sec  + ".png")
    webBrowser.save_screenshot(folder_name)
    
   

    webBrowser.quit()

   
   
CMT8_Pham_Van_Hai= "http://giaothong.hochiminhcity.gov.vn/expandcameraplayer/?camId=63ae7cfcbfd3d90017e8f422&camLocation=C%C3%A1ch%20M%E1%BA%A1ng%20Th%C3%A1ng%20T%C3%A1m%20%E2%80%93%20Ph%E1%BA%A1m%20V%C4%83n%20Hai&camMode=camera&videoUrl=https://d2zihajmogu5jn.cloudfront.net/bipbop-advanced/bipbop_16x9_variant.m3u8"
CMT8_Bac_Hai = "http://giaothong.hochiminhcity.gov.vn/expandcameraplayer/?camId=63ae798abfd3d90017e8f255&camLocation=C%C3%A1ch%20M%E1%BA%A1ng%20Th%C3%A1ng%20T%C3%A1m%20-%20B%E1%BA%AFc%20H%E1%BA%A3i&camMode=camera&videoUrl=https://d2zihajmogu5jn.cloudfront.net/bipbop-advanced/bipbop_16x9_variant.m3u8"
CMT8_Truong_Son = "http://giaothong.hochiminhcity.gov.vn/expandcameraplayer/?camId=63ae79aabfd3d90017e8f26a&camLocation=C%C3%A1ch%20M%E1%BA%A1ng%20Th%C3%A1ng%20T%C3%A1m%20-%20Tr%C6%B0%E1%BB%9Dng%20S%C6%A1n&camMode=camera&videoUrl=https://d2zihajmogu5jn.cloudfront.net/bipbop-advanced/bipbop_16x9_variant.m3u8"
CMT8_To_Hien_Thanh = "http://giaothong.hochiminhcity.gov.vn/expandcameraplayer/?camId=63ae7966bfd3d90017e8f240&camLocation=C%C3%A1ch%20M%E1%BA%A1ng%20Th%C3%A1ng%20T%C3%A1m%20-%20T%C3%B4%20Hi%E1%BA%BFn%20Th%C3%A0nh&camMode=camera&videoUrl=https://d2zihajmogu5jn.cloudfront.net/bipbop-advanced/bipbop_16x9_variant.m3u8"
CMT8_Hoa_Hung = "http://giaothong.hochiminhcity.gov.vn/expandcameraplayer/?camId=631955e7c9eae60017a1c30a&camLocation=C%C3%A1ch%20M%E1%BA%A1ng%20Th%C3%A1ng%20T%C3%A1m%20%E2%80%93%20H%C3%B2a%20H%C6%B0ng&camMode=camera&videoUrl=https://d2zihajmogu5jn.cloudfront.net/bipbop-advanced/bipbop_16x9_variant.m3u8"

while True:  
    ScreenShot_to_Url (CMT8_Pham_Van_Hai, CMT8_Bac_Hai, CMT8_Truong_Son, CMT8_To_Hien_Thanh, CMT8_Hoa_Hung)
    time.sleep (6)
