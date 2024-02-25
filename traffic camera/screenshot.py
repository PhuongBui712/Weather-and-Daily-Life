from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import time

num = 1
def ScreenShot_to_Url (url, num):
    service = Service (executable_path = "chromedriver.exe")
    driver = webdriver.Chrome(service=service)
    driver.get(url)
    time.sleep(2)
     
    element = driver.find_element(By.ID, "ext-element-1")
    driver.set_window_size(1920, 1080)
    time.sleep(2)
    driver.save_screenshot( str(num) + "_" + "traffic.png")
    driver.quit()

while True:
    ScreenShot_to_Url ("http://giaothong.hochiminhcity.gov.vn/expandcameraplayer/?camId=6318283cc9eae60017a19f0c&camLocation=Nguy%E1%BB%85n%20Th%E1%BB%8B%20Minh%20Khai%20-%20%C4%90inh%20Ti%C3%AAn%20Ho%C3%A0ng%201&camMode=camera&videoUrl=https://d2zihajmogu5jn.cloudfront.net/bipbop-advanced/bipbop_16x9_variant.m3u8", num)
    num = num + 1
    time.sleep(600)

   