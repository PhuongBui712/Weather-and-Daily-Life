from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

options = Options()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

driver.get("http://giaothong.hochiminhcity.gov.vn/expandcameraplayer/?camId=63ae7cfcbfd3d90017e8f422&camLocation=C%C3%A1ch%20M%E1%BA%A1ng%20Th%C3%A1ng%20T%C3%A1m%20%E2%80%93%20Ph%E1%BA%A1m%20V%C4%83n%20Hai&camMode=camera&videoUrl=https://d2zihajmogu5jn.cloudfront.net/bipbop-advanced/bipbop_16x9_variant.m3u8")
driver.set_window_size(1000, 800)
driver.save_screenshot("Pham Van Hai", "pic.png")