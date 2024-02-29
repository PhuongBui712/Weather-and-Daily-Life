import requests
import json
import os
from datetime import datetime

def get_distance_data(origin, destination, api_key):
    url = f"https://maps.googleapis.com/maps/api/distancematrix/json?origins={origin}&destinations={destination}&departure_time=now&key={api_key}"
    response = requests.get(url)
    data = response.json()
    return data

def analyze_traffic(data):
    result = {}
    if data['status'] == 'OK':
        row = data['rows'][0]
        element = row['elements'][0]
        duration = element['duration']['value']
        duration_in_traffic = element['duration_in_traffic']['value']
        traffic_difference = duration_in_traffic / duration
        if traffic_difference > 1.5:
            traffic = 'heavy'
        elif traffic_difference > 1.25:
            traffic = 'moderate'
        else:
            traffic = 'light'
        result['origin'] = data['origin_addresses'][0]
        result['destination'] = data['destination_addresses'][0]
        result['duration'] = duration
        result['duration_in_traffic'] = duration_in_traffic
        result['traffic'] = traffic
    else:
        print("Failed to fetch data.")
    return result


def save_data(result):
    # Tạo thư mục dựa trên ngày lấy dữ liệu
    current_date = datetime.now().strftime('%Y-%m-%d')
    folder_path = os.path.join('traffic_data', current_date)
    os.makedirs(folder_path, exist_ok=True)

    # Tạo tên tệp tin dựa trên thời gian lấy dữ liệu
    current_time = datetime.now().strftime('%H-%M-%S')
    file_name = f"{current_time}.json"
    file_path = os.path.join(folder_path, file_name)

    # Lưu dữ liệu vào tệp tin JSON
    with open(file_path, 'w', encoding='utf-8') as outfile:
        json.dump(result, outfile, ensure_ascii=False)

    print("Data saved successfully.")

# Thay thế 'YOUR_API_KEY' bằng API key của bạn
api_key = 'AIzaSyDg16DHjytKsi4JxUoZpQHMtbaxKxNpL1A'
# origin = 'Bùng Binh Phù Đổng'
# destination = '[ 10.792811, 106.653452]'
origin = "10.792786,106.653508"
destination = "10.771566,106.693125"

data = get_distance_data(origin, destination, api_key)
result = analyze_traffic(data)

save_data(result)