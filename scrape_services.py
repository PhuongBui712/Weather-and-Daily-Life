import pandas as pd
from requests import get, post
import json

API_KEY = 'AIzaSyD-kCN4eo8MiZoxg6j-sQb4iAxX_4hG7_s'
street = ' trên Đường Láng, Thanh Xuân, Hà Nội'
location_types = ['Sách', 'Barbershop, tiệm hớt tóc',
                  'Nhà thuốc', 'bệnh viện', 'Công ty', 'doanh nghiệp',
                  'Trạm xăng dầu', 'Gas tàu', 'bến xe bus',
                  'Dịch vụ oto', 'Dịch vụ xe máy',
                  'Trường học',
                  'Nhà hàng', 'Quán ăn', 'Coffee',
                  'Dịch vụ']
FIELDMASK = 'places.id,places.displayName,places.formattedAddress,places.types,places.priceLevel,places.rating,places.regularOpeningHours,places.userRatingCount'


def textPlaceSearch(text, max_res=1, fieldmask=FIELDMASK, api_key=API_KEY):
    textSearch_URL = r'https://places.googleapis.com/v1/places:searchText'
    headers = {
        'Content-Type': 'application/json',
        'X-Goog-Api-Key': api_key,
        'X-Goog-FieldMask': fieldmask
    }

    data = {
        'textQuery': text,
        'maxResultCount': max_res
    }

    response = post(textSearch_URL,
                    data=json.dumps(data),
                    headers=headers)
    if response.status_code == 200:
        return response.json()

    return -1


def check_valid_place(api_response):
    varified_str = ['Đ. Láng', 'Đường Láng']
    res = []
    for place in api_response['places']:
        if varified_str[0] in place['formattedAddress'] or varified_str[1] in place['formattedAddress']:
            res.append(place)

    return res


def placeDetailSearch(place_id, fieldmask=FIELDMASK, api_key=API_KEY):
    detailSearch_URL = r'https://places.googleapis.com/v1/places/' + place_id
    headers = {
        'Content-Type': 'application/json',
        'X-Goog-Api-Key': api_key,
        'X-Goog-FieldMask': fieldmask
    }

    response = get(detailSearch_URL, headers=headers)
    if response.status_code != 200:
        return -1

    return response.json()


def get_basic_services_information():
    response_list = []
    for type in location_types:
        response = textPlaceSearch(type + street, 20)
        if response != -1:
            response = check_valid_place(response)
            response_list += response

    df = pd.json_normalize(response_list)
    df.to_csv('./data/services_data.csv', index=False)