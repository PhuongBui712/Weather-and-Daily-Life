import os
from dotenv import load_dotenv
from tqdm import tqdm
import json

import googlemaps
import livepopulartimes

# load api key stored in .env file
load_dotenv()
API_KEY='AIzaSyDg16DHjytKsi4JxUoZpQHMtbaxKxNpL1A'

# global variable
location = 'đường Cách Mạng Tháng 8, Thành phố Hồ Chí Minh, Vietnam'
location_types = None
with open(r'/opt/airflow/dags/data/location_types.txt', 'r', encoding='utf-8') as file:
    location_types = [line.strip() for line in file.readlines()]


def update_json(file_path, new_json):
    # read exist json file
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            exist_data = json.load(file)
    except:
        exist_data = []

    # update json
    data = None
    if isinstance(new_json, list):
        data = exist_data + new_json
    else:
        data = exist_data.append(new_json)

    # write to file
    with open(file_path, 'w', encoding='utf-8') as fout:
        json.dump(data, fout, indent=3, ensure_ascii=False)


def is_live_data(name, address):
    try:
        is_live = livepopulartimes.get_populartimes_by_address(f'({name}) {address}')['populartimes']
    except:
        is_live = None

    return is_live


def places_search_by_type(query, max_pages=5, api_key=API_KEY):
    ggmap = googlemaps.Client(api_key)

    num_page = 1
    next_page_token = ''
    useful_info = ['place_id', 'name', 'business_status', 'formatted_address', 'price_level', 'rating', 'types']
    res = []
    while num_page <= max_pages:
        if num_page == 1:
            response = ggmap.places(query=query)
        else:
            response = ggmap.places(query=query, page_token=next_page_token)

        results = response['results']
        next_page_token = response.get('next_page_token', '')

        # just get services supported live-data
        for service in results:
            if is_live_data(service['name'], service['formatted_address']):
                res.append({info: service.get(info, None) for info in useful_info})

        num_page += 1
        if not next_page_token:
            break

    return res


def places_seach(location_types, location, progress_file_path, res_file_path, max_pages=5, api_key=API_KEY):
    # get lasted progress
    try:
        with open(progress_file_path, 'r', encoding='utf-8') as file:
            searched_types = set([line.strip() for line in file.readlines()])
    except:
            searched_types = set()

    # get types has not been crawled yet
    unsearched_types = list(set(location_types) - searched_types)

    # start searching
    res = []

    for t in tqdm(unsearched_types):
        search_res = places_search_by_type(', '.join([t, location]),
                                           max_pages=max_pages,
                                           api_key=api_key)

        # write the progress of searching type
        with open(progress_file_path, 'a', encoding='utf-8') as file:
            file.write(t + '\n')
        print(search_res)
        # write the search result
        update_json(res_file_path, search_res)

        # update final results
        res += search_res

    return res