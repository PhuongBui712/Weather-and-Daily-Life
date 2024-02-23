import os
from dotenv import load_dotenv
from tqdm import tqdm

import json

import googlemaps
import googletrans
import livepopulartimes

# load api key stored in .env file
load_dotenv()
API_KEY = os.getenv("API_KEY")

# global variable
location = 'Hà Nội, Việt Nam'
location_types = []
with open(r'../data/location_type.txt', 'r') as file:
    for line in file:
        location_types.append(line.strip().replace('_', ' '))


def update_json(file_path, new_json):
    # read exist json file
    try:
        with open(file_path, 'r') as file:
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
    with open(file_path, 'w') as fout:
        json.dump(data, fout, indent=3)


def is_live_data(address):
    try:
        is_live = livepopulartimes.get_populartimes_by_address(address)['current_popularity']
    except:
        is_live = None

    return is_live


def places_search_by_type(query, location, service_type, max_pages=5, api_key=API_KEY):
    ggmap = googlemaps.Client(api_key)

    num_page = 1
    next_page_token = ''
    useful_info = ['place_id', 'name', 'business_status', 'formatted_address', 'price_level', 'rating', 'types']
    res = []
    while num_page <= max_pages or next_page_token:
        if num_page == 1:
            response = ggmap.places(query=query, location=location, type=service_type)
        else:
            response = ggmap.places(query=query, location=location, type=service_type, page_token=next_page_token)

        results = response['results']
        next_page_token = response.get('next_page_token', '')

        # just get services supported live-data
        for service in results:
            if is_live_data(service['formatted_address']):
                res.append({info: service.get(info, None) for info in useful_info})
        num_page += 1

    return res


def places_seach(types, location, progress_file_path, res_file_path, max_pages=5, api_key=API_KEY):
    # get lasted progress
    try:
        with open(progress_file_path, 'r') as file:
            progress = set([line.strip() for line in file.readlines()])
    except:
            progress = set()

    # get unsearched types
    unsearched_types = list(set(types) - progress)
    translator = googletrans.Translator()
    viet_types = [translator.translate(loc, src='en', dest='vi').text for loc in unsearched_types]

    res = []
    for type in tqdm(zip(viet_types, unsearched_types)):
        search_res = places_search_by_type(f'{type[0]} ở Hà Nội, Việt Nam',
                                           location=location,
                                           service_type=type[1],
                                           max_pages=max_pages,
                                           api_key=api_key)

        # write the progress of searching type
        with open(progress_file_path, 'a') as file:
            file.write(type[1] + '\n')

        # write the search result
        update_json(res_file_path, search_res)

        # update final results
        res += search_res

    return res


service_list = places_seach(location_types, location,
                            r'../data/seaching_progress.txt',
                            r'./data/services.json',
                            max_pages=10)