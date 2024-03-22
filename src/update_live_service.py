import os
import pandas as pd
from livepopulartimes import get_populartimes_by_address
from time import sleep
from datetime import datetime, timedelta
import pytz
import schedule

from scrape_services_data import places_seach, location, location_types
from preprocess_service_data import preprocess_data

basic_attributes = ['name', 'formatted_address']
live_attributes = ['place_id', 'name', 'datetime', 'rating', 'rating_n',
                   'populartimes', 'usual_popularity', 'current_popularity']
Saigon_timezone = pytz.timezone('Asia/Saigon')


def get_basic_data(file_path, attributes):
    df = pd.read_csv(file_path)

    return df.loc[:, attributes].to_dict()


def map_weekday(origin):
    if origin:
        return origin - 1
    return 6


def update_live_data(basic_file_path, basic_attributes, attributes, file_dir=None, write_csv=False):
    basic_data_dict = get_basic_data(basic_file_path, basic_attributes)

    # define live data dictionary
    live_data_dict = {attr: [] for attr in attributes}
    live_data_dict.update({'datetime': []})
    name_dict, addr_dict = basic_data_dict[basic_attributes[0]], basic_data_dict[basic_attributes[1]]

    # get datetime
    dt_obj = datetime.now(Saigon_timezone)
    dt = dt_obj.strftime("%d-%m-%Y %H:%M")
    weekday = int(dt_obj.strftime('%w'))
    hour = int(dt_obj.strftime('%H'))

    # get live data
    for name, addr in zip(list(name_dict.values()), list(addr_dict.values())):
        live_response = get_populartimes_by_address(f'({name}) {addr}')

        if 'populartimes' in live_response:
            live_data_dict['usual_popularity'].append(live_response['populartimes'][map_weekday(weekday)]['data'][hour])
        else:
            continue

        live_data_dict['datetime'].append(dt)
        for attr in attributes:
            if attr not in ['datetime', 'usual_popularity']:
                live_data_dict[attr].append(live_response.get(attr, None))

    df = pd.DataFrame(live_data_dict)
    filename = '_'.join(['services', dt_obj.strftime("%d-%m-%Y_%H-%M")])
    filename += '.csv'
    if file_dir and write_csv:
        df.to_csv(os.path.join(file_dir, filename), sep=',')

    return df


def job():
    update_live_data(r'../data/services/services.csv',
                     basic_attributes=basic_attributes,
                     attributes=live_attributes,
                     file_dir=r'../data/services/live_services/',
                     write_csv=True)


if __name__ == "__main__":
    # check if scraping basic data yet
    if not os.path.isfile(r'../data/services/services.csv') and \
            not os.path.isfile(r'../data/services/services.json'):
        service_list = places_seach(location_types, location,
                                    r'../data/services/searching_progress.txt',
                                    r'../data/services/services.json',
                                    max_pages=10)

    elif not os.path.isfile(r'../data/services/services.csv'):
        preprocess_data(r'../data/services/services.json',
                        included_pattern=r'(Cách Mạng Tháng 8|CMT8).*(Hồ Chí Minh|HCM)',
                        file_out=r'../data/services/services.csv')

    # update live data
    schedule.every(15).minutes.do(job)
    while True:
        schedule.run_pending()
