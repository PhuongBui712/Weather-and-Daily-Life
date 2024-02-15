import pandas as pd
import livepopulartimes
from datetime import datetime
import os

from scrape_services import get_basic_services_information, placeDetailSearch, API_KEY


def update_status():
    pass


def update_live_services_status():
    if not os.path.isfile(r'./data/services_data.csv'):
        get_basic_services_information()

    base_df = pd.read_csv(r'./data/services_data.csv')
    place_id_address_l = list(zip(base_df.id.tolist(), base_df.formattedAddress.tolist()))
    opening_list = []

    live_details_list = []
    for id, address in place_id_address_l:
        live_details_list.append(livepopulartimes.get_populartimes_by_address(address))
    #     try:
    #         opening = placeDetailSearch(id, 'regularOpeningHours', API_KEY)['currentOpeningHours']['openNow']
    #     except:
    #         opening = 'unknown'
    #
    #     try:
    #         live_popular = populartimes.get_populartimes(API_KEY, id)
    #         print(live_popular)
    #     except:
    #         pass
    #     else:
    #         live_details_list += live_popular
    #
    #     if opening:
    #         opening_list.append('Yes')
    #     elif not opening:
    #         opening_list.append('No')
    #     else: opening_list.append('Unknown')
    #
    live_df = pd.json_normalize(live_details_list)
    live_df.to_csv(r'./data/live_services.csv', index=False)
    # live_df.insert(4, 'openning', opening_list, True)


update_live_services_status()