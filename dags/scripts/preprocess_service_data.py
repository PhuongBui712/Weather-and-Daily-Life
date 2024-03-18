import json
import pandas as pd
import re
import os


def load_json_data(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        json_data = json.load(file)

    return json_data


def is_valid_location(formatted_addr, included_pattern):
    if re.search(included_pattern, formatted_addr):
        return True

    return False


def preprocess_data(file_path, included_pattern, file_out=None, write_csv=True, delete_raw=False):
    json_data = load_json_data(file_path)
    df = pd.json_normalize(json_data)

    # drop duplicate
    df = df.drop_duplicates(subset=['place_id'])

    # check include string
    df = df.loc[df.formatted_address.apply(lambda x : is_valid_location(x, included_pattern))]

    if file_out and write_csv:
        df.to_csv(file_out, sep=',')

    if delete_raw:
        os.remove(file_path)

    return df