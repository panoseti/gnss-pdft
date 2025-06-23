

"""

Server-side code to handle GNSS control resources.
Run on the computer

"""
import os
import json

# import pandas as pd

import json
import ublox_control_pb2
# import seaborn as sns
# import matplotlib.pyplot as plt

ublox_control_config_file = 'ublox_control_config.json'

def save_data(df, fpath):
    with open(fpath, 'w') as f:
        df.to_csv(f)

def load_data(fpath):
    with open(fpath, 'r') as f:
        df = pd.read_csv(f, index_col=0)
    return df

def load_qerr_config():
    if not os.path.exists(ublox_control_config_file):
        raise FileNotFoundError(f"{ublox_control_config_file} does not exist."
                                f"\nPlease re-run this script with the help flag -h to see the option to create it.")
    with open(ublox_control_config_file) as f:
        return json.load(f)

def make_json_config():
    config_template = {
        'receiver': [
            {
                'name': '...',
                'device_rpi': '/dev/...',
                'device_wsl': '/dev/...',
                'baudrate': 38400,
                'comments': '...'
            },
            {
                'name': '...',
                'device_rpi': '/dev/...',
                'device_wsl': '/dev/...',
                'baudrate': 38400,
                'comments': '...'
            }
        ]
    }
    if not os.path.exists(ublox_control_config_file):
        with open(ublox_control_config_file, 'w') as f:
            json.dump(config_template, f, indent=4)
        print(f"created {ublox_control_config_file}")
    else:
        print(f"{ublox_control_config_file} already exists")



"""Common resources used in the gRPC route guide example."""




def read_route_guide_database():
    """Reads the route guide database.

    Returns:
      The full contents of the route guide database as a sequence of
        route_guide_pb2.Features.
    """
    feature_list = []
    with open("route_guide_db.json") as route_guide_db_file:
        for item in json.load(route_guide_db_file):
            feature = ublox_control_pb2.Feature(
                name=item["name"],
                location=ublox_control_pb2.Point(
                    latitude=item["location"]["latitude"],
                    longitude=item["location"]["longitude"],
                ),
            )
            feature_list.append(feature)
    return feature_list