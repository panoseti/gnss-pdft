#!/usr/bin/env python3

import os
import datetime
import argparse
import json

from serial import Serial
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pyubx2 import UBXReader, UBX_PROTOCOL, UBXMessage, SET_LAYER_RAM, POLL_LAYER_RAM, TXN_COMMIT, TXN_NONE

qerr_config_file = 'qerr_config.json'
packet_data_dir = 'data'

def get_experiment_fname(start_timestamp):
    experiment_fname = f'start_{start_timestamp}'
    return experiment_fname

def save_tim_tp_data(df):

    with open(f'{packet_data_dir}/{tim_tp_fname}', 'w') as f:
        df.to_csv(f)
    return tim_tp_fname

def load_tim_tp_data(fname):
    with open(f'{packet_data_dir}/{fname}', 'r') as f:
        df = pd.read_csv(f, index_col=0)
    return df
def load_qerr_config():
    if not os.path.exists(qerr_config_file):
        raise FileNotFoundError(f"{qerr_config_file} does not exist."
                                f"\nPlease re-run this script with the help flag -h to see the option to create it.")
    with open(qerr_config_file) as f:
        return json.load(f)

def make_json_config():
        template = {
            'receiver': [
                {
                    'name': '...',
                    'device': '/dev/...',
                    'comments': '...'
                },
                {
                    'name': '...',
                    'device': '/dev/...',
                    'comments': '...'
                }
            ]
        }
        if not os.path.exists(qerr_config_file):
            with open(qerr_config_file, 'w') as f:
                json.dump(template, f, indent=4)
            print(f"created {qerr_config_file}")
        else:
            print(f"{qerr_config_file} already exists")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--make_json_config",
                        help=f"creates '{qerr_config_file}' if it does not exist",
                        action="store_true")
    parser.add_argument('--start',
                        help='start data capture',
                        action='store_true')
    args = parser.parse_args()
    if args.make_json_config:
        make_json_config()
    elif args.start:
        qerr_config = load_qerr_config()
        os.makedirs(packet_data_dir, exist_ok=True)
        start_timestamp = datetime.datetime.now().isoformat()

