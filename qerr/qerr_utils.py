
"""
Utility functions for qerr_utils
"""
import os
import json

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

qerr_config_file = 'qerr_config.json'

def save_data(df, fpath):
    with open(fpath, 'w') as f:
        df.to_csv(f)

def load_data(fpath):
    with open(fpath, 'r') as f:
        df = pd.read_csv(f, index_col=0)
    return df

def load_qerr_config():
    if not os.path.exists(qerr_config_file):
        raise FileNotFoundError(f"{qerr_config_file} does not exist."
                                f"\nPlease re-run this script with the help flag -h to see the option to create it.")
    with open(qerr_config_file) as f:
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
    if not os.path.exists(qerr_config_file):
        with open(qerr_config_file, 'w') as f:
            json.dump(config_template, f, indent=4)
        print(f"created {qerr_config_file}")
    else:
        print(f"{qerr_config_file} already exists")


def plot():
    ...
