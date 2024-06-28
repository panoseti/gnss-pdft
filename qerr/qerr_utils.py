
"""
Utility functions for qerr_utils
"""
import os
import json

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


qerr_config_file = 'qerr_config.json'
packet_data_dir = 'data'
BAUDRATE = 38400


def get_experiment_dir(start_timestamp):
    return f'{packet_data_dir}/start_{start_timestamp}'

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

def create_empty_df(data_type):
    """
    @param data_type: 'NAV-TIMEUTC', 'TIM-TP', or 'MERGED'.
    @return: empty df with schema of requested data_type.
    """
    if data_type == 'NAV-TIMEUTC':
        df = pd.DataFrame(
            columns=[
                'pkt_unix_timestamp_NAV-TIMEUTC',
                # NAV-TIMEUTC data
                'iTOW (ms)',
                'tAcc (ns)',
                'year',
                'month',
                'day',
                'hour',
                'min',
                'sec',
                'validTOW_flag',
                'validWKN_flag',
                'validUTC_flag',
                'utcStandard_NAV-TIMEUTC',
            ]
        )
    elif data_type == 'TIM-TP':
        df = pd.DataFrame(
            columns=[
                'pkt_unix_timestamp_TIM-TP',
                # TIM-TP data
                'towMS (ms)',  # towMS (unit: ms)
                'towSubMS',  # towSubMS (unit: ms, scale: 2^-32)
                'qErr (ps)',  # qErr (unit: ps)
                'week (weeks)',  # week (unit: weeks)
                'timeBase_flag',
                'utc_flag',
                'raim_flag',
                'qErrInvalid_flag',
                'timeRefGnss',
                'utcStandard_TIM-TP'
            ]
        )

    elif data_type == 'MERGED':
        df = pd.DataFrame(
            columns=[
                'pkt_unix_timestamp_TIM-TP',
                'pkt_unix_timestamp_NAV-TIMEUTC',
                # NAV-TIMEUTC data
                'iTOW (ms)',
                'tAcc (ns)',
                'year',
                'month',
                'day',
                'hour',
                'min',
                'sec',
                'validTOW_flag',
                'validWKN_flag',
                'validUTC_flag',
                'utcStandard_NAV-TIMEUTC',
                # TIM-TP data
                'towMS (ms)',  # towMS (unit: ms)
                'towSubMS',  # towSubMS (unit: ms, scale: 2^-32)
                'qErr (ps)',  # qErr (unit: ps)
                'week (weeks)',  # week (unit: weeks)
                'timeBase_flag',
                'utc_flag',
                'raim_flag',
                'qErrInvalid_flag',
                'timeRefGnss',
                'utcStandard_TIM-TP'
            ]
        )
    else:
        raise ValueError(f'Unrecognized data_type: {data_type}')
    return df


def plot():
    ...
