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

def create_empty_df():
    df = pd.DataFrame(
        columns=[
            #
            # TIM-TP data
            'towMS (ms)',  # towMS (unit: ms)
             'towSubMS',  # towSubMS (unit: ms, scale: 2^-32)
             'qErr (ps)',  # qErr (unit: ps)
             'week (weeks)',  # week (unit: weeks)
             'timeBase_flag',
             'utc_flag',
             'raim_flag',
             'qErrInvalid_flag',
             'timeRefGnss'
        ]
    )
    return df




def get_experiment_fname(start_timestamp):
    return f'start_{start_timestamp}'

def save_data(df, fpath):
    with open(fpath, 'w') as f:
        df.to_csv(f)

def load_data(fpath):
    with open(fpath, 'r') as f:
        df = pd.read_csv(f, index_col=0)
    return df

def verify_data():
    ...

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
                    'device': '/dev/...',
                    'baudrate': 38400,
                    'comments': '...'
                },
                {
                    'name': '...',
                    'device': '/dev/...',
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


def collect_data(df, qerr_config, timeout=10):
    device = qerr_config['device']
    baudrate = qerr_config['baudrate']
    with Serial(device, baudrate, timeout=timeout) as stream:
        ubr = UBXReader(stream, protfilter=UBX_PROTOCOL)
        # Save packets and timestamp their arrival.
        packet_cache = {
            'TIM-TP': {
                'valid': False,
                'parsed_data': None,
                'timestamp': None
            },
            'NAV-TIMEUTC': {
                'valid': False,
                'parsed_data': None,
                'timestamp': None
            }
        }
        while True:
            # Check if packet cache is full.
            if packet_cache['TIM-TP']['valid'] and packet_cache['NAV-TIMEUTC']['valid']:
                if packet_cache['TIM-TP']['timestamp']:
                    ...
                    # TODO: verify that packet timestamps differ by no more than 1s.
                    # If packets do differ by more than 1s, write the earlier packet, and fill remaining data with NULLS?
                merged_data = {
                    **packet_cache['TIM-TP']['parsed_data'],
                    **packet_cache['NAV-TIMEUTC']['parsed_data']
                }
                if set(merged_data.keys()) != set(df.columns.tolist()):
                    raise KeyError(
                        'packet keys do not match data schema:\nPacket keys: {}\nSchema: {}'.format(
                            list(merged_data.keys()), df.columns.tolist()
                        )
                    )
                # Do write transaction
                df.loc[len(df)] = merged_data

                # Reset cache
                packet_cache['TIM-TP']['valid'] = False
                packet_cache['NAV-TIMEUTC']['valid'] = False

            raw_data, parsed_data = ubr.read()
            if parsed_data:
                if parsed_data.identity == 'NAV-TIMEUTC':
                    # UBX-NAV-TIMEUTC
                    packet_cache['NAV-TIMEUTC']['timestamp'] = datetime.datetime.now()
                    nav_time_utc_data = {
                        'towMS (ms)': parsed_data.towMS,
                        'towSubMS': parsed_data.towSubMS,
                        'qErr (ps)': parsed_data.qErr,
                        'week (weeks)': parsed_data.week,
                        'timeBase_flag': parsed_data.timeBase,
                        'utc_flag': parsed_data.utc,
                        'raim_flag': parsed_data.raim,
                        'qErrInvalid_flag': parsed_data.qErrInvalid,
                        'timeRefGnss': parsed_data.timeRefGnss,
                    }
                elif parsed_data.identity == 'TIM-TP':
                    # UBX-TIM-TP
                    packet_cache['TIM-TP']['timestamp'] = datetime.datetime.now()
                    packet_cache['TIM-TP']['parsed_data'] = {
                        'towMS (ms)':       parsed_data.towMS,
                        'towSubMS':         parsed_data.towSubMS,
                        'qErr (ps)':        parsed_data.qErr,
                        'week (weeks)':     parsed_data.week,
                        'timeBase_flag':    parsed_data.timeBase,
                        'utc_flag':         parsed_data.utc,
                        'raim_flag':        parsed_data.raim,
                        'qErrInvalid_flag': parsed_data.qErrInvalid,
                        'timeRefGnss':      parsed_data.timeRefGnss,
                    }


def start():
    qerr_config = load_qerr_config()
    os.makedirs(packet_data_dir, exist_ok=True)

    # TODO: Configure F9T chip to collect data.

    # TODO: Verify necessary packets are being sent.

    # TODO: Begin collecting and parsing packets.
    start_timestamp = datetime.datetime.now().isoformat()
    experiment_fname = get_experiment_fname(start_timestamp)
    fpath = f'{packet_data_dir}/{experiment_fname}'

    df = create_empty_df()
    try:
        collect_data(df, qerr_config)
    except KeyboardInterrupt:
        pass
    finally:
        save_data(df, fpath)



    # TODO: save data


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
        start()
