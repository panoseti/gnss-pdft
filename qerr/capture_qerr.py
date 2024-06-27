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


def create_empty_df():
    df = pd.DataFrame(
        columns=[
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
            'utcStandard_NAV-TIMEUTC'
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
    return df


def collect_data(df, qerr_config, timeout=10):
    device = qerr_config['device']
    baudrate = qerr_config['baudrate']
    with Serial(device, baudrate, timeout=timeout) as stream:
        ubr = UBXReader(stream, protfilter=UBX_PROTOCOL)
        # Save packets and timestamp their arrival.
        packet_cache = {
            'NAV-TIMEUTC': {
                'valid': False,
                'parsed_data': None,
                'timestamp': None
            },
            'TIM-TP': {
                'valid': False,
                'parsed_data': None,
                'timestamp': None
            }
        }
        while True:
            # Check if packet cache is full.
            if packet_cache['TIM-TP']['valid'] and packet_cache['NAV-TIMEUTC']['valid']:
                # Verify that packet timestamps differ by no more than 1s.
                time_diff = abs(packet_cache['TIM-TP']['timestamp'] - packet_cache['NAV-TIMEUTC']['timestamp'])
                microsec_time_diff = time_diff.seconds * 1e6 + time_diff.microseconds
                if microsec_time_diff < 1e6:
                    # Merge packet data into a single dict
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
                else:
                    # Drop the earlier packet.
                    if packet_cache['TIM-TP']['timestamp'] < packet_cache['NAV-TIMEUTC']['timestamp']:
                        packet_cache['TIM-TP']['valid'] = False
                    else:
                        packet_cache['NAV-TIMEUTC']['valid'] = False
            # Wait for next packet
            raw_data, parsed_data = ubr.read()
            if parsed_data:
                if parsed_data.identity == 'NAV-TIMEUTC':
                    # UBX-NAV-TIMEUTC
                    packet_cache['NAV-TIMEUTC']['timestamp'] = datetime.datetime.now()
                    packet_cache['NAV-TIMEUTC']['parsed_data'] = {
                        'iTOW (ms)':        parsed_data.iTOW,
                        'tAcc (ns)':        parsed_data.tAcc,
                        'year':             parsed_data.year,
                        'month':            parsed_data.month,
                        'day':              parsed_data.day,
                        'hour':             parsed_data.hour,
                        'min':              parsed_data.min,
                        'sec':              parsed_data.sec,
                        'validTOW_flag':    parsed_data.validTOW,
                        'validWKN_flag':    parsed_data.validWKN,
                        'validUTC_flag':    parsed_data.validUTC,
                        'utcStandard_NAV-TIMEUTC':  parsed_data.utcStandard
                    }
                elif parsed_data.identity == 'TIM-TP':
                    # UBX-TIM-TP
                    packet_cache['TIM-TP']['timestamp'] = datetime.datetime.now()
                    packet_cache['TIM-TP']['parsed_data'] = {
                        'towMS (ms)':           parsed_data.towMS,
                        'towSubMS':             parsed_data.towSubMS,
                        'qErr (ps)':            parsed_data.qErr,
                        'week (weeks)':         parsed_data.week,
                        'timeBase_flag':        parsed_data.timeBase,
                        'utc_flag':             parsed_data.utc,
                        'raim_flag':            parsed_data.raim,
                        'qErrInvalid_flag':     parsed_data.qErrInvalid,
                        'timeRefGnss':          parsed_data.timeRefGnss,
                        'utcStandard_TIM-TP':   parsed_data.utcStandard
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
