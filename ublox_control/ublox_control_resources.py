"""
Common functions for gRPC UbloxControl service.
"""
import os
import json
from typing import List, Callable, Tuple
from contextlib import contextmanager

import datetime
from unittest import TestResult

# import redis
from serial import Serial
from pyubx2 import UBXReader, UBX_PROTOCOL, UBXMessage, SET_LAYER_RAM, POLL_LAYER_RAM, TXN_COMMIT, TXN_NONE

import ublox_control_pb2

# message enums
from ublox_control_pb2 import TestCase, InitSummary, CaptureCommand

""" Config globals"""
ublox_control_config_file = 'ublox_control_config.json'
F9T_BAUDRATE = 38400

# Configuration for metadata capture from the u-blox ZED-F9T timing chip
# TODO: make this a separate config file and track with version control etc.
default_f9t_config = {
    "chip_name": "ZED-F9T",
    "device": None, # /dev file connection
    "protocol": {
        "ubx": {
            "cfg_keys": ["CFG_MSGOUT_UBX_TIM_TP_USB", "CFG_MSGOUT_UBX_NAV_TIMEUTC_USB"], # default cfg keys to poll
            "packet_ids": ['NAV-TIMEUTC', 'TIM-TP'], # packet_ids to capture: should be in 1-1 corresp with the cfg_keys.
        }
    },
    "timeout (s)": 7,
}

""" Synchronization helper functions """

@contextmanager
def acquire_timeout(lock, condvar, timeout):
    """https://stackoverflow.com/questions/16740104/python-lock-with-statement-and-timeout"""
    result = lock.acquire(timeout=timeout)
    try:
        yield result
    finally:
        if result:
            lock.release()


""" Redis utility functions """
def get_f9t_redis_key(chip_name, chip_uid, prot_msg):
    """
    Returns the hashset key for the given prot_msg and chip
    @param chip_uid: the unique chip ID returned by the `UBX-SEC-UNIQID` message. Must be a 10-digit hex integer.
    @param prot_msg: u-blox protocol message name (e.g. `UBX-TIM-TP`) as specified in the ZED-F9T data sheet.
    @param chip_name: chip name. For now, this will always be `ZED-F9T`, but in the future we may want to record data for other u-blox chip types.
    @return: Redis hash set key in the following format "UBLOX_{chip_name}_{chip_uid}_{data_type}", where each field is uppercase.
    """
    # Verify the chip_uid is a 10-digit hex number
    chip_uid_emsg = f"chip_uid must be a 10-digit hex integer. Got {chip_uid=}"
    try:
        assert len(chip_uid) == 10, chip_uid_emsg
        int(chip_uid, 16)   # verifies chip_uid is a valid hex integer
    except ValueError or AssertionError:
        raise ValueError(chip_uid_emsg)
    return f"UBLOX_{chip_name.upper()}_{chip_uid.upper()}_{prot_msg.upper()}"


""" Testing utils """



def get_experiment_dir(start_timestamp, device):
    device_name = device.split('/')[-1]
    return f'{packet_data_dir}/start_{start_timestamp}.device_{device_name}'

""" ----- F9T I/O functions ---- """

def get_f9t_device(f9t_config):
    return f9t_config['device']

def get_f9t_unique_id(device):
    """
    Poll the unique ID of the f9t chip.
    We need to write a custom poll command because the pyubx2 library doesn't implement this cfg message.
    """
    # UBX-SEC-UNIQID poll message (class 0x27, id 0x03)
    UBX_UNIQID_POLL = bytes([0xB5, 0x62, 0x27, 0x03, 0x00, 0x00, 0x2A, 0x8F])
    with Serial(device, F9T_BAUDRATE, timeout=2) as stream:
        ubr = UBXReader(stream)
        # Flush any existing input
        stream.reset_input_buffer()
        print("Sending UBX-SEC-UNIQID poll...")
        stream.write(UBX_UNIQID_POLL)
        stream.flush()
        # Wait for and parse the response
        start_time = time.time()
        while True:
            if time.time() - start_time > 5:
                print("Timeout waiting for response.")
                break
            raw_data, parsed_data = ubr.read()
            if parsed_data and parsed_data.identity == 'SEC-UNIQID':
                # The unique ID is in parsed_data.uniqueId (should be bytes)
                unique_id = parsed_data.uniqueId.hex()
                print(f"Unique ID: {unique_id}")
                return unique_id
            # # Look for UBX-SEC-UNIQID response (class 0x27, id 0x03)
            # if raw_data and raw_data[2] == 0x27 and raw_data[3] == 0x03:
            #     # Payload is at raw_data[6:-2], uniqueId is bytes 4:36 of payload
            #     payload = raw_data[6:-2]
            #     if len(payload) >= 36:
            #         unique_id = payload[4:36].hex()
            #         print(f"ZED-F9T Unique ID: {unique_id}")
            #     else:
            #         print("Received payload too short.")
            #     break

def poll_f9t_config(device, cfg=default_f9t_config):
    """
    Poll the current configuration settings for each cfg_key specified in the cfg dict.
    On startup, should be 0 by default.
    """
    layer = POLL_LAYER_RAM
    position = 0
    ubx_cfg = cfg['protocol']['ubx']

    msg = UBXMessage.config_poll(layer, position, keys=ubx_cfg['cfg_keys'])
    print('Polling configuration:')
    with Serial(device, F9T_BAUDRATE, timeout=ubx_cfg['timeout (s)']) as stream:
        stream.write(msg.serialize())
        ubr_poll_status = UBXReader(stream, protfilter=UBX_PROTOCOL)
        raw_data, parsed_data = ubr_poll_status.read()
        if parsed_data is not None:
            print('\t', parsed_data)


def set_f9t_config(device, cfg=default_f9t_config):
    """Tell chip to start sending metadata packets for each cfg_key"""
    layer = SET_LAYER_RAM
    transaction = TXN_NONE
    timeout = cfg['timeout (s)']
    ubx_cfg = cfg['protocol']['ubx']

    # Tell chip to start sending metadata packets for each cfg_key. Note: Unspecified keys are initialized to 0.
    cfgData = [(cfg_key, 1) for cfg_key in ubx_cfg['cfg_keys']]  # 1 = start sending packets of type cfg_key.
    msg = UBXMessage.config_set(layer, transaction, cfgData)

    with Serial(device, F9T_BAUDRATE, timeout=timeout) as stream:
        print('Updating configuration:')
        stream.write(msg.serialize())
        ubr = UBXReader(stream, protfilter=UBX_PROTOCOL)
        for i in range(1):
            raw_data, parsed_data = ubr.read()
            if parsed_data is not None:
                print('\t', parsed_data)

# def read_route_guide_database():
#     """Reads the route guide database.
#
#     Returns:
#       The full contents of the route guide database as a sequence of
#         route_guide_pb2.Features.
#     """
#     feature_list = []
#     with open("route_guide_db.json") as route_guide_db_file:
#         for item in json.load(route_guide_db_file):
#             feature = ublox_control_pb2.Feature(
#                 name=item["name"],
#                 location=ublox_control_pb2.Point(
#                     latitude=item["location"]["latitude"],
#                     longitude=item["location"]["longitude"],
#                 ),
#             )
#             feature_list.append(feature)
#     return feature_list

