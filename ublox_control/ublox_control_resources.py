"""
Common functions for gRPC UbloxControl service.
"""
import os
import json
from typing import List, Callable, Tuple

import datetime
from unittest import TestResult

# import redis
from serial import Serial
from pyubx2 import UBXReader, UBX_PROTOCOL, UBXMessage, SET_LAYER_RAM, POLL_LAYER_RAM, TXN_COMMIT, TXN_NONE

import ublox_control_pb2

# message enums
from ublox_control_pb2 import TestCase, InitSummary, CaptureCommand

ublox_control_config_file = 'ublox_control_config.json'


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

def check_device_exists(device):
    msg = ""
    if device is not None:
        if not os.path.exists(device):
            raise FileNotFoundError(f'Cannot access {device}')
            msg = f'Cannot access {device}'
        return True, msg
    return False, msg


def check_f9t_dataflow(device, cfg):
    """
    Verify all packets specified in the 'packet_ids' fields of cfg are being received.
    NOTE: for now this is hardcoded for UBX packets.
    @return: True if all packets have been received, False otherwise.
    """
    timeout = cfg['timeout (s)']
    ubx_cfg = cfg['protocol']['ubx']
    baudrate = cfg['baudrate']

    # Initialize dict for recording whether we're receiving packets of each type.
    pkt_id_flags = {pkt_id: False for pkt_id in ubx_cfg['packet_ids']}

    msg = ""
    try:
        with Serial(device, baudrate, timeout=timeout) as stream:
            ubr = UBXReader(stream, protfilter=UBX_PROTOCOL)
            print('Verifying packets are being received... (If stuck at this step, re-run with the "init" option.)')

            for i in range(timeout):  # assumes config packets are send every second -> waits for timeout seconds.
                raw_data, parsed_data = ubr.read() # blocking read operation -> waits for next UBX_PROTOCOL packet.
                if parsed_data:
                    for pkt_id in pkt_id_flags.keys():
                        if parsed_data.identity == pkt_id:
                            pkt_id_flags[pkt_id] = True
                if all(pkt_id_flags.values()):
                    print('All packets are being received.')
                    return True, msg
    except KeyboardInterrupt:
        print('Interrupted by KeyboardInterrupt.')
        return False, msg
    raise Exception(f'Not all packets are being received. Check the following for details: {pkt_id_flags=}')

def test_0():
    msg = "todo: replace with real test 0"
    return True, msg

def test_1():
    msg = "todo: replace with real test 1"
    return False, msg



def test_redis_daq_to_headnode_connection(host, port, socket_timeout):
    """
    Test Redis connection with specified connection parameters.
        1. Connect to Redis.
        2. Perform a series of pipelined write operations to a test hashset.
        3. Verify whether these writes were successful.
    Returns number of failed operations. (0 = test passed, 1+ = test failed.)
    """
    failures = 0

    try:
        print(f"Connecting to {host}:{port}")
        r = redis.Redis(host=host, port=port, db=0, socket_timeout=socket_timeout)
        if not r.ping():
            raise FileNotFoundError(f'Cannot connect to {host}:{port}')

        timestamp = datetime.datetime.now().isoformat()
        # Create a redis pipeline to efficiently send key updates.
        pipe = r.pipeline()

        # Queue updates to a test hash: write current timestamp to 10 test keys
        for i in range(20):
            field = f't{i}'
            value = datetime.datetime.now().isoformat()
            pipe.hset('TEST', field, value)

        # Execute the pipeline and get results
        results = pipe.execute(raise_on_error=False)

        # Check if each operation succeeded
        success = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                success.append('0')
                failures += 1
                print(f"Command {i} failed: {result=}")
            else:
                success.append('1')
        print(f'[{timestamp}]: success = [{" ".join(success)}]')

    except Exception:
        # Fail safely by reporting a failure in case of any exceptions
        return 1
    return failures



# def save_data(df, fpath):
#     with open(fpath, 'w') as f:
#         df.to_csv(f)
#
# def load_data(fpath):
#     with open(fpath, 'r') as f:
#         df = pd.read_csv(f, index_col=0)
#     return df
#
# def load_qerr_config():
#     if not os.path.exists(ublox_control_config_file):
#         raise FileNotFoundError(f"{ublox_control_config_file} does not exist."
#                                 f"\nPlease re-run this script with the help flag -h to see the option to create it.")
#     with open(ublox_control_config_file) as f:
#         return json.load(f)
#
# def make_json_config():
#     config_template = {
#         'receiver': [
#             {
#                 'name': '...',
#                 'device_rpi': '/dev/...',
#                 'device_wsl': '/dev/...',
#                 'baudrate': 38400,
#                 'comments': '...'
#             },
#             {
#                 'name': '...',
#                 'device_rpi': '/dev/...',
#                 'device_wsl': '/dev/...',
#                 'baudrate': 38400,
#                 'comments': '...'
#             }
#         ]
#     }
#     if not os.path.exists(ublox_control_config_file):
#         with open(ublox_control_config_file, 'w') as f:
#             json.dump(config_template, f, indent=4)
#         print(f"created {ublox_control_config_file}")
#     else:
#         print(f"{ublox_control_config_file} already exists")
#
