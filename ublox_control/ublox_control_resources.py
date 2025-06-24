"""
Common functions for the server-side cli and gRPC UbloxControl server.
"""
import os
import json

import datetime
from unittest import TestResult

# import redis
from serial import Serial
from pyubx2 import UBXReader, UBX_PROTOCOL, UBXMessage, SET_LAYER_RAM, POLL_LAYER_RAM, TXN_COMMIT, TXN_NONE

import ublox_control_pb2

# message enums
from ublox_control_pb2 import TestCase, InitSummary

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


""" Testing utils """

def test_0():
    msg = "todo: replace with real test 0"
    return True, msg

def test_1():
    msg = "todo: replace with real test 1"
    return False, msg

tests = [
    {
        "name": "example_0",
        "test_fn": test_0,
    },
    {
        "name": "example_1",
        "test_fn": test_1,
    }
]

def run_initialization_tests():
    """Run each test case in tests.
    Each test returns a tuple of (result: bool, message: str)
        - result: whether the test passed
        - message: information about any failure cases or warnings.
    Returns enum init_status and a list of test_results.
    """
    # TODO: add real validation checks here
    all_pass = True
    test_results = []
    for test in tests:
        result, message = test["test_fn"]()
        if result:
            result = TestCase.TestResult.PASS
        else:
            result = TestCase.TestResult.FAIL
            all_pass = False

        test_result = ublox_control_pb2.TestCase(
            name=test["name"],
            result=result,
            message=message
        )
        test_results.append(test_result)
    if all_pass:
        init_status = InitSummary.InitStatus.SUCCESS
    else:
        init_status = InitSummary.InitStatus.FAILURE
    return init_status, test_results


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
