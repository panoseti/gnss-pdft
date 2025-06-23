#!/usr/bin/env python3

"""
Command Line Interface for the Server-side RPC methods.
These commands expect the following to function correctly:
    1. A valid network connection to the Redis database on the headnode.
    2. R/W user permissions to the Redis UBLOX hashset.
    3. A valid ZED-F9T u-blox chip is available as a /dev/... file.
    4. All required Python packages are installed.

See https://github.com/semuconsulting/pyubx2 for documentation on UBX interface documentation.
"""

import time
import os
import datetime
import argparse

import redis
from serial import Serial
from pyubx2 import UBXReader, UBX_PROTOCOL, UBXMessage, SET_LAYER_RAM, POLL_LAYER_RAM, TXN_COMMIT, TXN_NONE
#from utils import *

BAUDRATE = 38400
packet_data_dir = 'data'

# Configuration for metadata capture from the u-blox ZED-F9T timing chip
f9t_config = {
    "chip_name": "ZED-F9T",
    "chip_uid": None,
    "protocol": {
        "ubx": {
            "device": None,
            "cfg_keys": ["CFG_MSGOUT_UBX_TIM_TP_USB", "CFG_MSGOUT_UBX_NAV_TIMEUTC_USB"], # default cfg keys to poll
            "packet_ids": ['NAV-TIMEUTC', 'TIM-TP'], # packet_ids to capture: should be in 1-1 corresp with the cfg_keys.
        }
    },
    "timeout (s)": 7,
    "init_success": False,
}

"""Server utility functions"""

def get_experiment_dir(start_timestamp, device):
    device_name = device.split('/')[-1]
    return f'{packet_data_dir}/start_{start_timestamp}.device_{device_name}'

def get_f9t_unique_id(device):
    """
    Poll the unique ID of the f9t chip.
    We need to write a custom poll command because the pyubx2 library doesn't implement this cfg message.
    """
    # UBX-SEC-UNIQID poll message (class 0x27, id 0x03)
    UBX_UNIQID_POLL = bytes([0xB5, 0x62, 0x27, 0x03, 0x00, 0x00, 0x2A, 0x8F])
    with Serial(device, BAUDRATE, timeout=2) as stream:
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

def poll_f9t_config(device, cfg=f9t_config):
    """
    Poll the current configuration settings for each cfg_key specified in the cfg dict.
    On startup, should be 0 by default.
    """
    layer = POLL_LAYER_RAM
    position = 0
    ubx_cfg = cfg['protocol']['ubx']

    msg = UBXMessage.config_poll(layer, position, keys=ubx_cfg['cfg_keys'])
    print('Polling configuration:')
    with Serial(device, BAUDRATE, timeout=ubx_cfg['timeout (s)']) as stream:
        stream.write(msg.serialize())
        ubr_poll_status = UBXReader(stream, protfilter=UBX_PROTOCOL)
        raw_data, parsed_data = ubr_poll_status.read()
        if parsed_data is not None:
            print('\t', parsed_data)


def set_f9t_config(device, cfg=f9t_config):
    """Tell chip to start sending metadata packets for each cfg_key"""
    layer = SET_LAYER_RAM
    transaction = TXN_NONE
    timeout = cfg['timeout (s)']
    ubx_cfg = cfg['protocol']['ubx']

    # Tell chip to start sending metadata packets for each cfg_key. Note: Unspecified keys are initialized to 0.
    cfgData = [(cfg_key, 1) for cfg_key in ubx_cfg['cfg_keys']]  # 1 = start sending packets of type cfg_key.
    msg = UBXMessage.config_set(layer, transaction, cfgData)

    with Serial(device, BAUDRATE, timeout=timeout) as stream:
        print('Updating configuration:')
        stream.write(msg.serialize())
        ubr = UBXReader(stream, protfilter=UBX_PROTOCOL)
        for i in range(1):
            raw_data, parsed_data = ubr.read()
            if parsed_data is not None:
                print('\t', parsed_data)

def check_f9t_dataflow(device, cfg=f9t_config):
    """
    Verify all packets specified in the 'packet_ids' fields of cfg are being received.
    NOTE: for now this is hardcoded for UBX packets.
    @return: True if all packets have been received, False otherwise.
    """
    check_device_exists(device)

    timeout = cfg['timeout (s)']
    ubx_cfg = cfg['protocol']['ubx']

    # Initialize dict for recording whether we're receiving packets of each type.
    pkt_id_flags = {pkt_id: False for pkt_id in ubx_cfg['packet_ids']}

    try:
        with Serial(device, BAUDRATE, timeout=timeout) as stream:
            ubr = UBXReader(stream, protfilter=UBX_PROTOCOL)
            print('Verifying packets are being received... (If stuck at this step, re-run with the "init" option.)')

            for i in range(timeout):  # assumes config packets are send every second -> waits for timeout seconds.
                raw_data, parsed_data = ubr.read() # blocking read operation -> waits for next UBX_PROTOCOL packet.
                if parsed_data:
                    for pkt_id in pkt_id_flags.keys():
                        if parsed_data.identity == pkt_id:
                            pkt_id_flags[pkt_id] = True
                if all(pkt_id_flags.values()):
                    print('All packets are being received.\n')
                    return True
    except KeyboardInterrupt:
        print('Interrupted by KeyboardInterrupt.')
        return False
    raise Exception(f'Not all packets are being received. Check the following for details: {pkt_id_flags=}')

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

""" Initialize u-blox device. """

def check_device_exists(device):
    if device is not None:
        if not os.path.exists(device):
            raise FileNotFoundError(f'Cannot access {device}')
        return True
    return False

def init(args):
    """Configure device and verify all desired packets are being received."""
    device = args.device
    check_device_exists(device)
    poll_f9t_config(device)
    set_f9t_config(device)
    poll_f9t_config(device)
    verified = check_f9t_dataflow(device)     # Thows an Exception if not all packet types are being received.
    if not verified:
        return False
    print(f"Device initialized. Ready to collect data!")
    f9t_config["init_success"] = True
    return True

""" Test redis connection """

def test_redis_cli_connection(args):
    """
    Test Redis connection with specified connection parameters.
        1. Connect to Redis.
        2. Perform a series of pipelined write operations to a test hashset.
        3. Verify whether these writes were successful.
    Returns True if all writes were successful (valid connection), False otherwise.
    """
    host = args.host
    port = args.port
    socket_timeout = args.timeout
    # 1. Connect to Redis
    print(f"Connecting to {host}:{port}")
    r = redis.Redis(host=host, port=port, db=0, socket_timeout=socket_timeout)
    if not r.ping():
        raise FileNotFoundError(f'Cannot connect to {host}:{port}')
    # print(r)

    start_timestamp = datetime.datetime.now().isoformat()
    print('Starting redis test:'
          '\n\tUse CTRL+C to stop.'
          '\n\tRun "HGETALL TEST" to view test keys on the Redis server'
          '\n\tStart timestamp: {}\n'.format(start_timestamp))

    failures = 0
    try:
        # while True:
        #     time.sleep(interval_seconds)
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

    except KeyboardInterrupt:
        print(f'\nStopping test. {failures=}')
        pass


""" Collect packets and forward to Redis. """
def collect_data(r: redis.Redis, device: str, cfg=f9t_config):
    timeout = cfg['timeout (s)']
    ubx_cfg = cfg['protocol']['ubx']

    # Cache for saving packets and timestamping their unix arrival time using host computer clock.
    packet_cache = {}
    for pkt_id in ubx_cfg['packet_ids']:
        packet_cache[pkt_id] = {'valid': False, 'parsed_data': None}

    def all_packets_valid():
        all_valid = True
        for pkt_id in ubx_cfg['packet_ids']:
            all_valid &= packet_cache[pkt_id]['valid']
        return all_valid

    def flush_packet_cache():
        curr_time = time.time()  # datetime.datetime.now()
        chip_name = cfg['chip_name']
        chip_uid = cfg['chip_uid']
        # Pipeline Redis key updates for efficiency.
        pipe = r.pipeline()
        for pkt_id in ubx_cfg['packet_ids']:
            prot_msg = f"UBX-{pkt_id}"  # # just ubx packets for now
            rkey = get_f9t_redis_key(chip_name, chip_uid, prot_msg)
            for k, v in packet_cache[pkt_id].items():
                pipe.hset(rkey, k, v)
            pipe.hset(rkey, 'Computer_UTC', curr_time)
            packet_cache[pkt_id]['valid'] = False # invalidate packet cache entry
        pipe.execute()

    with (Serial(device, BAUDRATE, timeout=timeout) as stream):
        ubr = UBXReader(stream, protfilter=UBX_PROTOCOL)
        while True:
            # Wait for next packet (blocking read)
            raw_data, parsed_data = ubr.read()

            # Add parsed data to cache
            if parsed_data is not None:
                pkt_id_curr = parsed_data.identity
                # Add new packet to packet cache
                if pkt_id_curr in packet_cache:
                    # First flush cache to Redis if the current packet will overwrite cached data
                    if packet_cache[pkt_id_curr]['valid']:
                        flush_packet_cache()
                    packet_cache[pkt_id_curr]['valid'] = True
                    packet_cache[pkt_id_curr]['parsed_data'] = parsed_data.to_dict()

            # Flush cache to Redis if all u-blox hk were received
            if all_packets_valid():
                flush_packet_cache()

def start_collect(args):
    device = args.device
    valid_dataflow = check_f9t_dataflow(device)     # Will throw Exception if not all packet types are being received.
    if not valid_dataflow:
        return False
    # Connect to Redis database
    r = redis.Redis(
        host="localhost", # TODO
        port=6379 # TODO
    )

    # Start data collection
    start_timestamp = datetime.datetime.now().isoformat()
    # experiment_dir = get_experiment_dir(start_timestamp, device)
    # os.makedirs(experiment_dir, exist_ok=False)
    print('Starting data collection. To stop collection, use CTRL+C.'
          '\nStart timestamp: {}'.format(start_timestamp))
    try:
        collect_data(r, device)
    except KeyboardInterrupt:
        print('Stopping data collection.')
        pass
    # finally:
        # Save data
        # for data_type in df_refs.keys():
        #     fpath = f'{experiment_dir}/data-type_{data_type}.start_{start_timestamp}'
        #     save_data(df_refs[data_type], fpath)
        # print('Data saved in {}'.format(experiment_dir))




def cli_handler():
    # create the top-level parser
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(required=True)

    # init command parser
    parser_init = subparsers.add_parser('init',
                                        description='Configures u-blox device to start sending the specified packets and verifies they are all being received.')
    parser_init.add_argument('device',
                             help='specify the device file path. e.g. /dev/ttyS3',
                             type=str)
    parser_init.set_defaults(func=init)

    # collect command parser
    parser_collect = subparsers.add_parser('collect',
                                           description='Start data collection. (To stop collection, use CTRL+C.)')
    parser_collect.add_argument('device',
                                help='specify the device path. example: /dev/ttyS3',
                                type=str)
    parser_collect.set_defaults(func=start_collect)

    # test command parser

    parser_test_redis = subparsers.add_parser('test_redis', description='Test Redis connection')
    parser_test_redis.add_argument('host',
                                help='host or ip address of the computer running the Redis server',
                                type=str,
                                )
    parser_test_redis.add_argument('port',
                                help='port of the Redis server. Default: 6379',
                                nargs="?",
                                type=int,
                                default=6379)
    parser_test_redis.add_argument('timeout',
                                   help='cli connection timeout seconds. Default: 5',
                                   nargs="?",
                                   type=int,
                                   default=5)
    # parser_test_redis.add_argument("-n", "--interval_seconds",
    #                                # action="store_true",
    #                                help="number of seconds between test write operations. Default: 1 second",
    #                                type=int,
    #                                default=1
    #                                )
    parser_test_redis.set_defaults(func=test_redis_cli_connection)

    # Dispatch command action
    args = parser.parse_args()
    args.func(args)

if __name__ == '__main__':
    cli_handler()
