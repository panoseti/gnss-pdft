"""
Test cases for the InitF9t RPC.

These functions verify a specified F9t is properly configured.
Each function must have type Callable[..., [bool, str]] as the examples below:
    def test_0():
        msg = "your debug or error message for test_0 here"
        return True, msg

    def test_1():
        msg = "your debug or error message test_1 here"
        return False, msg
"""
import os
from ublox_control_resources import *
from ublox_control_pb2 import TestCase, InitSummary

def run_all_tests(
        test_fn_list: List[Callable[..., Tuple[bool, str]]],
        args_list: List[List[...]],
) -> Tuple[bool, type(TestCase.TestResult)]:
    """
    Runs each test function in [test_functions].
    To ensure correct behavior new test functions have type Callable[..., Tuple[bool, str]] to ensure correct behavior.
    Returns enum init_status and a list of test_results.
    """
    assert len(test_fn_list) == len(args_list), "test_fn_list must have the same length as args_list"
    def get_test_name(test_fn):
        return f"%s.%s" % (test_fn.__module__, test_fn.__name__)

    all_pass = True
    test_results = []
    for test_fn, args in zip(test_fn_list, args_list):
        test_result, message = test_fn(*args)
        all_pass &= test_result
        test_result = ublox_control_pb2.TestCase(
            name=get_test_name(test_fn),
            result=TestCase.TestResult.PASS if test_result else TestCase.TestResult.FAIL,
            message=message
        )
        test_results.append(test_result)
    return all_pass, test_results

def check_client_f9t_cfg_keys(required_f9t_cfg_keys: List[str], client_f9t_keys: List[str]) -> Tuple[bool, str]:
    """Verify the client's f9t config keys contain all required keys."""
    required_key_set = set(required_f9t_cfg_keys)
    client_key_set = set(client_f9t_keys)
    if set(required_key_set).issubset(client_key_set):
        return True, "all required f9t_cfg keys are present"
    else:
        required_key_diff = required_key_set.difference(client_key_set)
        return False, f"the given f9t_cfg is missing the following required keys: {required_key_diff}"


def is_device_valid(device: str) -> Tuple[bool, str]:
    if not device:
        return False, f"{device=} is empty"
    elif os.path.exists(device):
        return True, f"{device} is valid"
    else:
        return False, f"{device} does not exist"


def is_os_posix():
    """Verify the server is running in a POSIX environment."""
    if os.name == 'posix':
        return True, f"detected a POSIX-compliant system"
    else:
        return False, f"{os.name} is not supported yet"

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

    # Initialize dict for recording whether we're receiving packets of each type.
    pkt_id_flags = {pkt_id: False for pkt_id in ubx_cfg['packet_ids']}

    msg = ""
    try:
        with Serial(device, F9T_BAUDRATE, timeout=timeout) as stream:
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
