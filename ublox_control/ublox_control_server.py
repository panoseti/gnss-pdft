"""
The Python implementation of a gRPC UbloxControl server.
The server requires the following to correctly:
    1. A valid network connection to the Redis database on the headnode with
    R/W user permissions to the Redis UBLOX hashset.
    2. A valid /dev file for a connected ZED-F9T u-blox chip.
    3. The installation of all Python packages specified in requirements.txt.
"""
import random
from concurrent import futures
import logging
import time
import re

import grpc

# gRPC reflection service: allows clients to discover available RPCs
from grpc_reflection.v1alpha import reflection

# standard gRPC protobuf types + utility functions
from google.protobuf.struct_pb2 import Struct
from google.protobuf.json_format import MessageToDict, ParseDict
from google.protobuf import timestamp_pb2

# protoc-generated marshalling / demarshalling code
import ublox_control_pb2
import ublox_control_pb2_grpc

# alias messages to improve readability
from ublox_control_pb2 import TestCase, InitSummary
from ublox_control_resources import *

# Default chip state upon server startup.
default_f9t_state = {
    "chip_name": "ZED-F9T",
    "chip_uid": None,
    "protocol": {
        "ubx": {
            "device": None,
            "cfg_keys": [], # default cfg keys to poll
            "packet_ids": [], # packet_ids to capture: should be in 1-1 corresp with the cfg_keys.
        }
    },
    "timeout (s)": 5,
    "init_success": False,
}



def run_initialization_tests(
        test_fn_list: List[Callable[..., Tuple[bool, str]]]
) -> Tuple[type(InitSummary.InitStatus), type(TestCase.TestResult)]:
    """
    Runs each test function in [test_functions].
    To ensure correct behavior new test functions have type Callable[..., Tuple[bool, str]] to ensure correct behavior.
    Returns enum init_status and a list of test_results.
    """

    def get_test_name(test_fn):
        return f"%s.%s" % (test_fn.__module__, test_fn.__name__)

    all_pass = True
    test_results = []
    for test_fn in test_fn_list:
        result, message = test_fn()
        if result:
            result = TestCase.TestResult.PASS
        else:
            result = TestCase.TestResult.FAIL
            all_pass = False

        test_result = ublox_control_pb2.TestCase(
            name=get_test_name(test_fn),
            result=result,
            message=message
        )
        test_results.append(test_result)
    if all_pass:
        init_status = InitSummary.InitStatus.SUCCESS
    else:
        init_status = InitSummary.InitStatus.FAILURE
    return init_status, test_results


"""gRPC server wrapper for UbloxControl RPCs"""

class UbloxControlServicer(ublox_control_pb2_grpc.UbloxControlServicer):
    """Provides methods that implement functionality of an u-blox control server."""

    def __init__(self):
        self.f9t_state = default_f9t_state.copy()
        self.packet_ids = ['NAV-TIMEUTC', 'TIM-TP']
        self.init_tests = [test_0, test_1]

    def InitF9t(self, request, context):
        """Configure a connected F9t chip"""
        f9t_config_dict = MessageToDict(request.config, preserving_proto_field_name=True)
        # TODO: add real validation checks here
        init_status, test_results = run_initialization_tests(self.init_tests)

        message = ""

        init_summary = ublox_control_pb2.InitSummary(
            init_status=init_status,
            message=message,
            f9t_state=ParseDict(self.f9t_state, Struct()),
            test_results=test_results,
        )
        return init_summary

    def CapturePackets(self, request, context):
        """Forward u-blox packets to the client"""
        packet_id_pattern = request.packet_id_pattern
        while context.is_active():
            # Generate next response
            time.sleep(random.uniform(0.1, 0.5)) # simulate waiting for next u-blox packet
            # TODO: replace these hard-coded values with packets received from the connected u-blox chip
            packet_id = "TEST"
            parsed_data = {
                'qErr': random.randint(-4, 4),
                'field2': 'hello',
                'field3': random.random(),
                'field4': None
            }
            timestamp = timestamp_pb2.Timestamp()
            timestamp.GetCurrentTime()
            # send packet if packet_id matches packet_id_pattern or the pattern is an empty string
            if not packet_id_pattern or re.search(packet_id_pattern, packet_id):
                packet_data = ublox_control_pb2.PacketData(
                    packet_id=packet_id,
                    parsed_data=ParseDict(parsed_data, Struct()),
                    timestamp=timestamp
                )
                yield packet_data
            else:
                time.sleep(0.01)  # Avoid tight loops

def serve():
    """Create the gRPC server threadpool and start providing the UbloxControl service."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    ublox_control_pb2_grpc.add_UbloxControlServicer_to_server(
        UbloxControlServicer(), server
    )

    # Add RPC reflection to show available commands to users
    SERVICE_NAMES = (
        ublox_control_pb2.DESCRIPTOR.services_by_name["UbloxControl"].full_name,
        reflection.SERVICE_NAME,
    )
    reflection.enable_server_reflection(SERVICE_NAMES, server)

    # Start gRPC and configure to listen on port 50051
    server.add_insecure_port("[::]:50051")
    server.start()
    print("[gRPC server is running]\n[Enter CTRL+C to stop]")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("[^C received, shutting down the server]")



if __name__ == "__main__":
    logging.basicConfig()
    serve()
