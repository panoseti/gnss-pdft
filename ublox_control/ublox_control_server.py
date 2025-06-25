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
import threading
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
from ublox_control_resources import *
from init_f9t_tests import run_all_init_f9t_tests, is_os_posix

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
    "timeout (s)": 5
}


"""gRPC server wrapper for UbloxControl RPCs"""

class UbloxControlServicer(ublox_control_pb2_grpc.UbloxControlServicer):
    """Provides methods that implement functionality of an u-blox control server."""

    def __init__(self):
        # Mesa monitor for RW lock guarding access to F9t data.
        #   "Writers" = threads executing the InitF9t RPC.
        #   "Readers" = threads executing any other UbloxControl RPC that depends on F9t data
        self.shared_state = {
            "is_f9t_valid": False,
            "wr": 0, # waiting readers
            "ww": 0, # waiting writers
            "ar": 0, # active readers
            "aw": 0, # active writers
        }
        self.lock = threading.Lock()
        self.condvar = threading.Condition(self.rw_lock)

        # F9t config
        self.f9t_state = default_f9t_state.copy()
        self.packet_ids = ['NAV-TIMEUTC', 'TIM-TP']
        self.init_tests = [
            is_os_posix
        ]

    def InitF9t(self, request, context):
        """Configure a connected F9t chip. [writer]"""
        f9t_config_dict = MessageToDict(request.config)
        if self.lock.acquire(blocking=True, timeout=2):
            # BEGIN Critical section: synchronize access to functions that modify F9t state
            # Wait until no active readers or writers
            self.shared_state['ww'] += 1  # This thread is a "writer"
            while not (self.shared_state['aw'] > 0 or self.shared_state['ar'] > 0):
                self.condvar.wait()

            self.shared_state['ww'] -= 1
            self.shared_state['aw'] += 1
            self.lock.release()
            # END critical section: synchronize access to functions that modify F9t state

            # TODO: do initialization work here

            # Run tests to verify f9t initialization
            init_status, test_results = run_all_init_f9t_tests(self.init_tests)
            message = ""
            # TODO: add checkout and wake up a waiting writer or all waiting readers
            self.f9t_is_valid = (init_status == ublox_control_pb2.InitSummary.InitStatus.SUCCESS)
        else:
            init_status = ublox_control_pb2.InitSummary.InitStatus.FAILURE
            message = "Failed to acquire the lock for F9t configuration"
            test_results = []

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

            packet_data = ublox_control_pb2.PacketData(
                packet_id=packet_id,
                parsed_data=ParseDict(parsed_data, Struct()),
                timestamp=timestamp
            )

            # send packet if packet_id matches packet_id_pattern or the pattern is an empty string
            if not packet_id_pattern or re.search(packet_id_pattern, packet_id):
                yield packet_data


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
