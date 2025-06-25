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

# etc = {
#     "protocol": {
#         "ubx": {
#             "device": None,
#             "cfg_keys": [],  # default cfg keys to poll
#             "packet_ids": [],  # packet_ids to capture: should be in 1-1 corresp with the cfg_keys.
#         }
#     },
# }
# Default chip state upon server startup.
default_f9t_state = {
    "chip_name": "ZED-F9T",
    "chip_uid": None,
    "timeout (s)": 5,
    "baudrate": 38_400,
    "is_valid": False,
}


"""gRPC server wrapper for UbloxControl RPCs"""

class UbloxControlServicer(ublox_control_pb2_grpc.UbloxControlServicer):
    """Provides methods that implement functionality of an u-blox control server."""

    def __init__(self):
        # verify the server is running on a POSIX-compliant system
        test_result, msg = is_os_posix()
        assert test_result == True, msg

        # Mesa monitor for synchronizing access to functions that modify F9t state
        #   "Writers" = threads executing the InitF9t RPC.
        #   "Readers" = threads executing any other UbloxControl RPC that depends on F9t data
        self._rw_lock_state = {
            "wr": 0,  # waiting readers
            "ww": 0,  # waiting writers
            "ar": 0,  # active readers
            "aw": 0,  # active writers
        }
        self.lock = threading.Lock()
        self.reader_cond = threading.Condition(self.lock)
        self.writer_cond = threading.Condition(self.lock)

        # F9t state
        self._f9t_state = default_f9t_state.copy()
        self.packet_ids = ['NAV-TIMEUTC', 'TIM-TP']

        # test suite that is run after every F9t configuration operation
        self.init_tests = [ is_os_posix ]

        self._logger = logging.getLogger(__name__)

    def InitF9t(self, request, context):
        """Configure a connected F9t chip. [writer]"""
        tid = threading.get_ident()
        f9t_config_dict = MessageToDict(request.config)
        try:
            with self.lock:
                # BEGIN check-in critical section
                # Wait until no active readers or active writers
                self._rw_lock_state['ww'] += 1
                self._logger.debug(f"check-in (start): {self._rw_lock_state=}")
                while (self._rw_lock_state['aw'] > 0) or (self._rw_lock_state['ar'] > 0):
                    # This RPC is a "writer"
                    self.writer_cond.wait()
                self._rw_lock_state['ww'] -= 1
                self._rw_lock_state['aw'] += 1
                self._logger.debug(f"check-in (end): {self._rw_lock_state=}")
                # END check-in critical section
            time.sleep(1)  # add delay to expose race conditions

            # TODO: do F9t initialization work here

            # Run tests to verify f9t initialization
            init_status, test_results = run_all_init_f9t_tests(self.init_tests)
            message = "Completed initialization and ran tests"

            # TODO: add checkout and wake up a waiting writer or all waiting readers
            self._f9t_state['is_valid'] = (init_status == ublox_control_pb2.InitSummary.InitStatus.SUCCESS)
        finally:
            with self.lock:
                # BEGIN check-out critical section
                self._logger.debug(f"check-out (start): {self._rw_lock_state=}")
                if self._rw_lock_state['ww'] > 0: # Give lock priority to waiting writers
                    self.writer_cond.notify()
                elif self._rw_lock_state['wr'] > 0:
                    self.reader_cond.notify_all()
                self._rw_lock_state['aw'] = max(0, self._rw_lock_state['aw'] - 1)
                self._logger.debug(f"check-out (end): {self._rw_lock_state=}")
                # END check-out critical section

        # Send summary of initialization process to client
        init_summary = ublox_control_pb2.InitSummary(
            init_status=init_status,
            message=message,
            f9t_state=ParseDict(self._f9t_state, Struct()),
            test_results=test_results,
        )
        return init_summary

    def CapturePackets(self, request, context):
        """Forward u-blox packets to the client. [reader]"""
        tid = threading.get_ident()
        try:
            with self.lock:
                # BEGIN check-in critical section
                # Wait until no active writers
                self._rw_lock_state['wr'] += 1
                self._logger.debug(f"check-in (start): {self._rw_lock_state=}")
                while self._rw_lock_state['aw'] > 0 or self._rw_lock_state['ww'] > 0:
                    # This RPC is a "reader"
                    self.reader_cond.wait()
                self._rw_lock_state['wr'] -= 1
                self._rw_lock_state['ar'] += 1
                self._logger.debug(f"check-in (end): {self._rw_lock_state=}")
                # END check-in critical section

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
        finally:
            with self.lock:
                # BEGIN check-out critical section
                self._logger.debug(f"check-out (start): {self._rw_lock_state=}")
                if self._rw_lock_state['ww'] > 0: # Give lock priority to waiting writers
                    self.writer_cond.notify()
                elif self._rw_lock_state['wr'] > 0:
                    self.reader_cond.notify_all()
                self._rw_lock_state['ar'] = max(0, self._rw_lock_state['ar'] - 1)
                self._logger.debug(f"check-out (end): {self._rw_lock_state=}")
                # END check-out critical section


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
    print(f"The gRPC services {SERVICE_NAMES} are running.\nEnter CTRL+C to stop them.")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("'^C' received, shutting down the server.")



if __name__ == "__main__":
    """ Configure logger """
    # Define the log format string
    LOG_FORMAT = (
        "[tid=%(thread)d] [%(funcName)s()] %(message)s "
        # "[%(filename)s:%(lineno)d %(funcName)s()]"
    )

    # Configure logging with RichHandler
    logging.basicConfig(
        level=logging.DEBUG,
        format=LOG_FORMAT,
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[RichHandler(rich_tracebacks=True)]
    )

    serve()
