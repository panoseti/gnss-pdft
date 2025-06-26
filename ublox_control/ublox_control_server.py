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
import json

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



"""gRPC server implementing UbloxControl RPCs"""

class UbloxControlServicer(ublox_control_pb2_grpc.UbloxControlServicer):
    """Provides methods that implement functionality of an u-blox control server."""
    server_cfg_file = "ublox_control_server_config.json"

    def __init__(self):
        # verify the server is running on a POSIX-compliant system
        test_result, msg = is_os_posix()
        assert test_result, msg

        # Initialize mesa monitor for synchronizing access to the F9T chip
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

        # Load server configuration
        with open(server_cfg_file, "r") as f:
            self._server_cfg = json.load(f)

        # Load F9t configuration
        with open(self._server_cfg["f9t_cfg_file"], "r") as f:
            self._f9t_cfg = json.load(f)
            # check if headnode needs to first configure F9t with an InitF9t RPC
            if not self._server_cfg["require_headnode_init"] and self._f9t_cfg["cfg_is_valid"]:
                self._f9t_cfg['is_init_valid'] = True
                # TODO: setup ubx connection threads here
            else:
                self._f9t_cfg['is_init_valid'] = False

        # Define test suite for the InitF9t RPC
        self.init_f9t_tests = [
            is_os_posix,
        ]

        # Configure the server's logger
        LOG_FORMAT = (
            "[tid=%(thread)d] [%(funcName)s()] %(message)s "
            # "[%(filename)s:%(lineno)d %(funcName)s()]"
        )

        logging.basicConfig(
            level=logging.DEBUG,
            format=LOG_FORMAT,
            datefmt="%Y-%m-%d %H:%M:%S",
            handlers=[RichHandler(rich_tracebacks=True)]
        )
        self.logger = logging.getLogger(__name__)

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
            self._f9t_cfg['is_init_valid'] = (init_status == ublox_control_pb2.InitSummary.InitStatus.SUCCESS)
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


    serve()
