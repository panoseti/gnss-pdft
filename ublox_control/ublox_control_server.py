#!/usr/bin/env python3
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
from init_f9t_tests import *#run_all_tests, is_os_posix, check_client_f9t_cfg_keys



"""gRPC server implementing UbloxControl RPCs"""

class UbloxControlServicer(ublox_control_pb2_grpc.UbloxControlServicer):
    """Provides methods that implement functionality of an u-blox control server."""

    def __init__(self, server_cfg_file="ublox_control_server_config.json"):
        # verify the server is running on a POSIX-compliant system
        test_result, msg = is_os_posix()
        assert test_result, msg

        self.cfg_dir = cfg_dir
        self.server_cfg_file = server_cfg_file

        # Initialize mesa monitor for synchronizing access to the F9T chip
        #   "Writers" = threads executing the InitF9t RPC.
        #   "Readers" = threads executing any other UbloxControl RPC that depends on F9t data
        self.__f9t_rw_lock_state = {
            "wr": 0,  # waiting readers
            "ww": 0,  # waiting writers
            "ar": 0,  # active readers
            "aw": 0,  # active writers
        }
        self.__f9t_lock = threading.Lock()
        self.__read_ok = threading.Condition(self.__f9t_lock)
        self.__write_ok = threading.Condition(self.__f9t_lock)

        # Load server configuration
        with open(cfg_dir/server_cfg_file, "r") as f:
            self.__server_cfg = json.load(f)

        # Load F9t configuration
        with open(cfg_dir/self.__server_cfg["f9t_cfg_file"], "r") as f:
            self.__f9t_cfg = json.load(f)
            # check if headnode needs to first configure F9t with an InitF9t RPC
            if not self.__server_cfg["require_headnode_init"] and self.__f9t_cfg["is_valid"]:
                self.__server_cfg['is_init_valid'] = True
                # TODO: setup ubx connection threads here
            else:
                self.__server_cfg['is_init_valid'] = False

        # Create the server's logger
        self.logger = make_rich_logger(__name__)

    @contextmanager
    def __f9t_lock_writer(self):
        with self.__f9t_lock:
            # BEGIN check-in critical section
            # Wait until no active readers or active writers
            self.logger.debug(f"(writer) check-in (start):\t{self.__f9t_rw_lock_state=}")
            self.__f9t_rw_lock_state['ww'] += 1
            while (self.__f9t_rw_lock_state['aw'] + self.__f9t_rw_lock_state['ar']) > 0:
                self.__write_ok.wait()
            self.__f9t_rw_lock_state['ww'] -= 1
            self.__f9t_rw_lock_state['aw'] += 1
            self.logger.debug(f"(writer) check-in (end):\t\t{self.__f9t_rw_lock_state=}")
        # END check-in critical section
        yield None
        with self.__f9t_lock:
            # BEGIN check-out critical section
            self.logger.debug(f"(writer) check-out (start):\t{self.__f9t_rw_lock_state=}")
            self.__f9t_rw_lock_state['aw'] = max(0, self.__f9t_rw_lock_state['aw'] - 1)  # no longer active
            if self.__f9t_rw_lock_state['ww'] > 0:  # Give lock priority to waiting writers
                self.__write_ok.notify()
            elif self.__f9t_rw_lock_state['wr'] > 0:
                self.__read_ok.notify_all()
            self.logger.debug(f"(writer) check-out (end):\t{self.__f9t_rw_lock_state=}")

    @contextmanager
    def __f9t_lock_reader(self):
        with self.__f9t_lock:
            # BEGIN check-in critical section
            # Wait until no active writers
            self.__f9t_rw_lock_state['wr'] += 1
            self.logger.debug(f"(reader) check-in (start):\t{self.__f9t_rw_lock_state=}")
            while (self.__f9t_rw_lock_state['aw'] + self.__f9t_rw_lock_state['ww']) > 0:  # safe to read?
                self.__read_ok.wait()
            self.__f9t_rw_lock_state['wr'] -= 1
            self.__f9t_rw_lock_state['ar'] += 1
            self.logger.debug(f"(reader) check-in (end):\t\t{self.__f9t_rw_lock_state=}")
            # END check-in critical section
        yield None
        with self.__f9t_lock:
            # BEGIN check-out critical section
            self.logger.debug(f"(reader) check-out (start):\t{self.__f9t_rw_lock_state=}")
            self.__f9t_rw_lock_state['ar'] = max(0, self.__f9t_rw_lock_state['ar'] - 1)  # no longer active
            # Give lock priority to waiting writers
            if self.__f9t_rw_lock_state['ar'] == 0 and self.__f9t_rw_lock_state['ww'] > 0:
                self.__write_ok.notify()
            elif self.__f9t_rw_lock_state['wr'] > 0:
                self.__read_ok.notify_all()
            self.logger.debug(f"(reader) check-out (end):\t{self.__f9t_rw_lock_state=}")
            # END check-out critical section

    def InitF9t(self, request, context):
        """Configure a connected F9t chip. [writer]"""
        f9t_cfg_keys_to_copy = ['device', 'chip_name', 'timeout', 'cfg_key_settings', 'comments']
        with self.__f9t_lock_writer():
            # BEGIN critical section for F9t [write] access
            commit_changes = True
            test_results = []
            time.sleep(1)  # DEBUG: add delay to expose race conditions

            client_f9t_cfg = MessageToDict(request.f9t_cfg)
            # TODO: Validate f9t_cfg here:
            #  0. System is POSIX
            #  1. all keys in f9t_cfg_keys_to_copy are present in client_f9t_cfg
            #  2. device file is valid
            #  3. device file points to an f9t chip
            #  4. we can send SET and POLL requests to the chip and read responses with GET
            #  5. all keys under "set_cfg_keys" are valid and supported by pyubx2
            cfg_all_pass, cfg_test_results = run_all_tests(
                test_fn_list=[
                    is_os_posix,
                    check_client_f9t_cfg_keys,
                    is_device_valid,
                ],
                args_list=[
                    [],
                    [f9t_cfg_keys_to_copy, client_f9t_cfg.keys()],
                    [client_f9t_cfg['device']]
                ]
            )
            commit_changes = cfg_all_pass
            test_results.extend(cfg_test_results)

            # TODO: Do F9t initialization
            #   Set the configuration according to client_f9t_cfg['set_cfg_keys'].
            #   NOTE: unspecified keys are returned to default values.
            if commit_changes:

                # Run tests to verify f9t initialization succeeded
                init_all_pass, init_test_results = run_all_tests(
                    test_fn_list=[
                        check_f9t_dataflow
                    ],
                    args_list=[
                        [client_f9t_cfg]
                    ]
                )
                # Cancel transaction if any tests fail
                commit_changes = init_all_pass
                test_results.extend(init_test_results)
            # Commit changes to self._f9t_cfg only if all tests pass
            if commit_changes:
                self.__server_cfg['is_init_valid'] = True
                self.__f9t_cfg['is_valid'] = True
                for key in f9t_cfg_keys_to_copy:
                    self.__f9t_cfg[key] = client_f9t_cfg[key]
                message = "InitF9t transaction successful"
                init_status = ublox_control_pb2.InitSummary.InitStatus.SUCCESS
            else:
                # TODO: rollback F9t configuration to previous config specified by self._f9t_cfg
                message = "InitF9t transaction cancelled. See the test_cases field for information about failing tests"
                init_status = ublox_control_pb2.InitSummary.InitStatus.FAILURE
            # END critical section for F9t [write] access

        # Send summary of initialization process to client
        init_summary = ublox_control_pb2.InitSummary(
            init_status=init_status,
            message=message,
            f9t_cfg=ParseDict(self.__f9t_cfg, Struct()),
            test_results=test_results,
        )
        return init_summary

    def CapturePackets(self, request, context):
        """Forward u-blox packets to the client. [reader]"""
        patterns = request.patterns
        regex_list = [re.compile(pattern) for pattern in patterns]
        # TODO: check if the patterns are valid
        with self.__f9t_lock_reader():
            time.sleep(1)
            # BEGIN critical section for F9t [read] access
            if self.__server_cfg['is_init_valid']:
                self.logger.info("Streaming messages matching ")
                while context.is_active():
                    # Generate next response
                    time.sleep(random.uniform(0.1, 0.5)) # simulate waiting for next u-blox packet
                    # TODO: replace these hard-coded values with packets received from the connected u-blox chip
                    name = "TEST"
                    parsed_data = {
                        'qErr': random.randint(-4, 4),
                        'field2': 'hello',
                        'field3': random.random(),
                        'field4': None
                    }
                    timestamp = timestamp_pb2.Timestamp()
                    timestamp.GetCurrentTime()

                    packet_data = ublox_control_pb2.PacketData(
                        name=name,
                        parsed_data=ParseDict(parsed_data, Struct()),
                        timestamp=timestamp
                    )

                    # send packet if its name matches any of the given patterns or if no patterns were given.
                    if len(regex_list) == 0 or any(regex.search(name) for regex in regex_list):
                        yield packet_data
            else:
                timestamp = timestamp_pb2.Timestamp()
                timestamp.GetCurrentTime()
                packet_data = ublox_control_pb2.PacketData(
                    name="INVALID_F9T_INITIALIZATION: Run InitF9t to configure the F9t with a valid config",
                    # parsed_data=ParseDict(parsed_data, Struct()),
                    timestamp=timestamp
                )
                yield packet_data
            # End critical section for F9t [read] access

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
