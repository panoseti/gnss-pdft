#!/usr/bin/env python3

"""
The Python implementation of a gRPC UbloxControl server.
The server requires the following to correctly:
    1. A valid network connection to the Redis database on the headnode with
    R/W user permissions to the Redis UBLOX hashset.
    2. A valid /dev file for a connected ZED-F9T u-blox chip.
    3. The installation of all Python packages specified in requirements.txt.
"""
import logging
import random
from concurrent import futures
import threading
from threading import Event, Thread
from queue import Queue
from serial import Serial
import time
import re

import grpc

# gRPC reflection service: allows clients to discover available RPCs
from grpc_reflection.v1alpha import reflection

# standard gRPC protobuf types + utility functions
from google.protobuf.struct_pb2 import Struct
from google.protobuf.json_format import MessageToDict, ParseDict
from google.protobuf import timestamp_pb2

from pyubx2 import POLL, UBX_PAYLOADS_POLL, UBX_PROTOCOL, UBXMessage, UBXReader

# protoc-generated marshalling / demarshalling code
import ublox_control_pb2
import ublox_control_pb2_grpc

# alias messages to improve readability
from ublox_control_resources import *
from init_f9t_tests import *  #run_all_tests, is_os_posix, check_client_f9t_cfg_keys


def f9t_io_data(
    device: str,
    timeout: float,
    baudrate: int,
    read_queues: List[Queue],
    read_queue_freemap: List[bool],
    send_queue: Queue,
    stop: Event,
    logger: logging.Logger
):
    """
    THREADED
    Read and parse inbound UBX data and place
    raw and parsed data on queue.

    Send any queued outbound messages to receiver.
    :license: BSD 3-Clause
    """
    with Serial(device, baudrate, timeout=timeout) as stream:
        ubr = UBXReader(stream, protfilter=UBX_PROTOCOL)
        logger.info("Established serial connection to F9t chip")
        while not stop.is_set():
            try:
                (raw_data, parsed_data) = ubr.read()
                if parsed_data:
                    for read_queue, is_allocated in zip(read_queues, read_queue_freemap):
                        if is_allocated:  # only populate read_queues that are actively being used
                            read_queue.put((raw_data, parsed_data))

                # refine this if outbound message rates exceed inbound
                while not send_queue.empty():
                    data = send_queue.get(False)
                    if data is not None:
                        ubr.datastream.write(data.serialize())
                    send_queue.task_done()

            except Exception as err:
                print(f"\n\nSomething went wrong - {err}\n\n")
                logger.error(f"Error: {err}")
                continue
    logger.info("f9t_io thread exited")
    # print("f9t_io_data exited.")


def f9t_io_data_DEBUG(
        device: str,
        timeout: float,
        baudrate: int,
        read_queues: List[Queue],
        read_queue_freemap: List[bool],
        send_queue: Queue,
        stop: Event,
        logger: logging.Logger
):
    """
    THREADED
    Read and parse inbound UBX data and place
    raw and parsed data on queue.

    Send any queued outbound messages to receiver.
    :license: BSD 3-Clause
    """
    logger.info("Established serial connection to F9t chip")
    while not stop.is_set():
        try:
            time.sleep(1)
            parsed_data = {
                "identity": "f9t_io DEBUG",
                "test_field": random.choice(["fox", "socks", "box", "knox"]),
                "id": random.randint(-100000, 100000)
            }
            raw_data = random.randint(-100, 100)
            logger.debug(f"[read] {raw_data=}, {parsed_data=}")
            # (raw_data, parsed_data) = random.randint(-100, 100), parsed_data
            if parsed_data:
                for read_queue, is_allocated in zip(read_queues, read_queue_freemap):
                    if is_allocated:  # only populate read_queues that are actively being used
                        read_queue.put((raw_data, parsed_data))

            # refine this if outbound message rates exceed inbound
            while not send_queue.empty():
                data = send_queue.get(False)
                if data is not None:
                    # ubr.datastream.write(data.serialize())
                    logger.debug(f"[write] {data=}")
                send_queue.task_done()

        except Exception as err:
            print(f"\n\nSomething went wrong - {err}\n\n")
            logger.error(f"Error: {err}")
            continue
    logger.info("f9t_io thread exited")
    # print("f9t_io_data exited.")

def process_data(queue: Queue, stop: Event):
    """
    THREADED
    Get UBX data from queue and display.
    :author: semuadmin
    :copyright: SEMU Consulting © 2021
    :license: BSD 3-Clause
    """

    while not stop.is_set():
        if queue.empty() is False:
            (_, parsed) = queue.get()
            print(parsed)
            queue.task_done()


"""gRPC server implementing UbloxControl RPCs"""

class UbloxControlServicer(ublox_control_pb2_grpc.UbloxControlServicer):
    """Provides methods that implement functionality of an u-blox control server."""

    def __init__(self, server_cfg):
        # verify the server is running on a POSIX-compliant system
        test_result, msg = is_os_posix()
        assert test_result, msg

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

        self.__server_cfg = server_cfg
        # Load F9t configuration
        with open(cfg_dir/self.__server_cfg["f9t_cfg_file"], "r") as f:
            self.__f9t_cfg = json.load(f)
            # check if headnode needs to first configure F9t with an InitF9t RPC
            if not self.__server_cfg["require_headnode_init"] and self.__f9t_cfg["is_valid"]:
                self.__server_cfg['f9t_init_valid'] = True
                # TODO: setup ubx connection threads here
            else:
                self.__server_cfg['f9t_init_valid'] = False

        ## State for single producer, multiple consumer F9t access
        # A single IO thread manages the dataflow between multiple concurrent RPC threads and the F9t:
        #   [single RPC writer -> F9t IO thread] write POLL and SET requests to the F9t
        #   [F9t IO thread -> many RPC readers] read (GET) packets from the F9t and write to max_workers read_queues

        # Create an array of read_queues and freemap locks to support up to max_worker concurrent reader RPCs
        self.__read_queues = []  # Duplicate queues to implement single producer, multiple independent consumer model
        self.__read_queues_freemap = []  # True iff corresponding queue is allocated to a reader
        for _ in range(server_cfg['max_workers']):
            self.__read_queues.append(Queue(maxsize=server_cfg['max_read_queue_size']))
            self.__read_queues_freemap.append(False)
        self.__send_queue = Queue()  # Used by InitF9t and PollMessage to send SET and POLL requests to the F9t chip
        self.__stop_io = Event()  # Signals F9t IO thread to release the serial connection to the F9t
        self.__f9t_io_thread = None

        # Create the server's logger
        self.logger = make_rich_logger(__name__, level=logging.INFO)

    def __del__(self):
        # assert the stop event and wait for the f9t_io thread to exit
        self.__stop_io.set()
        if self.__f9t_io_thread is not None:
            self.__f9t_io_thread.join()
        self.logger.info("Successfully freed resources")

    @contextmanager
    def __f9t_lock_writer(self):
        try:
            with self.__f9t_lock:
                # BEGIN check-in critical section
                # Wait until no active readers or active writers
                self.logger.debug(f"(writer) check-in (start):\t{self.__f9t_rw_lock_state=}")
                self.__f9t_rw_lock_state['ww'] += 1
                while (self.__f9t_rw_lock_state['aw'] + self.__f9t_rw_lock_state['ar']) > 0:
                    self.__write_ok.wait(timeout=10)
                self.__f9t_rw_lock_state['ww'] -= 1
                self.__f9t_rw_lock_state['aw'] += 1
                self.logger.debug(f"(writer) check-in (end):\t\t{self.__f9t_rw_lock_state=}")
                # END check-in critical section
            yield None
        finally:
            with self.__f9t_lock:
                # BEGIN check-out critical section
                self.logger.debug(f"(writer) check-out (start):\t{self.__f9t_rw_lock_state=}")
                self.__f9t_rw_lock_state['aw'] = max(0, self.__f9t_rw_lock_state['aw'] - 1)  # no longer active
                if self.__f9t_rw_lock_state['ww'] > 0:  # Give lock priority to waiting writers
                    self.__write_ok.notify()
                elif self.__f9t_rw_lock_state['wr'] > 0:
                    self.__read_ok.notify_all()
                self.logger.debug(f"(writer) check-out (end):\t{self.__f9t_rw_lock_state=}")
                # END check-out critical section

    @contextmanager
    def __f9t_lock_reader(self):
        read_fmap_idx = -1  # remember which read_queue freemap entry corresponds to this thread
        try:
            with self.__f9t_lock:
                # BEGIN check-in critical section
                # Wait until no active writers
                self.__f9t_rw_lock_state['wr'] += 1
                self.logger.info(f"(reader) check-in (start):\t{self.__f9t_rw_lock_state=}")
                while (self.__f9t_rw_lock_state['aw'] + self.__f9t_rw_lock_state['ww']) > 0:  # safe to read?
                    self.__read_ok.wait()
                self.__f9t_rw_lock_state['wr'] -= 1
                self.__f9t_rw_lock_state['ar'] += 1

                # allocate a read queue for this thread
                for idx, is_allocated in enumerate(self.__read_queues_freemap):
                    if not is_allocated:
                        read_fmap_idx = idx
                        self.__read_queues_freemap[idx] = True
                        break
                self.logger.info(f"{self.__read_queues_freemap=}")
                if read_fmap_idx == -1:
                    self.logger.critical("read_queue_freemap allocation failed! [SHOULD NEVER HAPPEN]")
                self.logger.info(f"(reader) check-in (end):\t\t{self.__f9t_rw_lock_state=}, fmap_idx={read_fmap_idx}")
                # END check-in critical section
            yield read_fmap_idx
        finally:
            with self.__f9t_lock:
                # BEGIN check-out critical section
                self.logger.debug(f"(reader) check-out (start):\t{self.__f9t_rw_lock_state=}")
                self.__f9t_rw_lock_state['ar'] = max(0, self.__f9t_rw_lock_state['ar'] - 1)  # no longer active
                self.__read_queues_freemap[read_fmap_idx] = False  # release the read queue
                # Wake up waiting reader / writers (prioritize waiting writers).
                if self.__f9t_rw_lock_state['ar'] == 0 and self.__f9t_rw_lock_state['ww'] > 0:
                    self.__write_ok.notify()
                elif self.__f9t_rw_lock_state['wr'] > 0:
                    self.__read_ok.notify_all()
                self.logger.debug(f"(reader) check-out (end):\t{self.__f9t_rw_lock_state=}")
                # END check-out critical section

    def InitF9t(self, request, context):
        """Configure a connected F9t chip. [writer]"""
        f9t_cfg_keys_to_copy = ['device', 'chip_name', 'timeout', 'cfg_key_settings', 'comments']
        test_results = []

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
                # is_device_valid,
            ],
            args_list=[
                [],
                [f9t_cfg_keys_to_copy, client_f9t_cfg.keys()],
                # [client_f9t_cfg['device']]
            ]
        )
        commit_changes = cfg_all_pass
        test_results.extend(cfg_test_results)

        # TODO: Do F9t initialization
        #   Set the configuration according to client_f9t_cfg['set_cfg_keys'].
        #   NOTE: unspecified keys are returned to default values.
        if commit_changes:

            # Only enter critical section as a writer if the client's F9t configuration is valid
            with self.__f9t_lock_writer():
                time.sleep(0.5)  # DEBUG: add delay to expose race conditions
                # BEGIN critical section for F9tInit [write] access
                # Terminate any previous F9t_io thread
                if self.__f9t_io_thread is not None:
                    self.__stop_io.set()
                    self.__f9t_io_thread.join()
                # Create new f9t_io_thread using the client's configuration
                self.__stop_io.clear()
                self.__f9t_io_thread = Thread(
                    # target=f9t_io_data,
                    target=f9t_io_data_DEBUG,
                    args=(
                        client_f9t_cfg['device'],
                        client_f9t_cfg['timeout'],
                        F9T_BAUDRATE,
                        self.__read_queues,
                        self.__read_queues_freemap,
                        self.__send_queue,
                        self.__stop_io,
                        self.logger
                    ),
                    daemon=False,
                )
                self.__f9t_io_thread.start()

                # Run tests with the F9t_io thread to verify f9t initialization succeeded
                init_all_pass, init_test_results = run_all_tests(
                    test_fn_list=[
                        # check_f9t_dataflow # TODO: implement this
                        poll_nav_messages,
                    ],
                    args_list=[
                        # [client_f9t_cfg],
                        [self.__send_queue]
                    ]
                )
                commit_changes = init_all_pass
                test_results.extend(init_test_results)
                self.logger.debug("InitF9t: success")
                # Commit changes to self._f9t_cfg only if all tests pass
                if commit_changes:
                    self.__server_cfg['f9t_init_valid'] = True
                    self.__f9t_cfg['is_valid'] = True
                    for key in f9t_cfg_keys_to_copy:
                        self.__f9t_cfg[key] = client_f9t_cfg[key]
                    message = "InitF9t transaction successful"
                    init_status = ublox_control_pb2.InitSummary.InitStatus.SUCCESS
                else:
                    # Cancel transaction if any tests fail
                    # TODO: rollback F9t configuration to previous config specified by self._f9t_cfg
                    ...
                # END critical section for F9t [write] access
        if not commit_changes:
            message = "InitF9t transaction cancelled. See the test_cases field for information about failing tests"
            init_status = ublox_control_pb2.InitSummary.InitStatus.FAILURE

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
        # unpack the requested message pattern filters
        patterns = request.patterns
        regex_list = [re.compile(pattern) for pattern in patterns]
        # TODO: check if the patterns are valid
        with self.__f9t_lock_reader() as rid:  # rid = allocated reader id
            # Clear the read_queue of old data
            #time.sleep(0.)
            rq = self.__read_queues[rid]
            while not rq.empty():
                rq.get()
            # BEGIN critical section for F9t [read] access
            if self.__server_cfg['f9t_init_valid']:
                # self.logger.info("Streaming messages")
                while context.is_active():
                    self.logger.debug("waiting for input")
                    raw_data, parsed_data = rq.get()
                    self.logger.debug(f"got {parsed_data=}")
                    # Generate next response
                    # time.sleep(random.uniform(0.1, 0.5))  # simulate waiting for next u-blox packet
                    # TODO: replace these hard-coded values with packets received from the connected u-blox chip
                    name = parsed_data['identity']
                    #name = parsed_data.identity
                    # TODO: find or create handlers to unpack each packet type into a dictionary of KV pairs
                    if name == 'TIM-TP':
                        ...
                    parsed_data_dict = {
                        'test_field': parsed_data['test_field'],
                        'uid': parsed_data['id'],
                    }
                    timestamp = timestamp_pb2.Timestamp()
                    timestamp.GetCurrentTime()

                    packet_data = ublox_control_pb2.PacketData(
                        name=name,
                        parsed_data=ParseDict(parsed_data_dict, Struct()),
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

def serve(server_cfg):
    """Create the gRPC server threadpool and start providing the UbloxControl service."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=server_cfg['max_workers']))
    ublox_control_pb2_grpc.add_UbloxControlServicer_to_server(
        UbloxControlServicer(server_cfg), server
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
        print("'^C' received, shutting down the server in 5 seconds.")
        server.stop(grace=5).wait()



if __name__ == "__main__":
    # Load server configuration
    server_cfg_file = "ublox_control_server_config.json"
    with open(cfg_dir / server_cfg_file, "r") as f:
        server_cfg = json.load(f)
    serve(server_cfg)
