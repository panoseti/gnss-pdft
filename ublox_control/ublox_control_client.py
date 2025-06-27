#!/usr/bin/env python3

"""
The Python implementation of a gRPC UbloxControl client.

Run this on the headnode to configure the u-blox GNSS receivers in remote domes.
"""
import logging
import queue
import random

import pyubx2
import redis
from rich import print
from rich.pretty import pprint
import re
import datetime

## gRPC imports
import grpc

# gRPC reflection service: allows clients to discover available RPCs
from google.protobuf.descriptor_pool import DescriptorPool
from grpc_reflection.v1alpha.proto_reflection_descriptor_database import (
    ProtoReflectionDescriptorDatabase,
)
# Standard gRPC protobuf types
from google.protobuf.struct_pb2 import Struct
from google.protobuf.json_format import MessageToDict, ParseDict
from google.protobuf import timestamp_pb2

# protoc-generated marshalling / demarshalling code
import ublox_control_pb2
import ublox_control_pb2_grpc

# alias messages to improve readability
from ublox_control_pb2 import CaptureCommand, InitSummary, F9tConfig


## our code
from ublox_control_resources import *


def get_services(channel):
    """Prints all available RPCs for the UbloxControl service represented by [channel]."""
    def format_rpc_service(method):
        name = method.name
        input_type = method.input_type.name
        output_type = method.output_type.name
        client_stream = "stream " if method.client_streaming else ""
        server_stream = "stream " if method.server_streaming else ""
        return f"rpc {name}({client_stream}{input_type}) returns ({server_stream}{output_type})"
    reflection_db = ProtoReflectionDescriptorDatabase(channel)
    services = reflection_db.get_services()
    print(f"found services: {services}")

    desc_pool = DescriptorPool(reflection_db)
    service_desc = desc_pool.FindServiceByName("ubloxcontrol.UbloxControl")
    print(f"found UbloxControl service with name: {service_desc.full_name}")
    for method in service_desc.methods:
        print(f"\tfound: {format_rpc_service(method)}")


def init_f9t(stub, f9t_cfg) -> dict:
    """Initializes an F9T device according to the specification in f9t_config."""
    f9t_config = F9tConfig(
        f9t_cfg=ParseDict(f9t_cfg, Struct())
    )
    init_summary = stub.InitF9t(f9t_config)
    # unpack init_summary
    init_status = InitSummary.InitStatus.Name(init_summary.init_status)
    curr_f9t_cfg = MessageToDict(init_summary.f9t_cfg, preserving_proto_field_name=True)
    print(f'init_summary.status=', init_status)
    print(f'{init_summary.message=}')
    print("init_summary.f9t_cfg=", curr_f9t_cfg, end='')
    pprint(curr_f9t_cfg, expand_all=True)
    print(init_summary.test_results)
    # for i, test_result in enumerate(init_summary.test_results):
    #     print(f'TEST {i}:')
    #     print("\t" + str(test_result).replace("\n", "\n\t"))
    return curr_f9t_cfg


def capture_packets(stub, patterns, f9t_cfg):
    # valid_capture_command_aliases = ['start', 'stop']

    def make_capture_command(pats):
        if pats is None:
            return CaptureCommand()
        for pat in pats:
            re.compile(pat)  # verify each regex pattern compiles
        return CaptureCommand(patterns=pats)

    def format_packet_data(name, parsed_data, timestamp: datetime.datetime):
        timestamp = timestamp.isoformat()
        return f"{name=} : {timestamp=} : {parsed_data}"

    def write_packet_data_to_redis(r, chip_name, chip_uid, packet_id, parsed_data, timestamp: datetime.datetime):
        # curr_time = datetime.datetime.now(tz=datetime.timezone.utc)
        # TODO: write methods to unpack parsed data
        rkey = get_f9t_redis_key(chip_name, chip_uid, packet_id)
        for k, v in parsed_data.items():
            r.hset(rkey, k, v)
        timestamp_float = timestamp.timestamp()  # timestamp when packet was received by the daq node
        r.hset(rkey, 'Computer_UTC', timestamp_float)

    # start packet stream
    packet_data_stream = stub.CapturePackets(
        make_capture_command(patterns)
    )

    redis_host, redis_port = "localhost", 6379
    chip_name = f9t_cfg['chip_name']
    chip_uid = f9t_cfg['chip_uid']
    with redis.Redis(host=redis_host, port=redis_port) as r:
        for packet_data in packet_data_stream:
            packet_id = packet_data.name
            parsed_data = MessageToDict(packet_data.parsed_data)
            timestamp = packet_data.timestamp.ToDatetime()
            print(format_packet_data(packet_id, parsed_data, timestamp))
            write_packet_data_to_redis(r, chip_name, chip_uid, packet_id, parsed_data, timestamp)



def run(host, port=50051):
    # NOTE(gRPC Python Team): .close() is possible on a channel and should be
    # used in circumstances in which the with statement does not fit the needs
    # of the code.
    connection_target = f"{host}:{port}"
    try:
        with grpc.insecure_channel(connection_target) as channel:
            stub = ublox_control_pb2_grpc.UbloxControlStub(channel)

            print("-------------- ServerReflection --------------")
            get_services(channel)

            #for i in range(1):
            print("-------------- InitF9t --------------")
            client_f9t_cfg = default_f9t_cfg
            client_f9t_cfg['chip_uid'] = 'DEADBEEFED'
            curr_f9t_cfg = client_f9t_cfg
            # curr_f9t_cfg = init_f9t(stub, client_f9t_cfg)

            print("-------------- CapturePackets --------------")
            capture_packets(stub, None, curr_f9t_cfg)
    except KeyboardInterrupt:
        logger.info(f"'^C' received, closing connection to UbloxControl server at {repr(connection_target)}")
    except grpc.RpcError as rpc_error:
        logger.error(f"{type(rpc_error)}\n{repr(rpc_error)}")


if __name__ == "__main__":
    # logging.basicConfig()
    logger = make_rich_logger(__name__)

    # Run client-side tests
    print("-------------- Client-side Tests --------------")
    all_pass, _ = run_all_tests(
        test_fn_list=[
            test_redis_connection,
        ],
        args_list=[
            ["localhost", 6379, 1, logger]
        ]
    )
    assert all_pass, "at least one client-side test failed"
    # test_redis_connection("localhost", logger=logger)
    # run(host="10.0.0.60")
    run(host="localhost")


