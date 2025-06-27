"""
The Python implementation of a gRPC UbloxControl client.

Run this on the headnode to configure the u-blox GNSS receivers in remote domes.
"""
import logging
import queue
import random

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


def init_f9t(stub, f9t_config):
    """Initializes an F9T device according to the specification in f9t_config."""
    f9t_config_msg = F9tConfig(
        f9t_cfg=ParseDict(f9t_config, Struct())
    )
    init_summary = stub.InitF9t(f9t_config_msg)
    print(f'init_summary.status=', InitSummary.InitStatus.Name(init_summary.init_status))
    print(f'{init_summary.message=}')
    print("init_summary.f9t_state=", end='')
    pprint(MessageToDict(init_summary.f9t_cfg, preserving_proto_field_name=True), expand_all=True)
    for i, test_result in enumerate(init_summary.test_results):
        print(f'TEST {i}:')
        print("\t" + str(test_result).replace("\n", "\n\t"))


def capture_packets(stub, patterns=None):
    # valid_capture_command_aliases = ['start', 'stop']

    def make_capture_command(pats):
        if pats is None:
            return CaptureCommand()
        for pat in pats:
            re.compile(pat)  # verify each regex pattern compiles
        return CaptureCommand(patterns=pats)

    packet_data_stream = stub.CapturePackets(
        make_capture_command(patterns)
    )
    for i, packet_data in zip(range(random.randint(100, 100)), packet_data_stream):
        name = packet_data.name
        parsed_data = MessageToDict(packet_data.parsed_data)
        timestamp = packet_data.timestamp.ToDatetime()
        print(f"[ {name=} ] @ {timestamp=} : ", end="")
        pprint(parsed_data, expand_all=False)



def run(host, port=50051):
    # NOTE(gRPC Python Team): .close() is possible on a channel and should be
    # used in circumstances in which the with statement does not fit the needs
    # of the code.
    connection_target = f"{host}:{port}"
    with grpc.insecure_channel(connection_target) as channel:
        stub = ublox_control_pb2_grpc.UbloxControlStub(channel)

        print("-------------- ServerReflection --------------")
        get_services(channel)

        for i in range(1):
            print("-------------- InitF9t --------------")
            init_f9t(stub, default_f9t_cfg)

            print("-------------- CapturePackets --------------")
            capture_packets(stub)


if __name__ == "__main__":
    logging.basicConfig()
    # run(host="10.0.0.60")
    run(host="localhost")

