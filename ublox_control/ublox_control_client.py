"""
The Python implementation of a gRPC UbloxControl client.

Run this on the headnode to configure the u-blox GNSS receivers in remote domes.
"""
import logging
from rich import print
from rich.pretty import pprint

## gRPC imports
import grpc

# gRPC reflection service (tells which services are available)
from google.protobuf.descriptor_pool import DescriptorPool
from grpc_reflection.v1alpha.proto_reflection_descriptor_database import (
    ProtoReflectionDescriptorDatabase,
)
# Standard gRPC protobuf types
from google.protobuf.struct_pb2 import Struct
from google.protobuf.json_format import MessageToDict, ParseDict

# protoc-generated marshalling / demarshalling code
import ublox_control_pb2
import ublox_control_pb2_grpc

## our code
import ublox_control_resources


# Configuration for metadata capture from the u-blox ZED-F9T timing chip
# TODO: make this a separate config file and track with version control etc.
f9t_config = {
    "chip_name": "ZED-F9T",
    "protocol": {
        "ubx": {
            "device": None,
            "cfg_keys": ["CFG_MSGOUT_UBX_TIM_TP_USB", "CFG_MSGOUT_UBX_NAV_TIMEUTC_USB"], # default cfg keys to poll
            "packet_ids": ['NAV-TIMEUTC', 'TIM-TP'], # packet_ids to capture: should be in 1-1 corresp with the cfg_keys.
        }
    },
    "timeout (s)": 7,
}


def init_f9t(stub):
    f9t_config_msg = ublox_control_pb2.F9tConfig(
        config=ParseDict(f9t_config, Struct())
    )
    init_summary = stub.InitF9t(f9t_config_msg)
    print(f'init_summary.status=', ublox_control_pb2.InitSummary.InitStatus.Name(init_summary.init_status))
    print(f'{init_summary.message=}')
    print("init_summary.f9t_state=", end='')
    pprint(MessageToDict(init_summary.f9t_state), expand_all=True)
    for i, test_result in enumerate(init_summary.test_results):
        print(f'TEST {i}:')
        print("\t" + str(test_result).replace("\n", "\n\t"))



def get_services(channel):
    reflection_db = ProtoReflectionDescriptorDatabase(channel)
    services = reflection_db.get_services()
    print(f"found services: {services}")

    desc_pool = DescriptorPool(reflection_db)
    service_desc = desc_pool.FindServiceByName("ubloxcontrol.UbloxControl")
    print(f"found UbloxControl service with name: {service_desc.full_name}")
    for methods in service_desc.methods:
        print(f"found method name: {methods.full_name}")
        input_type = methods.input_type
        output_type = methods.output_type
        print(f"\tinput type: {input_type.full_name}")
        print(f"\toutput type: {output_type.full_name}")


def run(host, port=50051):
    # NOTE(gRPC Python Team): .close() is possible on a channel and should be
    # used in circumstances in which the with statement does not fit the needs
    # of the code.
    connection_target = f"{host}:{port}"
    with grpc.insecure_channel(connection_target) as channel:
        stub = ublox_control_pb2_grpc.UbloxControlStub(channel)

        print("-------------- ServerReflection --------------")
        get_services(channel)

        print("-------------- InitF9t --------------")
        init_f9t(stub)


if __name__ == "__main__":
    logging.basicConfig()
    # run(host="10.0.0.60")
    run(host="localhost")

