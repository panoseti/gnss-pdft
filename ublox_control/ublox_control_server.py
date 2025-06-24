"""
The Python implementation of a gRPC UbloxControl server.
The server requires the following to correctly:
    1. A valid network connection to the Redis database on the headnode with
    R/W user permissions to the Redis UBLOX hashset.
    2. A valid /dev file for a connected ZED-F9T u-blox chip.
    3. The installation of all Python packages specified in requirements.txt.
"""

from concurrent import futures
import logging

import grpc
from grpc_reflection.v1alpha import reflection
from google.protobuf.struct_pb2 import Struct
from google.protobuf.json_format import MessageToDict, ParseDict

import ublox_control_pb2
import ublox_control_pb2_grpc
from ublox_control_resources import *

# Default chip state upon server startup.
f9t_state = {
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


"""gRPC server wrapper for UbloxControl RPCss"""

class UbloxControlServicer(ublox_control_pb2_grpc.UbloxControlServicer):
    """Provides methods that implement functionality of an u-blox control server."""

    def __init__(self):
        self.f9t_state = f9t_state.copy()

    def InitF9t(self, request, context):
        f9t_config_dict = MessageToDict(request.config, preserving_proto_field_name=True)
        # TODO: add real validation checks here
        init_status, test_results = run_initialization_tests()

        message = ""

        # TODO: change later
        init_summary = ublox_control_pb2.InitSummary(
            init_status=init_status,
            message=message,
            f9t_state=ParseDict(self.f9t_state, Struct()),
            test_results=test_results,
        )
        return init_summary


def serve():
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
    print("gRPC server is running.\nPress CTRL+C to stop.")
    server.wait_for_termination()


if __name__ == "__main__":
    logging.basicConfig()
    serve()
