"""The Python implementation of the gRPC ublox control server."""

from concurrent import futures
import logging
import math
import time
import json

import datetime

import grpc
from grpc_reflection.v1alpha import reflection
from google.protobuf.struct_pb2 import Struct
from google.protobuf.json_format import MessageToDict, ParseDict

import ublox_control_pb2
import ublox_control_pb2_grpc
from ublox_control_resources import *

"""route guide example utils"""

def get_feature(feature_db, point):
    """Returns Feature at given location or None."""
    for feature in feature_db:
        if feature.location == point:
            return feature
    return None


def get_distance(start, end):
    """Distance between two points."""
    coord_factor = 10000000.0
    lat_1 = start.latitude / coord_factor
    lat_2 = end.latitude / coord_factor
    lon_1 = start.longitude / coord_factor
    lon_2 = end.longitude / coord_factor
    lat_rad_1 = math.radians(lat_1)
    lat_rad_2 = math.radians(lat_2)
    delta_lat_rad = math.radians(lat_2 - lat_1)
    delta_lon_rad = math.radians(lon_2 - lon_1)

    # Formula is based on http://mathforum.org/library/drmath/view/51879.html
    a = pow(math.sin(delta_lat_rad / 2), 2) + (
        math.cos(lat_rad_1)
        * math.cos(lat_rad_2)
        * pow(math.sin(delta_lon_rad / 2), 2)
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    R = 6371000
    # metres
    return R * c


"""ublox control utils"""

def test_0():
    msg = "todo: replace with real test 0"
    return True, msg

def test_1():
    msg = "todo: replace with real test 1"
    return False, msg

tests = [
    {
        "name": "example_0",
        "test_fn": test_0,
    },
    {
        "name": "example_1",
        "test_fn": test_1,
    }
]

def run_initialization_tests():
    """Run each test case in tests.
    Each test returns a tuple of (result: bool, message: str)
        - result: whether the test passed
        - message: information about any failure cases or warnings.
    Returns enum init_status and a list of test_results.
    """
    # TODO: add real validation checks here
    all_pass = True
    test_results = []
    for test in tests:
        result, message = test["test_fn"]()
        if result:
            result = ublox_control_pb2.TestResult.Result.PASS
        else:
            result = ublox_control_pb2.TestResult.Result.FAIL
            all_pass = False

        test_result = ublox_control_pb2.TestResult(
            name=test["name"],
            result=result,
            message=message
        )
        test_results.append(test_result)
    if all_pass:
        init_status = ublox_control_pb2.InitSummary.Status.SUCCESS
    else:
        init_status = ublox_control_pb2.InitSummary.Status.FAILURE
    return init_status, test_results


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
        self.db = read_route_guide_database() # TODO: change
        self.f9t_state = f9t_state.copy()

    def GetFeature(self, request, context):
        feature = get_feature(self.db, request)
        if feature is None:
            return ublox_control_pb2.Feature(name="", location=request)
        else:
            return feature

    def ListFeatures(self, request, context):
        left = min(request.lo.longitude, request.hi.longitude)
        right = max(request.lo.longitude, request.hi.longitude)
        top = max(request.lo.latitude, request.hi.latitude)
        bottom = min(request.lo.latitude, request.hi.latitude)
        for feature in self.db:
            if (
                feature.location.longitude >= left
                and feature.location.longitude <= right
                and feature.location.latitude >= bottom
                and feature.location.latitude <= top
            ):
                yield feature

    def RecordRoute(self, request_iterator, context):
        point_count = 0
        feature_count = 0
        distance = 0.0
        prev_point = None

        start_time = time.time()
        for point in request_iterator:
            point_count += 1
            if get_feature(self.db, point):
                feature_count += 1
            if prev_point:
                distance += get_distance(prev_point, point)
            prev_point = point

        elapsed_time = time.time() - start_time
        return ublox_control_pb2.RouteSummary(
            point_count=point_count,
            feature_count=feature_count,
            distance=int(distance),
            elapsed_time=int(elapsed_time),
        )

    def RouteChat(self, request_iterator, context):
        prev_notes = []
        for new_note in request_iterator:
            for prev_note in prev_notes:
                if prev_note.location == new_note.location:
                    yield prev_note
            prev_notes.append(new_note)

    def InitF9t(self, request, context):
        f9t_config_dict = MessageToDict(request.config, preserving_proto_field_name=True)
        # TODO: add real validation checks here
        init_status, test_results = run_initialization_tests()

        message = ""

        # TODO: change later
        init_summary = ublox_control_pb2.InitSummary(
            status=init_status,
            message=message,
            f9t_state=ParseDict(self.f9t_state, Struct()),
            test_results=test_results,
        )
        return init_summary

        # # Now you can work with the dictionary
        # print(metadata_dict["key1"])  # Output: value1
        # feature = get_feature(self.db, request)
        # if feature is None:
        #     return ublox_control_pb2.Feature(name="", location=request)
        # else:
        #     return feature





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
    server.wait_for_termination()


if __name__ == "__main__":
    logging.basicConfig()
    serve()
