# Copyright 2015 gRPC authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""The Python implementation of the gRPC route guide client."""

from __future__ import print_function

import logging
import random

from rich import print
from rich.pretty import pprint

## gRPC imports
import grpc

# gRPC reflection service (tells which services are available)
from google.protobuf.descriptor_pool import DescriptorPool
from grpc_reflection.v1alpha.proto_reflection_descriptor_database import (
    ProtoReflectionDescriptorDatabase,
)
# Standard datatypes
from google.protobuf.struct_pb2 import Struct
from google.protobuf.json_format import MessageToDict, ParseDict

# Generated marshalling / demarshalling code
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



def make_route_note(message, latitude, longitude):
    return ublox_control_pb2.RouteNote(
        message=message,
        location=ublox_control_pb2.Point(latitude=latitude, longitude=longitude),
    )


def format_point(point):
    # not delegating in point.__str__ because it is an empty string when its
    # values are zero. In addition, it puts a newline between the fields.
    return "latitude: %d, longitude: %d" % (point.latitude, point.longitude)


def guide_get_one_feature(stub, point):
    feature = stub.GetFeature(point)
    if not feature.location:
        print("Server returned incomplete feature")
        return

    if feature.name:
        print(
            "Feature called %r at %s"
            % (feature.name, format_point(feature.location))
        )
    else:
        print("Found no feature at %s" % format_point(feature.location))


def guide_get_feature(stub):
    guide_get_one_feature(
        stub, ublox_control_pb2.Point(latitude=409146138, longitude=-746188906)
    )
    guide_get_one_feature(stub, ublox_control_pb2.Point(latitude=0, longitude=0))


def guide_list_features(stub):
    rectangle = ublox_control_pb2.Rectangle(
        lo=ublox_control_pb2.Point(latitude=400000000, longitude=-750000000),
        hi=ublox_control_pb2.Point(latitude=420000000, longitude=-730000000),
    )
    print("Looking for features between 40, -75 and 42, -73")

    features = stub.ListFeatures(rectangle)

    for feature in features:
        print(
            "Feature called %r at %s"
            % (feature.name, format_point(feature.location))
        )


def generate_route(feature_list):
    for _ in range(0, 10):
        random_feature = random.choice(feature_list)
        print("Visiting point %s" % format_point(random_feature.location))
        yield random_feature.location


def guide_record_route(stub):
    feature_list = ublox_control_resources.read_route_guide_database()

    route_iterator = generate_route(feature_list)
    route_summary = stub.RecordRoute(route_iterator)
    print("Finished trip with %s points " % route_summary.point_count)
    print("Passed %s features " % route_summary.feature_count)
    print("Travelled %s meters " % route_summary.distance)
    print("It took %s seconds " % route_summary.elapsed_time)



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


def generate_messages():
    messages = [
        make_route_note("First message", 0, 0),
        make_route_note("Second message", 0, 1),
        make_route_note("Third message", 1, 0),
        make_route_note("Fourth message", 0, 0),
        make_route_note("Fifth message", 1, 0),
    ]
    for msg in messages:
        print("Sending %s at %s" % (msg.message, format_point(msg.location)))
        yield msg


def guide_route_chat(stub):
    responses = stub.RouteChat(generate_messages())
    for response in responses:
        print(
            "Received message %s at %s"
            % (response.message, format_point(response.location))
        )

def get_services():
    with grpc.insecure_channel("localhost:50051") as channel:
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
            print(f"\tinput type for this method: {input_type.full_name}")
            print(f"\toutput type for this method: {output_type.full_name}")

        # request_desc = desc_pool.FindMessageTypeByName(
        #     "helloworld.HelloRequest"
        # )
        # print(f"found request name: {request_desc.full_name}")


def run():
    # NOTE(gRPC Python Team): .close() is possible on a channel and should be
    # used in circumstances in which the with statement does not fit the needs
    # of the code.
    with grpc.insecure_channel("localhost:50051") as channel:
    # with grpc.insecure_channel("10.0.0.60:50051") as channel:
        stub = ublox_control_pb2_grpc.UbloxControlStub(channel)
        # print("-------------- GetFeature --------------")
        # guide_get_feature(stub)
        # print("-------------- ListFeatures --------------")
        # guide_list_features(stub)
        # print("-------------- RecordRoute --------------")
        # guide_record_route(stub)
        # print("-------------- RouteChat --------------")
        # guide_route_chat(stub)
        print("-------------- InitF9t --------------")
        init_f9t(stub)


if __name__ == "__main__":
    logging.basicConfig()
    # get_services()
    run()
