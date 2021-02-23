""" Run this file to publish all message types using direct message publisher"""
import pickle
from concurrent.futures.thread import ThreadPoolExecutor
from typing import TypeVar, Generic

from solace.messaging.config import _sol_constants
from solace.messaging.messaging_service import MessagingService
from solace.messaging.resources.topic import Topic
from solace.messaging.utils.converter import ObjectToBytes
from solace.messaging.utils.manageable import Metric
from solace_sampler.how_to_access_api_metrics import HowToAccessApiMetrics
from solace_sampler.sampler_boot import SamplerBoot, SolaceConstants

X = TypeVar('X')
constants = SolaceConstants
boot = SamplerBoot()


class MyData(Generic[X]):
    """ sample  class for business object"""
    name = 'some string'

    def __init__(self, name):
        self.name = name

    def get_name(self):
        """ return the name"""
        return self.name


class PopoConverter(ObjectToBytes):  # plain old python object - popo
    """sample converter class"""

    def to_bytes(self, src) -> bytes:
        """This Method converts the given business object to bytes"""

        object_to_byte = pickle.dumps(src)
        return object_to_byte


class HowToDirectPublishMessage:
    """
    class to show how to create a messaging service
    """

    @staticmethod
    def direct_message_publish(messaging_service: MessagingService, destination, message):
        """ to publish str or byte array type message"""

        try:
            direct_publish_service = messaging_service.create_direct_message_publisher_builder().build()
            direct_publish_service.start_async()
            direct_publish_service.publish(destination=destination, message=message)
        finally:
            direct_publish_service.terminate(0)

    @staticmethod
    def direct_message_publish_outbound(messaging_service: MessagingService, destination, message):
        """ to publish outbound message"""
        try:
            direct_publish_service = messaging_service.create_direct_message_publisher_builder().build()
            direct_publish_service.start_async()
            outbound_msg = messaging_service.message_builder() \
                .with_application_message_id(constants.APPLICATION_MESSAGE_ID) \
                .build(message)
            direct_publish_service.publish(destination=destination, message=outbound_msg)
        finally:
            direct_publish_service.terminate(0)

    @staticmethod
    def direct_message_publish_outbound_properties(messaging_service: MessagingService, destination, message):
        """ to publish outbound message with additional properties"""
        try:
            direct_publish_service = messaging_service.create_direct_message_publisher_builder().build()
            direct_publish_service.start_async()
            outbound_msg = messaging_service.message_builder() \
                .with_application_message_id(constants.APPLICATION_MESSAGE_ID) \
                .from_properties(constants.CUSTOM_PROPS).build(message)
            direct_publish_service.publish(destination=destination, message=outbound_msg)
        finally:
            direct_publish_service.terminate(0)

    @staticmethod
    def direct_message_publish_outbound_with_all_props(messaging_service: MessagingService, destination, message):
        """ to publish outbound message"""
        try:
            direct_publish_service = messaging_service.create_direct_message_publisher_builder().build()
            direct_publish_service.start_async()
            outbound_msg = messaging_service.message_builder() \
                .with_property("custom_key", "custom_value") \
                .with_expiration(SolaceConstants.DEFAULT_TIMEOUT_MS) \
                .with_priority(1) \
                .with_sequence_number(12345) \
                .with_application_message_id(constants.APPLICATION_MESSAGE_ID) \
                .with_application_message_type("app_msg_type") \
                .with_http_content_header("text/html", _sol_constants.ENCODING_TYPE) \
                .build(message)
            direct_publish_service.publish(destination=destination, message=outbound_msg)
        finally:
            direct_publish_service.terminate(0)

    @staticmethod
    def direct_message_publish_outbound_business_obj(messaging_service: MessagingService, destination, message_obj,
                                                     converter):
        """ to publish outbound message from a custom object supplied with its own converter"""
        try:
            direct_publish_service = messaging_service.create_direct_message_publisher_builder().build()
            direct_publish_service.start_async()
            outbound_msg = messaging_service.message_builder() \
                .with_application_message_id(constants.APPLICATION_MESSAGE_ID) \
                .build(message_obj, converter=converter)
            direct_publish_service.publish(destination=destination, message=outbound_msg)
        finally:
            direct_publish_service.terminate(0)

    @staticmethod
    def publish_message_with_unique_service():
        try:
            service = MessagingService.builder().from_properties(boot.broker_properties()).build()
            service.connect_async()
            destination_name = Topic.of(constants.TOPIC_ENDPOINT_DEFAULT)
            direct_publish_service = service.create_direct_message_publisher_builder().build()
            direct_publish_service.start_async()
            direct_publish_service.publish(destination=destination_name, message=constants.MESSAGE_TO_SEND)
        finally:
            service.disconnect()
            direct_publish_service.terminate(0)

    @staticmethod
    def run():
        try:
            messaging_service = MessagingService.builder().from_properties(boot.broker_properties()).build()
            messaging_service.connect_async()
            destination_name = Topic.of(constants.TOPIC_ENDPOINT_DEFAULT)

            print("Execute Direct Publish - String")
            HowToDirectPublishMessage() \
                .direct_message_publish(messaging_service, destination_name, constants.MESSAGE_TO_SEND)

            print("Execute Direct Publish - Byte Array")
            HowToDirectPublishMessage() \
                .direct_message_publish(messaging_service, destination_name,
                                        bytearray(constants.MESSAGE_TO_SEND, _sol_constants.ENCODING_TYPE))

            print("Execute Direct Publish - String Outbound Message")
            HowToDirectPublishMessage() \
                .direct_message_publish_outbound(messaging_service, destination_name,
                                                 constants.MESSAGE_TO_SEND + str("_outbound based"))

            print("Execute Direct Publish - Byte Array Outbound Message")
            HowToDirectPublishMessage() \
                .direct_message_publish_outbound(messaging_service, destination_name,
                                                 bytearray(constants.MESSAGE_TO_SEND + str("_outbound based"),
                                                           _sol_constants.ENCODING_TYPE))

            print("Execute Direct Publish - Byte Array Outbound Message with props")
            HowToDirectPublishMessage() \
                .direct_message_publish_outbound_properties(messaging_service, destination_name,
                                                            bytearray(constants.MESSAGE_TO_SEND +
                                                                      str("_outbound based with props"),
                                                                      _sol_constants.ENCODING_TYPE))

            print("Execute Direct Publish - String Outbound Message with all props")
            HowToDirectPublishMessage(). \
                direct_message_publish_outbound_with_all_props(messaging_service, destination_name,
                                                               constants.MESSAGE_TO_SEND + str("_outbound based"))

            print("Execute Direct Publish - Generics Outbound Message")
            HowToDirectPublishMessage() \
                .direct_message_publish_outbound_business_obj(messaging_service, destination_name,
                                                              message_obj=MyData('some value'),
                                                              converter=PopoConverter())

            print("Execute Direct Publish - Concurrent testing")
            for e in range(10):  # make sure you have try-me1 & try-me2 already
                destination_name = Topic.of(constants.TOPIC_ENDPOINT_2)
                if e % 2 == 0:
                    destination_name = Topic.of(constants.TOPIC_ENDPOINT_1)

                with ThreadPoolExecutor() as executor:
                    future = executor.submit(HowToDirectPublishMessage().direct_message_publish, messaging_service,
                                             destination_name, constants.MESSAGE_TO_SEND)

                    future.result()
        finally:
            api_metrics = HowToAccessApiMetrics()
            api_metrics.access_individual_api_metrics(messaging_service, Metric.TOTAL_MESSAGES_SENT)
            api_metrics.to_string_api_metrics(messaging_service)

            messaging_service.disconnect_async()


if __name__ == '__main__':
    HowToDirectPublishMessage().run()
