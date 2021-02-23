"""sampler module for reconnection strategy"""
import time

from solace.messaging.config.retry_strategy import RetryStrategy
from solace.messaging.config.solace_properties import transport_layer_properties
from solace.messaging.errors.pubsubplus_client_error import PubSubPlusClientError
from solace.messaging.messaging_service import MessagingService
from solace.messaging.resources.topic import Topic
from solace_sampler.sampler_boot import SolaceConstants, SamplerBoot, SamplerUtil

constants = SolaceConstants
boot = SamplerBoot()


class HowToConnectWithDifferentStrategy:
    """
    This is a sampler for reconnection strategy
    """

    @staticmethod
    def connect_never_retry():
        """
             creates a new instance of message service, that is used to configure
             direct message instances from config

             Returns: new connection for Direct messaging
             Raises:
                PubSubPlusClientError
         """
        try:
            messaging_service = MessagingService.builder().from_properties(boot.broker_properties()) \
                .with_reconnection_retry_strategy(RetryStrategy.never_retry()).build()
            future = messaging_service.connect_async()

            return future.result()

        except PubSubPlusClientError as exception:
            raise exception

        finally:
            messaging_service.disconnect_async()

    @staticmethod
    def connect_retry_interval(retry_interval):
        """
             creates a new instance of message service, that is used to configure
             direct message instances from config

             Returns: new connection for Direct messaging
             Raises:
                PubSubPlusClientError
         """
        try:
            messaging_service = MessagingService.builder().from_properties(boot.broker_properties()) \
                .with_reconnection_retry_strategy(RetryStrategy.forever_retry(retry_interval)).build()
            future = messaging_service.connect_async()
            return future.result()

        except PubSubPlusClientError as exception:
            raise exception
        finally:
            messaging_service.disconnect_async()

    @staticmethod
    def connect_parametrized_retry(retries, retry_interval):
        """
             creates a new instance of message service, that is used to configure
             direct message instances from config

             Returns: new connection for Direct messaging
             Raises:
                PubSubPlusClientError
         """
        try:
            message_service = MessagingService.builder() \
                .from_properties(boot.broker_properties()) \
                .with_reconnection_retry_strategy(RetryStrategy.parametrized_retry(retries, retry_interval)) \
                .build(SamplerUtil.get_new_application_id())
            return message_service.connect()
        except PubSubPlusClientError as exception:
            print(f'Exception: {exception}')
            raise exception
        finally:
            message_service.disconnect()

    @staticmethod
    def connect_using_properties(retries: int, retry_interval: int):
        """
             creates a new instance of message service, that is used to configure
             direct message instances from config

             Returns: new connection for Direct messaging
             Raises:
                PubSubPlusClientError
         """
        service_config = dict()
        try:
            service_config[transport_layer_properties.RECONNECTION_ATTEMPTS] = retries

            service_config[transport_layer_properties.RECONNECTION_ATTEMPTS_WAIT_INTERVAL] = retry_interval
            future = MessagingService.builder().from_properties(boot.broker_properties()).build().connect_async()
            return future.result()
        except PubSubPlusClientError as exception:
            raise exception

    @staticmethod
    def add_listener_when_reconnection_happens(retries: int, retry_interval: int) -> 'MessagingService':
        """method adds a reconnection listener when an reconnection happens using the reconnection strategy
        Args:
            retries (int): the number of retries count
            retry_interval (int): the retry interval value

        Returns:
            the listener events
        """
        events = list()

        def on_reconnection(event_p):
            events.append(event_p)
            print("current event", events)

        try:
            destination_name = Topic.of(constants.TOPIC_ENDPOINT_DEFAULT)
            message = constants.MESSAGE_TO_SEND
            number_of_message_to_send = 10

            messaging_service = MessagingService.builder().from_properties(boot.broker_properties()) \
                .with_reconnection_retry_strategy(RetryStrategy.parametrized_retry(retries, retry_interval)) \
                .add_reconnection_listener(on_reconnection).build()
            messaging_service1_status_code = messaging_service.connect_async()
            print("messaging service status code: ", messaging_service1_status_code)
            for _ in range(number_of_message_to_send):
                publish_service = messaging_service.create_direct_message_publisher_builder().build()
                publish_service.publish(destination=destination_name, message=message)

            session_force_failure_status = messaging_service.disconnect_force()

            print("session force failure status: ", session_force_failure_status)

            for _ in range(number_of_message_to_send):
                publish_service = messaging_service.create_direct_message_publisher_builder().build()
                publish_service.publish(destination=destination_name, message=message)
            messaging_service.disconnect()

        finally:
            timeout = time.time() + 60 * 1
            while True:
                if 13 in events or time.time() > timeout:
                    break
            return events  # MessagingService got list

    @staticmethod
    def run():
        """
        :return: Success or Failed according to connection established
        """
        print("\tSuccess" if HowToConnectWithDifferentStrategy().connect_never_retry() == 0 else "Failed")
        print("\tSuccess" if HowToConnectWithDifferentStrategy().connect_retry_interval(3) == 0 else "Failed")
        print("\tSuccess" if HowToConnectWithDifferentStrategy().connect_parametrized_retry(3, 40) == 0 else "Failed")
        print("\tSuccess" if HowToConnectWithDifferentStrategy().connect_using_properties(5, 30000) == 0 else "Failed")
        # print("\tSuccess" if HowToConnectWithDifferentStrategy()
        #       .add_listener_when_reconnection_happens(3, 3000) == [0, 12, 13] else "Failed")


if __name__ == '__main__':
    HowToConnectWithDifferentStrategy().run()
