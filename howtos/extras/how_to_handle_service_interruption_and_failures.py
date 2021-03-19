"""module to handle service interruption and failures"""

from solace.messaging.messaging_service import MessagingService, ServiceInterruptionListener, ServiceEvent
from solace.messaging.publisher.direct_message_publisher import PublishFailureListener
from solace.messaging.resources.topic import Topic
from sampler_boot import SamplerBoot, SolaceConstants

constants = SolaceConstants
boot = SamplerBoot()


class PublishFailureListenerImpl(PublishFailureListener):
    def on_failed_publish(self, failed_publish_event: 'FailedPublishEvent'):
        print(f"fail_destination name:{failed_publish_event.get_destination()}\n"
              f"fail_message:{failed_publish_event.get_message()}\n"
              f"fail_timestamp:{failed_publish_event.get_timestamp()}\n"
              f"fail_exception:{failed_publish_event.get_exception()}\n")


class ServiceInterruptionListenerImpl(ServiceInterruptionListener):
    """implementation class for the service interruption listener"""
    def on_service_interrupted(self, event: ServiceEvent):
        print("Service Interruption Listener Callback")
        print(f"Timestamp: {event.get_time_stamp()}")
        print(f"Uri: {event.get_broker_uri()}")
        print(f"Error cause: {event.get_cause()}")
        print(f"Message: {event.get_message()}")


class HowToHandleServiceInterruptionAndFailures:

    @staticmethod
    def notify_about_service_access_unrecoverable_interruption(messaging_service, destination_name, message):
        """example how to configure service access to receive notifications about unrecoverable interruption

        Returns:
            configured instance of messaging service
        """
        try:
            service_interruption_listener = ServiceInterruptionListenerImpl()
            messaging_service.add_service_interruption_listener(service_interruption_listener)

            direct_publish_service = messaging_service.create_direct_message_publisher_builder() \
                .build()
            direct_publish_service.start()
            direct_publish_service.publish(destination=destination_name, message=message)

            return messaging_service
        finally:
            messaging_service.remove_service_interruption_listener(service_interruption_listener)

    @staticmethod
    def notify_on_direct_publisher_failures(messaging_service: MessagingService, destination_name, message,
                                            buffer_capacity, message_count):
        """
        configure direct message publisher to receive notifications about publish
        failures if any occurred
        """
        try:
            direct_publish_service = messaging_service.create_direct_message_publisher_builder() \
                .on_back_pressure_reject(buffer_capacity=buffer_capacity) \
                .build()
            direct_publish_service.start()
            publish_failure_listener = PublishFailureListenerImpl()
            direct_publish_service.set_publish_failure_listener(publish_failure_listener)
            outbound_msg = messaging_service.message_builder() \
                .with_application_message_id(constants.APPLICATION_MESSAGE_ID) \
                .build(message)
            direct_publish_service.publish(destination=destination_name, message=outbound_msg)

        finally:
            direct_publish_service.terminate(0)

    @staticmethod
    def run():
        try:
            messaging_service = MessagingService.builder().from_properties(boot.broker_properties()).build()
            messaging_service.connect_async()
            destination_name = Topic.of(constants.TOPIC_ENDPOINT_DEFAULT)
            buffer_capacity = 20
            message_count = 50

            HowToHandleServiceInterruptionAndFailures.notify_on_direct_publisher_failures(messaging_service,
                                                                                          destination_name,
                                                                                          constants.MESSAGE_TO_SEND,
                                                                                          buffer_capacity,
                                                                                          message_count)
            HowToHandleServiceInterruptionAndFailures.\
                notify_about_service_access_unrecoverable_interruption(messaging_service,
                                                                       destination_name, constants.MESSAGE_TO_SEND)
        finally:
            messaging_service.disconnect_async()


if __name__ == '__main__':
    HowToHandleServiceInterruptionAndFailures.run()
