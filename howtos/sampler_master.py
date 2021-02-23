"""this module is used for running the samplers connect messaging service and direct message publisher"""
import urllib3

from solace.messaging.messaging_service import MessagingService
from solace_sampler.how_to_access_api_metrics import HowToAccessApiMetrics
from solace_sampler.how_to_configure_authentication import HowToConfigureAuthentication
from solace_sampler.how_to_configure_service_connection_reconnection_retries import HowToConnectWithDifferentStrategy
from solace_sampler.how_to_configure_transport_layer_security import HowToConnectWithTls
from solace_sampler.how_to_connect_messaging_service import HowToConnectMessagingService
from solace_sampler.how_to_for_unusual_situtations import HowToConnectMessagingServiceWithReConnectionStrategy
from solace_sampler.how_to_set_core_api_log_level import HowToSetCoreApiLogLevel
from solace_sampler.pubsub.how_to_direct_consume_message import HowToDirectConsumeSampler
from solace_sampler.pubsub.how_to_direct_consume_with_share_name import \
    HowToDirectConsumeShareNameSampler
from solace_sampler.pubsub.how_to_direct_publish_consume_business_obj import \
    HowToDirectConsumeBusinessObjectSampler
from solace_sampler.pubsub.how_to_direct_publish_message import HowToDirectPublishMessage
from solace_sampler.pubsub.how_to_publish_health_check import \
    HowToDirectMessagingHealthCheckSampler
from solace_sampler.pubsub.how_to_use_publish_with_back_pressure import \
    HowToDirectPublishWithBackPressureSampler
from solace_sampler.sampler_boot import SamplerUtil, SamplerBoot

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)  # we are suppressing explicitly about the warning


class SamplerMaster:
    """this class is used to run the multiple samplers"""

    @staticmethod
    def connect_messaging_service():
        messaging_service = MessagingService.builder().from_properties(SamplerBoot().broker_properties()).build()
        messaging_service.connect()
        return messaging_service

    @staticmethod
    def run_samplers():
        """method to run all the samplers"""
        HowToConnectMessagingService().run()
        HowToConfigureAuthentication.run()
        HowToConnectWithDifferentStrategy().run()
        HowToConnectWithTls.run()
        HowToDirectPublishMessage().run()
        HowToDirectMessagingHealthCheckSampler().run()
        HowToDirectPublishWithBackPressureSampler().run()
        HowToDirectConsumeBusinessObjectSampler().publish_and_subscribe()
        HowToDirectConsumeSampler.run()
        HowToDirectPublishWithBackPressureSampler().run()
        HowToAccessApiMetrics().run()
        HowToDirectConsumeShareNameSampler().publish_and_subscribe()
        HowToConnectMessagingServiceWithReConnectionStrategy().run()
        HowToSetCoreApiLogLevel.run()


if __name__ == '__main__':
    boot = SamplerBoot()
    broker_props = boot.broker_properties()
    semp_config = boot.read_semp_configuration()
    SamplerUtil.cert_feature(semp_props=semp_config, broker_props=broker_props)

    SamplerMaster.run_samplers()
