"""sampler for configuring the transport layer security"""
import configparser
import os
from os.path import dirname
from solace.messaging.config.transport_security_strategy import TLS
from solace.messaging.messaging_service import MessagingService
from solace_sampler.sampler_boot import SamplerUtil

CONFIG_INI_FILE_NAME = "config.ini"


def config_parser():
    """method used to parse the config properties from the config.ini"""
    config = configparser.ConfigParser()
    config.read(os.path.join(dirname(dirname(__file__)), "solace_sampler", CONFIG_INI_FILE_NAME))
    config_parser_dict = {s: dict(config.items(s)) for s in config.sections()}
    if 'solace_properties_template' not in config_parser_dict:
        raise Exception('Unable to locate "solace_properties_template" broker properties in config.ini')
    return config_parser_dict['solace_properties_template']


broker_properties_value = config_parser()


class HowToConnectWithTls:
    """sampler class for validating transport layer security configuration"""

    @staticmethod
    def tls_with_certificate_validation_and_trusted_store_settings(props, ignore_expiration: bool,
                                                                   trust_store_path: str):
        """method for validating tls certificate along with trusted store by providing the file path
        Args:
            props: broker properties
            trust_store_path (str): trust store file path
            ignore_expiration (bool): holds a boolean flag whether to ignore expiration or not

        Returns:
            a new connection to messaging service
        """
        try:
            transport_security = TLS.create() \
                .with_certificate_validation(ignore_expiration=ignore_expiration,
                                             trust_store_file_path=trust_store_path)
            messaging_service = MessagingService.builder().from_properties(props)\
                .with_transport_security_strategy(transport_security).build()
            messaging_service.connect()

        finally:
            messaging_service.disconnect()

    @staticmethod
    def tls_downgradable_to_plain_text(props):
        """method for validating the tls downgradable to plain text
        Args:
            props: broker properties

        Returns:
            a new connection to messaging service
        """
        try:
            transport_security = TLS.create().downgradable()
            messaging_service = MessagingService.builder().from_properties(props) \
                .with_transport_security_strategy(transport_security).build()
            messaging_service.connect()
        finally:
            messaging_service.disconnect()

    @staticmethod
    def tls_with_excluded_protocols(props, excluded_protocol: TLS.SecureProtocols):
        """method for validating excluding tls protocols
        Args:
            props: broker properties
            excluded_protocol (SecuredProtocols): contains a value or a list of values of protocols to be excluded

        Returns:
            a new connection to messaging service
        """
        try:
            transport_security = TLS.create().with_excluded_protocols(excluded_protocol)

            messaging_service = MessagingService.builder().from_properties(props) \
                .with_transport_security_strategy(transport_security).build()
            messaging_service.connect()
        finally:
            messaging_service.disconnect()

    @staticmethod
    def tls_with_cipher_suites(props, cipher_suite: str):
        """method for validating the cipher suited with tls
        Args:
            props: broker properties
            cipher_suite (str): cipher suite list

        Returns:
            a new connection to messaging service
        """
        try:
            transport_security = TLS.create().with_cipher_suites(cipher_suite)

            messaging_service = MessagingService.builder().from_properties(props) \
                .with_transport_security_strategy(transport_security).build()
            messaging_service.connect()
        finally:
            messaging_service.disconnect()

    @staticmethod
    def run():
        """method to run all the methods related to tls configuration """
        props = broker_properties_value
        trust_store_path_name = SamplerUtil.get_trusted_store_dir()
        cipher_suite = "ECDHE-RSA-AES256-GCM-SHA384"

        HowToConnectWithTls.\
            tls_with_certificate_validation_and_trusted_store_settings(props, ignore_expiration=True,
                                                                       trust_store_path=trust_store_path_name)

        HowToConnectWithTls.tls_downgradable_to_plain_text(props)

        HowToConnectWithTls.tls_with_excluded_protocols(props,
                                                        excluded_protocol=TLS.SecureProtocols.SSLv3)

        HowToConnectWithTls.tls_with_cipher_suites(props, cipher_suite=cipher_suite)


if __name__ == '__main__':
    HowToConnectWithTls.run()
