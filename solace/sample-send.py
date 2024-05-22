import json
from solace.messaging.messaging_service import MessagingService
from solace.messaging.resources.queue import Queue
from solace.messaging.messaging_service import MessagingServiceBuilder
from solace.messaging.config.transport_security_strategy import TLS
from solace.messaging.publisher.direct_message_publisher import DirectMessagePublisherBuilder
from solace.messaging.publisher.outbound_message import OutboundMessageBuilder
from solace.messaging.errors.pubsubplus_client_error import PubSubPlusClientError
import time


def create_messaging_service(host, vpn_name, username, password):
    service = MessagingServiceBuilder().from_properties({
        "solace.messaging.transport.host": host,
        "solace.messaging.service.vpn-name": vpn_name,
        "solace.messaging.authentication.scheme.basic.username": username,
        "solace.messaging.authentication.scheme.basic.password": password
    }).with_transport_security(TLS.create()).build()
    
    service.connect()
    return service


def create_direct_message_publisher(service):
    publisher = service.create_direct_message_publisher_builder().build()
    publisher.start()
    return publisher


def create_outbound_message_builder(service):
    return service.message_builder()


def send_message(publisher, outbound_message_builder, queue_name, json_data):
    try:
        message = outbound_message_builder.build(json_data)
        publisher.publish(message, destination=Queue.durable_exclusive(queue_name))
        print(f"Message sent to queue {queue_name}: {json_data}")
    except PubSubPlusClientError as e:
        print(f"Failed to send message: {e}")


def main():
    host = "YOUR_SOLACE_HOST"
    vpn_name = "YOUR_VPN_NAME"
    username = "YOUR_USERNAME"
    password = "YOUR_PASSWORD"
    queue_name = "YOUR_QUEUE_NAME"

    json_data = json.dumps({"key": "value"})

    service = create_messaging_service(host, vpn_name, username, password)
    publisher = create_direct_message_publisher(service)
    outbound_message_builder = create_outbound_message_builder(service)
    
    send_message(publisher, outbound_message_builder, queue_name, json_data)
    
    time.sleep(1)  # Ensure message is sent before disconnecting
    service.disconnect()


if __name__ == "__main__":
    main()

