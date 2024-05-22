import json
import time
from solace.messaging.messaging_service import MessagingService
from solace.messaging.resources.queue import Queue
from solace.messaging.config.transport_security_strategy import TLS
from solace.messaging.receiver.queue_message_receiver import QueueMessageReceiverBuilder
from solace.messaging.receiver.inbound_message import InboundMessage
from solace.messaging.receiver.message_handler import MessageHandler
from solace.messaging.errors.pubsubplus_client_error import PubSubPlusClientError


class QueueMessageHandler(MessageHandler):
    def on_message(self, message: InboundMessage):
        try:
            payload = message.get_payload_as_string()
            json_data = json.loads(payload)
            print(f"Received message: {json_data}")
            message.acknowledge()
        except json.JSONDecodeError as e:
            print(f"Failed to decode JSON message: {e}")


def create_messaging_service(host, vpn_name, username, password):
    service = MessagingService.builder().from_properties({
        "solace.messaging.transport.host": host,
        "solace.messaging.service.vpn-name": vpn_name,
        "solace.messaging.authentication.scheme.basic.username": username,
        "solace.messaging.authentication.scheme.basic.password": password
    }).with_transport_security(TLS.create()).build()
    
    service.connect()
    return service


def create_queue_message_receiver(service, queue_name):
    queue = Queue.durable_exclusive(queue_name)
    receiver = service.create_queue_message_receiver_builder().with_queue(queue).build()
    receiver.start()
    return receiver


def main():
    host = "YOUR_SOLACE_HOST"
    vpn_name = "YOUR_VPN_NAME"
    username = "YOUR_USERNAME"
    password = "YOUR_PASSWORD"
    queue_name = "YOUR_QUEUE_NAME"

    service = create_messaging_service(host, vpn_name, username, password)
    receiver = create_queue_message_receiver(service, queue_name)
    
    message_handler = QueueMessageHandler()
    receiver.receive_async(message_handler)
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Shutting down...")
    finally:
        receiver.terminate()
        service.disconnect()


if __name__ == "__main__":
    main()

