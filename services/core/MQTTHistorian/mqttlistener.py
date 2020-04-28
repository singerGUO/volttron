from paho.mqtt.client import MQTTv311, MQTTv31
from paho.mqtt.subscribe import callback

PORT = 8883
PROTOCOL = MQTTv311

# Callback function to print out message topics and payloads
def listen(client, userdata, message):
    print(message.topic, message.payload)

# Subscribe to all messages and loop forever
#callback(listen, '#', port=PORT, protocol=PROTOCOL)
callback(listen, '#', port=PORT, protocol=PROTOCOL,hostname="TignisIhub.azure-devices.net")
#callback(listen, 'devices/device1/messages/events', port=PORT, protocol=PROTOCOL,hostname="mqtt.eclipse.org")
#callback(listen, 'devices/device1/messages/events', port=PORT, protocol=PROTOCOL,hostname="mqtt.eclipse.org")
#callback(listen, 'devices/device1/messages/events', port=PORT, protocol=PROTOCOL,hostname="mqtt.eclipse.org") 
#callback(listen, 'devices/device1/messages/events', port=PORT, protocol=PROTOCOL,hostname="mqtt.eclipse.org")
