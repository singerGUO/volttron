
import json
from time import sleep
import datetime
import logging
import sys
import time
import gevent
import pytz as pytz
from paho.mqtt import client as mqtt



from volttron.platform.agent.base_historian import BaseHistorian, add_timing_data_to_header
from volttron.platform.agent import utils

from paho.mqtt.client import MQTTv311, MQTTv31
import paho.mqtt.publish as publish


utils.setup_logging()
_log = logging.getLogger(__name__)
__version__ = '0.2'
client=None


class MQTTHistorian(BaseHistorian):
    """This historian publishes data to MQTT.
    """

    def __init__(self, config_path, **kwargs):

        config = utils.load_config(config_path)

        # We pass every optional parameter to the MQTT library functions so they
        # default to the same values that paho uses as defaults.
        self.mqtt_qos = config.get('mqtt_qos', 0)
        self.mqtt_retain = config.get('mqtt_retain', False)

        self.mqtt_hostname = config.get('mqtt_hostname', 'localhost')
        self.mqtt_port = config.get('mqtt_port', 1883)
        self.mqtt_client_id = config.get('mqtt_client_id', '')
        self.mqtt_keepalive = config.get('mqtt_keepalive', 60)
        self.mqtt_will = config.get('mqtt_will', None)
        self.mqtt_auth = config.get('mqtt_auth', None)
        self.mqtt_tls = config.get('mqtt_tls', None)
        self.connection_result_codes = [
            'Connection successful',
            'Connection refused - incorrect protocol version',
            'Connection refused - invalid client identifier',
            'Connection refused - server unavailable',
            'Connection refused - bad username or password',
            'Connection refused - not authorised']

        protocol = config.get('mqtt_protocol', MQTTv311)
        if protocol == "MQTTv311":
        	protocol = MQTTv311
        elif protocol == "MQTTv31":
            protocol = MQTTv31

        if protocol not in (MQTTv311, MQTTv31):
            raise ValueError("Unknown MQTT protocol: {}".format(protocol))

        self.mqtt_protocol = protocol

        # will be available in both threads.
        self._last_error = 0

        super(MQTTHistorian, self).__init__(**kwargs)


    def on_connect(self, client, userdata, flags, rc):
        print("Connected with result code " + str(rc) + " (" + self.connection_result_codes[rc] + ")")

        # Subscribing in on_connect() means that if we lose the connection and
        # reconnect then subscriptions will be renewed.
        topics = "devices/{}/messages/devicebound/#".format(self.mqtt_client_id)
        client.subscribe(topics)

    
    def mqtt_client(self,on_message):

        global client
        # For Azure IoT Hub, the client_id parameter is REQUIRED, and must be set to the IoT Hub Device ID
        client = mqtt.Client(client_id=self.mqtt_client_id)
        client.on_connect = self.on_connect
        client.on_message = on_message
        client.username_pw_set(
            username=self.mqtt_auth["username"],
            password=self.mqtt_auth["password"])
        client.tls_set(
            # ca_certs=path_to_root_cert,
            # certfile=None, keyfile=None, # only need these for *client* authentication, default None
            # cert_reqs=ssl.CERT_REQUIRED,   # default: broker MUST provide a certificate
        )
        client.connect(self.mqtt_hostname, port=self.mqtt_port, keepalive=self.mqtt_keepalive)
        client.loop_start()


    def timestamp(self):
        return time.mktime(datetime.datetime.now().timetuple())

    def publish_to_historian(self, to_publish_list):
        if client is None:
            self.mqtt_client(None)
        _log.debug("publish_to_historian number of items: {}"
                   .format(len(to_publish_list)))
        current_time = self.timestamp()

        if self._last_error:
            # if we failed we need to wait 60 seconds before we go on.
            if self.timestamp() < self._last_error + 60:
                _log.debug('Not allowing send < 60 seconds from failure')
                return
        to_send = []
        for x in to_publish_list:
            _log.debug("The dictionary is :{} ".format(x))
            topic = x['topic']
            _log.debug("The topic published previously:{} ".format(topic))
            topic="devices/device1/messages/events/"
            _log.debug("The topic published now:{} ".format(topic))
            # Construct payload from data in the publish item.
            # Available fields: 'value', 'headers', and 'meta'
            payload = x['value']
            start_ts = datetime.datetime.now(tz=pytz.utc)
            m = json.dumps({
                'message': 'Testing2',
                'timestamp': str(start_ts)
                })
            _log.debug("payload published is :{} ".format(m))

            to_send.append(m)

            #to_send.append({'topic': topic,
                            #'payload': m,
                            #'qos': self.mqtt_qos,
                            #'retain': self.mqtt_retain})
        # try:
        #     publish.multiple(to_send,
        #                      hostname=self.mqtt_hostname,
        #                      port=self.mqtt_port,
        #                      client_id=self.mqtt_client_id,
        #                      keepalive=self.mqtt_keepalive,
        #                      will=self.mqtt_will,
        #                      auth=self.mqtt_auth,
        #                      tls=self.mqtt_tls,
        #                      protocol=self.mqtt_protocol)
        for m in to_send:
            try:
                client.publish(topic=topic,
                                       payload=m,qos=self.mqtt_qos)

                self.report_all_handled()
                sleep(5)
            except Exception as e:
                _log.warning("Exception ({}) raised by publish: {}".format(
                    e.__class__.__name__,
                    e))
                self._last_error = self.timestamp()


def main(argv=sys.argv):
    utils.vip_main(MQTTHistorian)


if __name__ == '__main__':
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        pass

