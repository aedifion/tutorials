import argparse
from datetime import datetime
import logging
import math
from numbers import Integral
import random
import ssl
import sys
import time
import threading

import certifi
from dateutil.parser import parse
from pytz import UTC
from six import text_type

import paho.mqtt.client as mqtt

logging.basicConfig(format='%(asctime)s %(levelname)s - [%(processName)s - %(threadName)s] - %(name)s %(funcName)s - %(message)s', datefmt="%Y-%m-%d %H:%M:%S", level="INFO")
logger = logging.getLogger(__name__)

_EPOCH = UTC.localize(datetime.utcfromtimestamp(0))

def _convert_timestamp(timestamp):
    if isinstance(timestamp, Integral):
        return timestamp
    if isinstance(timestamp, text_type):
        timestamp = parse(timestamp)
    if isinstance(timestamp, datetime):
        if not timestamp.tzinfo:
            timestamp = UTC.localize(timestamp)
        return int((timestamp - _EPOCH).total_seconds() * 1e9)
    else:
        raise ValueError(timestamp)

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    logger.info("Connected with result code: {} - {}".format(rc, mqtt.connack_string(rc)))
    client.connected.set()

def on_publish(client, userdata, mid):
    logger.debug("Published message mid={}.".format(mid))

def on_disconnect(client, userdata, rc):
    logger.info("Disconnected with result code: {} - {}".format(rc, mqtt.connack_string(rc)))
    client.connected.clear()

def on_message(client, userdata, message):
    logger.info("Received message on topic {}: {}".format(message.topic, message.payload))

def on_subscribe(client, userdata, mid, granted_qos):
    logger.debug("Successfully subscribed to topic: {}".format(_sub_topics.get(mid, "UnknownTopic")))

def _generate_data(pattern, n):
    logger.debug("Generating {} datapoints with pattern '{}'.".format(n, pattern))
    if pattern == "linear":
        return range(n)
    elif pattern == "sinus":
        return [math.sin(i * 0.25) + 20 for i in range(n)]
    else:
        return [random.uniform(15.0, 25.0) for _ in range(n)]

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Run an MQTT publisher')
    parser.add_argument('-H', '--host', default="mqtt-dev.aedifion.io", help="The MQTT broker address.")
    parser.add_argument('-P', '--port', type=int, default=8884, help="The MQTT broker port.")
    parser.add_argument('-u', '--username', help="The login username.")
    parser.add_argument('-p', '--password', help="The login password.")
    parser.add_argument('-t', '--topic', help="The MQTT topic which to publish/subscribe to.")
    parser.add_argument('--client-id', help="Custom client id password or None for random.")
    parser.add_argument('--datapoint', default="test_datapoint", help="The name of the datapoint to publish to.")
    parser.add_argument('--num-observations', default=60, type=int, help="The number of observations to generate and publish.")
    parser.add_argument('--pattern', choices=["random", "sinus", "linear"], default="sinus", help="The type of data to generate.")
    args = parser.parse_args()

    # Check parameters
    if args.topic is None:
        logger.error("Please specify a valid topic to publish/subscribe to.")
        sys.exit(-1)
    if args.username is None or args.password is None:
        logger.error("Please set login credentials using '-U/--username' and '-P/--password'")
        sys.exit(-1)

    # Instantiate an MQTT Client
    client_id = args.client_id
    client = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv31)
    client.enable_logger()
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_publish = on_publish
    client.on_message = on_message

    # Configure TLS
    ca_certs = certifi.where()
    client.tls_set(ca_certs=ca_certs, tls_version=ssl.PROTOCOL_TLSv1_2)

    # Confiugre login credentials
    client.username_pw_set(args.username, args.password)

    # Connect the client to the broker
    # Note: ``connect_async`` connects asynchronously, i.e. within another thread.
    #       We set the Event ``client.connected`` from the callback
    #       ``on_connect`` to signal to this thread that the connetion has been
    #       established.
    logger.info("Connecting to {}@{}:{}".format(args.username, args.host, args.port))
    client.connected = threading.Event()
    client.connect_async(args.host, args.port, 60)
    client.loop_start()

    # Wait for connection
    logger.debug("Waiting for connection to broker ...")
    client.connected.wait()

    # Subscribe to topic that we will publish to
    logger.info("Subscribing to topic: {}".format(args.topic))
    result, mid = client.subscribe(args.topic, qos=1)
    _sub_topics = {mid: args.topic}

    # Generate some dummy data and publish
    data = _generate_data(args.pattern, args.num_observations)
    for i, value in enumerate(data):
        timestamp = _convert_timestamp(datetime.utcnow())
        payload = "{} value={} {}".format(args.datapoint, value, timestamp)  # Influx line protocol format
        logger.info("Publishing to message {}: {}".format(i, payload))
        rc, mid = client.publish(args.topic, payload, qos=1)
        time.sleep(1)

    # Disconnect cleanly
    client.disconnect()
    client.loop_stop()
