# MQTT Publish/Subscribe Example

This example shows how to publish and subscribe to the aedifion.io MQTT broker.
It shows basic handling of the Eclipse paho mqtt python library as well as the correct message format for the aedifion.io platform.

## Setup

Frist, you need obtain an MQTT topic and valid credentials to read and write to that topic.
To this end, please contact support@aedifion.com.

To run the example, you need to install the reqiured Python dependencies via

	pip3 install -r requirements.txt

Hint: Consider using virutal environments (https://docs.python.org/3/tutorial/venv.html) to sepearte these dependencies from other Python projects.

## Run

Once you have obtained a topic and credentials and installed the dependencies, run the example by

	python3 main.py -U <username> -P <password> -t <topic>

# Contact

- Jan Henrik Ziegeldorf <hziegeldorf (at) aedifion (dot) com>