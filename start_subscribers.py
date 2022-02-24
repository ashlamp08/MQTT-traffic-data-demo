import paho.mqtt.client as mqtt
from db_connection import DBConnection
from typing import List
import sys
import json


def main(topics: list):
    """Start a subscriber and subcsribe to given topic

    Args:
        topic_name (str): The name of the topic to subscribe to
    """

    broker_address = "0.0.0.0"
    subscribers: List[mqtt.Client] = []

    def on_message(client: mqtt.Client, userdata, message):
        """A custom callback for processing recieved messages"""

        client_id = str(client._client_id.decode("utf-8"))
        data = str(message.payload.decode("utf-8"))

        data_json = json.loads(data)

        print(f"{client_id} recieved: {data[:100]} .. ")

        # log message as recieved
        with DBConnection() as conn:
            conn.log_message(data_json["timestamp"], json.dumps(data_json["road_name"]), json.dumps(data_json["segment"]))
        

    for i, topic_name in enumerate(topics):
        print(f"Creating subscriber to topic [{topic_name}]")
        client = mqtt.Client(f"S{i}")
        client.connect(broker_address)
        client.loop_start()
        client.subscribe(topic_name)
        client.on_message = on_message
        subscribers.append(client)

    # keep loop open until keybord interrupt
    print("")
    try:
        while True:
            pass
    except KeyboardInterrupt:
        pass

    print("stop subscriber")
    for s in subscribers:
        s.loop_stop()


if __name__ == "__main__":
    # pass first argument (topic to subscribe to)
    topics = []
    for topic_name in sys.argv[1:]:
        topics.append(topic_name)

    main(topics)
