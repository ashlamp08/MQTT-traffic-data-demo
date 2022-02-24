import paho.mqtt.client as mqtt
from db_connection import DBConnection
from typing import List, Set
import sys
import json
import random


def main(topics: list, subscriber_count, randomized_unsubscribe):
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
        

    for i in range(subscriber_count):
        topic_name = random.choice(topics)
        print(f"Creating subscriber to topic [{topic_name}]")
        client = mqtt.Client(f"S{i}")
        client.connect(broker_address)
        client.on_message = on_message
        client.loop_start()
        client.subscribe(topic_name)
        subscribers.append(client)

    if randomized_unsubscribe:
        # TODO implement randomized unsubscribe
        pass

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
    topics = []
    SUBSCRIBER_AMOUNT = int(sys.argv[1])

    # Find all the possible roads
    with open(f"./sample_data/1577484032.json", "r") as f:
        data = json.loads(f.read())
        for road_data in data:
            road_name = road_data["road_name"]
            topics.append(f"{road_name}/#")

    main(topics, SUBSCRIBER_AMOUNT, False)
