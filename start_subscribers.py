import paho.mqtt.client as mqtt
from db_connection import DBConnection
from typing import List, Set
import sys
import json
import random
import time


def main(topics: list, subscriber_count, max_subscribed_topics, randomized_unsubscribe):
    """Start subscribers"""

    broker_address = "0.0.0.0"
    subscribers = {}

    def on_message(client: mqtt.Client, userdata, message):
        """A custom callback for processing recieved messages"""
        consumed_time = time.time()

        client_id = str(client._client_id.decode("utf-8"))
        data = str(message.payload.decode("utf-8"))

        data_json = json.loads(data)

        print(f"{client_id} recieved: {data[:100]} .. ")

        # log message as recieved
        with DBConnection() as conn:
            conn.log_message(data_json["timestamp"], json.dumps(data_json["road_name"]), json.dumps(data_json["segment"]), json.dumps(data_json["produced_time"]), consumed_time)
        

    for i in range(subscriber_count):
        topic_name = random.choice(topics)
        client = mqtt.Client(f"S{i}")
        client.connect(broker_address)
        client.on_message = on_message
        client.loop_start()
        subscribed_topics = []
        for i, topic_name in enumerate(random.choices(topics, k=max_subscribed_topics)):
            client.subscribe(topic_name)
            subscribed_topics.append(topic_name)
            print(f"S{i} subscribe to topic [{topic_name}]")
        subscribers[client] = subscribed_topics

    if randomized_unsubscribe:
        # Start randomized unsubscribtions and subscriptions
        print("Start randomized unsubscribtions and subscriptions")
        try:
            while True:
                time.sleep(5)
                # choose a random client 
                client, subscribed_topics = random.choice(list(subscribers.items()))
                
                # choose an action
                if random.randrange(10)%2 == 0:
                    # UNSUBSCRIBE from a random topic
                    try:
                        topic_name = random.choice(subscribed_topics)
                        client.unsubscribe(topic_name)
                        subscribed_topics.remove(topic_name)
                        subscribers[client] = subscribed_topics
                        print(f"{client._client_id.decode('utf-8')} unsubscribed from topic [{topic_name}]")
                    except:
                        pass
                else:
                    # SUBSCRIBE to a random topic
                    topic_name = random.choice(topics)
                    if topic_name not in subscribed_topics:
                        client.subscribe(topic_name)
                        subscribed_topics.append(topic_name)
                        subscribers[client] = subscribed_topics
                        print(f"{client._client_id.decode('utf-8')} subscribed to topic [{topic_name}]")
                    else:
                        print(f"{client._client_id.decode('utf-8')} not subscribing to duplicate topic [{topic_name}]")
        except KeyboardInterrupt:
            print("Stop randomized unsubscribtions and subscriptions")
            pass
    else:
        # keep loop open until keybord interrupt
        print("")
        try:
            while True:
                pass
        except KeyboardInterrupt:
            pass

    print("stop subscriber")
    for s in subscribers.keys:
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

    main(topics, SUBSCRIBER_AMOUNT, 3, True)
