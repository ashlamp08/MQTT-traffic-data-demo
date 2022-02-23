import paho.mqtt.client as mqtt
from db_connection import DBConnection
from typing import List


broker_address = "0.0.0.0"
subscriber_amount = 1
subscribers: List[mqtt.Client] = []


def on_message(client: mqtt.Client, userdata, message):
    """A custom callback for processing recieved messages"""

    client_id = str(client._client_id.decode("utf-8"))
    message = str(message.payload.decode("utf-8"))

    print(f"{client_id} recieved: {message[:100]} .. ")

    # log message as recieved
    """
    with DBConnection() as conn:
        conn.log_message(int(message), 0, 1)
    """


print(f"Create, connect, subscribe {subscriber_amount} subscribers")
for i in range(subscriber_amount):

    client = mqtt.Client(f"S{i}")
    client.connect(broker_address)
    client.loop_start()
    client.subscribe("topic1")
    client.on_message = on_message
    subscribers.append(client)


# keep loop open until keybord interrupt
try:
    while True:
        pass
except KeyboardInterrupt:
    pass

print("stop subscribers")
for s in subscribers:
    s.loop_stop()
