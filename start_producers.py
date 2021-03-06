from dataclasses import is_dataclass
from os import walk
import sys

import paho.mqtt.client as mqtt

import multiprocessing as mp
import threading

import json
import datetime
import time


def start_producer(q):
    """Start the event (traffic data) producer

    Args:
        q (_type_): Reference to the main queue
    """

    # read files to list
    f = []
    for (dirpath, dirnames, filenames) in walk("./sample_data"):
        f.extend(filenames)

    # sort files (so timestamps in correct order)
    f.sort()

    # loop all filenames
    for filename in f:

        # extract timestamp, datetime from filename
        timestamp = int(filename.replace(".json", ""))
        date_and_time = datetime.datetime.fromtimestamp(timestamp)

        print(f"Producer: queue all data from [{date_and_time}]")

        # open actual file, and queue each roads data
        with open(f"./sample_data/{filename}", "r") as f:
            data = json.loads(f.read())
            for road_data in data:
                road_name = road_data["road_name"]
                for segment in road_data["segments"]:
                    # construct a payload to pass to publishers
                    topic = f"{road_name}/{segment['cross_road']}"
                    payload = {
                        "topic": topic,
                        "data": {
                            "timestamp": timestamp,
                            "road_name": road_name,
                            "segment": segment,
                        },
                    }
                    q.put(payload)

        # TODO: assert 50 second passing
        # simulate passing of time
        time.sleep(10)


def process_queue_items(p, q, client):
    """Process items from the main queue

    Args:
        p (_type_): Thread id
        q (_type_): The main queue
        client (_type_): A paho mqtt client
    """

    print(p, "starting")
    # keep processing data from the queue when available

    while True:
        payload = q.get()
        topic = payload["topic"]
        # log the time when the message is produced
        payload["data"]["produced_time"] = time.time()
        
        data = json.dumps(payload["data"])
        # print(f"{p} -> {topic} -> {data[:20]}")
        client.publish(topic, data)

def start_publishers(q, n: int):
    """Start the publishers (consumers) of traffic data

    Args:
        q (_type_): The main queue
        n (int): The amount of publishers to start
    """

    T = []  # for holding the threads
    # start n threads, add a client for each
    for i in range(n):

        # create client for the thread
        client = mqtt.Client(f"P{i}")
        client.connect("0.0.0.0")

        # construct the thread
        t = threading.Thread(
            target=process_queue_items,
            args=(
                f"T{i}",
                q,
                client,
            ),
        )
        T.append(t)  # add thread reference to list for join later
        t.start()  # start the thread

    [t.join() for t in T]  # wait for all threads to terminate


if __name__ == "__main__":

    PUBLISHER_AMOUNT = int(sys.argv[1])

    ctx = mp.get_context("spawn")
    q = ctx.Queue()

    producer = ctx.Process(
        target=start_producer,
        args=(q,),
    )
    publishers = ctx.Process(
        target=start_publishers,
        args=(
            q,
            PUBLISHER_AMOUNT,
        ),
    )

    producer.start()
    publishers.start()
    producer.join()
    publishers.join()
