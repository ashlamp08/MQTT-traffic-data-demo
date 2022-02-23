from os import walk

import paho.mqtt.client as mqtt

import multiprocessing as mp
import threading

import json
import datetime
import time


def start_producer(q):
    # read files to list
    f = []
    for (dirpath, dirnames, filenames) in walk("./sample_data"):
        f.extend(filenames)

    # sort files
    f.sort()

    # add all timestamps to queue
    for filename in f:

        timestamp = int(filename.replace(".json", ""))
        date_and_time = datetime.datetime.fromtimestamp(timestamp)

        print(f"Producer: queue all data from [{date_and_time}]")
        with open(f"./sample_data/{filename}", "r") as f:
            data = json.loads(f.read())
            for road in data:
                # put all timestamps data into queue
                q.put(road)
        time.sleep(10)


def process_queue_items(p, q, client):
    """Process items from the main queue

    Args:
        p (_type_): Thread id
        q (_type_): The main queue
        client (_type_): A paho mqtt client
    """

    print(p, "starting")
    while True:
        message = str(q.get())
        print(f"{p} -> {message[:100]} ..")
        client.publish("topic1", message)

    print(p, "done")


def start_publishers(q, n: int):

    # start
    T = []  # for holding the threads
    for i in range(n):
        # create client for the thread
        client = mqtt.Client(f"P{i}")
        client.connect("0.0.0.0")

        t = threading.Thread(
            target=process_queue_items,
            args=(
                f"T{i}",
                q,
                client,
            ),
        )  # construct the thread
        T.append(t)  # add thread reference to list for join later
        t.start()  # start the thread

    [t.join() for t in T]  # wait for all threads to terminate


if __name__ == "__main__":
    PUBLISHER_AMOUNT = 10

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
