# MQTT traffic data pub/sub demo

## starting the brokers

first broker
```
mosquitto
```

second (bridge messages here)
```
mosquitto -c bridge1.conf
```

## Starting 5 subscribers to random topics

```
python start_subscribers.py 5
```

## Start main producer and 100 publishers

```
python start_publishers.py 100
```
