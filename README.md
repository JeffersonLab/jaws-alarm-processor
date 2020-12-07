# shelved-timer [![Build Status](https://travis-ci.com/JeffersonLab/shelved-timer.svg?branch=master)](https://travis-ci.com/JeffersonLab/shelved-timer)
A [Kafka Streams](https://kafka.apache.org/documentation/streams/) application to expire shelved alarms in the [kafka-alarm-system](https://github.com/JeffersonLab/kafka-alarm-system).

---
 - [Quick Start with Compose](https://github.com/JeffersonLab/shelved-timer#quick-start-with-compose)
 - [Build](https://github.com/JeffersonLab/shelved-timer#build)
 - [Docker](https://github.com/JeffersonLab/shelved-timer#docker)
 - [See Also](https://github.com/JeffersonLab/shelved-timer#see-also)
 ---

## Quick Start with Compose 
1. Grab project
```
git clone https://github.com/JeffersonLab/shelved-timer
cd shelved-timer
```
2. Launch Docker
```
docker-compose up
```
3. Shelve an alarm for 5 seconds
```
docker exec -it console /scripts/shelved-alarms/set-shelved.py channel1 
```
4. Verify that the expiration tombstone message is received 
```
docker exec -it console /scripts/shelved-alarms/list-shelved.py 
```

## Build
```
gradlew build
```

## Docker
```
docker pull slominskir/shelved-timer
```
Image hosted on [DockerHub](https://hub.docker.com/r/slominskir/shelved-timer)

## See Also
   - [Developer Notes](https://github.com/JeffersonLab/shelved-timer/wiki/Developer-Notes)
