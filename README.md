# shelved-timer [![Java CI with Gradle](https://github.com/JeffersonLab/shelved-timer/workflows/Java%20CI%20with%20Gradle/badge.svg)](https://github.com/JeffersonLab/shelved-timer/actions?query=workflow%3A%22Java+CI+with+Gradle%22) [![Docker Image Version (latest semver)](https://img.shields.io/docker/v/slominskir/shelved-timer?sort=semver)](https://hub.docker.com/r/slominskir/shelved-timer)
A [Kafka Streams](https://kafka.apache.org/documentation/streams/) application to expire shelved alarms in the [kafka-alarm-system](https://github.com/JeffersonLab/kafka-alarm-system). The shelved-timer app expires shelved messages with tombstone records to notify clients that the shelved alarm duration is over.   This moves the burden of managing expiration timers off of every client and onto a single app.

---
 - [Quick Start with Compose](https://github.com/JeffersonLab/shelved-timer#quick-start-with-compose)
 - [Build](https://github.com/JeffersonLab/shelved-timer#build)
 - [Configure](https://github.com/JeffersonLab/shelved-timer#configure)
 - [Deploy](https://github.com/JeffersonLab/shelved-timer#deploy)
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
3. Monitor for expiration tombstone message 
```
docker exec -it console /scripts/list-shelved.py --monitor 
```
4. Shelve an alarm for 5 seconds
```
docker exec -it console /scripts/set-shelved.py channel1 --reason "We are testing this alarm" --expirationseconds 5
```

**Note**: When developing the app you can mount the build artifact into the container by substituting the `docker-compose up` command with:
```
docker-compose -f docker-compose.yml -f docker-compose-dev.yml up
```

## Build
This [Java 11](https://adoptopenjdk.net/) project uses the [Gradle 6](https://gradle.org/) build tool to automatically download dependencies and build the project from source:

```
git clone https://github.com/JeffersonLab/shelved-timer
cd shelved-timer
gradlew build
```
**Note**: If you do not already have Gradle installed, it will be installed automatically by the wrapper script included in the source

**Note**: Jefferson Lab has an intercepting [proxy](https://gist.github.com/slominskir/92c25a033db93a90184a5994e71d0b78)

## Configure
Environment Variables

| Name | Description |
|---|---|
| BOOTSTRAP_SERVERS | Comma-separated list of host and port pairs pointing to a Kafka server to bootstrap the client connection to a Kafka Cluser; example: `kafka:9092` |
| SCHEMA_REGISTRY | URL to Confluent Schema Registry; example: `http://registry:8081` |

## Deploy
The Kafka Streams app is a regular Java application, and start scripts are created and dependencies collected by the Gradle distribution targets:

```
gradlew assembleDist
```

[Releases](https://github.com/JeffersonLab/shelved-timer/releases)

Launch with:

UNIX:
```
bin/shelved-timer
```
Windows:
```
bin/shelved-timer.bat
```

## Docker
```
docker pull slominskir/shelved-timer
```
Image hosted on [DockerHub](https://hub.docker.com/r/slominskir/shelved-timer)

## See Also
   - [Developer Notes](https://github.com/JeffersonLab/shelved-timer/wiki/Developer-Notes)
