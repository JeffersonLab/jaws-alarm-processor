# jaws-effective-processor [![CI](https://github.com/JeffersonLab/jaws-effective-processor/actions/workflows/ci.yaml/badge.svg)](https://github.com/JeffersonLab/jaws-effective-processor/actions/workflows/ci.yaml) [![Docker](https://img.shields.io/docker/v/jeffersonlab/jaws-effective-processor?sort=semver&label=DockerHub)](https://hub.docker.com/r/jeffersonlab/jaws-effective-processor)
A set of connected [Kafka Streams](https://kafka.apache.org/documentation/streams/) apps for [JAWS](https://github.com/JeffersonLab/jaws) that process alarm registration and notification data and compute effective state.  

Read more about [Overrides and Effective State](https://github.com/JeffersonLab/jaws/wiki/Overrides-and-Effective-State) and [Software Design](https://github.com/JeffersonLab/jaws/wiki/Software-Design#effective-processor).

---
 - [Quick Start with Compose](https://github.com/JeffersonLab/jaws-effective-processor#quick-start-with-compose)
 - [Install](https://github.com/JeffersonLab/jaws-effective-processor#install) 
 - [Configure](https://github.com/JeffersonLab/jaws-effective-processor#configure)
 - [Build](https://github.com/JeffersonLab/jaws-effective-processor#build)
 - [Develop](https://github.com/JeffersonLab/jaws-effective-processor#develop)  
 - [Release](https://github.com/JeffersonLab/jaws-effective-processor#release)  
 - [See Also](https://github.com/JeffersonLab/jaws-effective-processor#see-also)
 ---

## Quick Start with Compose 
1. Grab project
```
git clone https://github.com/JeffersonLab/jaws-effective-processor
cd jaws-effective-processor
```
2. Launch [Compose](https://github.com/docker/compose)
```
docker compose up
```
3. Monitor for expiration tombstone message 
```
docker exec -it jaws list_overrides --monitor 
```
4. Shelve an alarm for 5 seconds
```
docker exec jaws set_override --override Shelved alarm1 --reason Other --expirationseconds 5
```

**See**: More [Usage Examples](https://github.com/JeffersonLab/jaws-effective-processor/wiki/Usage-Examples)

## Install
This application requires a Java 11+ JVM and standard library to run.

Download from [Releases](https://github.com/JeffersonLab/jaws-effective-processor/releases) or [build](https://github.com/JeffersonLab/jaws-effective-processor#build) the [distribution](https://github.com/JeffersonLab/jaws-effective-processor#release) yourself.


Launch with:

UNIX:
```
bin/jaws-effective-processor
```
Windows:
```
bin/jaws-effective-processor.bat
```

## Configure
Environment Variables

| Name              | Description                                                                                                                                                                                                                                                                                                         |
|-------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| BOOTSTRAP_SERVERS | Comma-separated list of host and port pairs pointing to a Kafka server to bootstrap the client connection to a Kafka Cluser; example: `kafka:9092`                                                                                                                                                                  |
| SCHEMA_REGISTRY   | URL to Confluent Schema Registry; example: `http://registry:8081`                                                                                                                                                                                                                                                   |
| STATE_DIR         | Directory where local Kafka Streams state is stored [[1](https://kafka.apache.org/documentation/#streamsconfigs_state.dir)], [[2](https://kafka.apache.org//documentation/streams/developer-guide/app-reset-tool)].  Defaults to `java.io.tmp` system property value with an appended subdir named `kafka-streams`. |

## Build
This project is built with [Java 17](https://adoptium.net/) (compiled to Java 11 bytecode), and uses the [Gradle 7](https://gradle.org/) build tool to automatically download dependencies and build the project from source:

```
git clone https://github.com/JeffersonLab/jaws-effective-processor
cd jaws-effective-processor
gradlew build
```

**Note**: If you do not already have Gradle installed, it will be installed automatically by the wrapper script included in the source

**Note for JLab On-Site Users**: Jefferson Lab has an intercepting [proxy](https://gist.github.com/slominskir/92c25a033db93a90184a5994e71d0b78)

**See**: [Docker Development Quick Reference](https://gist.github.com/slominskir/a7da801e8259f5974c978f9c3091d52c#development-quick-reference)

## Develop
In order to iterate rapidly when making changes it's often useful to run the app directly on the local workstation, perhaps leveraging an IDE. In this scenario run the service dependencies with:
```
docker compose -f deps.yaml up
```
Then run the app with:
```
gradlew run
```

**Note**: The `STATE_DIR` config is set to the gradle `build` dir such that running a `clean` task will clear the local state.  You may need to reset the Kafka server state after running a clean task by restarting Kafka from scratch else using the [Reset Tool](https://kafka.apache.org//documentation/streams/developer-guide/app-reset-tool). 

**Note**: Javadocs can be generated with the command:
```
gradlew javadoc
```

## Release
1. Bump the version number in the VERSION file and commit and push to GitHub (using [Semantic Versioning](https://semver.org/)).
2. The [CD](https://github.com/JeffersonLab/jaws-effective-processor/blob/main/.github/workflows/cd.yaml) GitHub Action should run automatically invoking:
    - The [Create release](https://github.com/JeffersonLab/java-workflows/blob/main/.github/workflows/gh-release.yaml) GitHub Action to tag the source and create release notes summarizing any pull requests.   Edit the release notes to add any missing details.  A zip file artifact is attached to the release.
    - The [Publish docker image](https://github.com/JeffersonLab/container-workflows/blob/main/.github/workflows/docker-publish.yaml) GitHub Action to create a new demo Docker image.

## See Also
   - [Developer Notes](https://github.com/JeffersonLab/jaws-effective-processor/wiki/Developer-Notes)
