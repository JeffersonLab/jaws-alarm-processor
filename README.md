# jaws-effective-processor [![CI](https://github.com/JeffersonLab/jaws-effective-processor/actions/workflows/ci.yml/badge.svg)](https://github.com/JeffersonLab/jaws-effective-processor/actions/workflows/ci.yml) [![Docker](https://img.shields.io/docker/v/jeffersonlab/jaws-effective-processor?sort=semver&label=DockerHub)](https://hub.docker.com/r/jeffersonlab/jaws-effective-processor)
A set of connected [Kafka Streams](https://kafka.apache.org/documentation/streams/) apps for [JAWS](https://github.com/JeffersonLab/jaws) that process alarm registration and notification data and compute effective state.  

Read more about [Overrides and Effective State](https://github.com/JeffersonLab/jaws/wiki/Overrides-and-Effective-State) and [Software Design](https://github.com/JeffersonLab/jaws/wiki/Software-Design#effective-processor).

---
 - [Quick Start with Compose](https://github.com/JeffersonLab/jaws-effective-processor#quick-start-with-compose)
 - [Install](https://github.com/JeffersonLab/jaws-effective-processor#install) 
 - [Configure](https://github.com/JeffersonLab/jaws-effective-processor#configure)
 - [Build](https://github.com/JeffersonLab/jaws-effective-processor#build) 
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
docker exec -it jaws-libp list_overrides --monitor 
```
4. Shelve an alarm for 5 seconds
```
docker exec -it jaws-libp set_override --override Shelved alarm1 --reason Other --expirationseconds 5
```
**See**: [Docker Compose Strategy](https://gist.github.com/slominskir/a7da801e8259f5974c978f9c3091d52c)

**See**: More [Usage Examples](https://github.com/JeffersonLab/jaws/wiki/Usage-Examples)

## Install
This application requires a Java 11+ JVM and standard library to run.

Download from [Releases](https://github.com/JeffersonLab/jaws-effective-processor/releases) or [build](https://github.com/JeffersonLab/jaws-effective-processor#build) yourself.

Start scripts are created and dependencies collected by the Gradle distribution target:
```
gradlew assembleDist
```

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

| Name | Description |
|---|---|
| BOOTSTRAP_SERVERS | Comma-separated list of host and port pairs pointing to a Kafka server to bootstrap the client connection to a Kafka Cluser; example: `kafka:9092` |
| SCHEMA_REGISTRY | URL to Confluent Schema Registry; example: `http://registry:8081` |

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

## Release
1. Bump the version number in build.gradle and commit and push to GitHub (using [Semantic Versioning](https://semver.org/)).   
1. Create a new release on the GitHub [Releases](https://github.com/JeffersonLab/jaws-effective-processor/releases) page corresponding to same version in build.gradle (Enumerate changes and link issues).   Run dist build and attach zip to release.
1. [Publish to DockerHub](https://github.com/JeffersonLab/jaws-effective-processor/actions/workflows/docker-publish.yml) GitHub Action should run automatically.
1. Bump and commit quick start [image version](https://github.com/JeffersonLab/jaws-effective-processor/blob/main/docker-compose.override.yml) after confirming new image works

## See Also
   - [Developer Notes](https://github.com/JeffersonLab/jaws-effective-processor/wiki/Developer-Notes)
