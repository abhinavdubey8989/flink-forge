# Containerized setup : Apache-Flink

## Aim
Build and experiment with real-time stream processing applications using Apache Flink, Kafka, Docker, and observability tools while exploring stateful processing, checkpointing, scalability, and performance tuning.

## Tech Stack
| Technology | Version |
|---|---|
| Apache Flink | 2.2 |
| Java | 21 |


## Directory Structure


## UI links (if applicable)


## Docker image(s) reference


## Other References
- [Spring initializr](https://start.spring.io/)


## Useful Commands
- Assuming the host machine has multiple java version managed using `jenv`, below are the `jenv` commands

```
# 1. Check jev version
jenv --version

# 2. get list of java version on host machine
jenv versions

# 3. switch to a particular java version
jenv local 21

# 4. Check java version for this project
java --version
```


- Adding new dependency
```
- Step-1 : Mention/add the dependency in build.gradle
- Step-2 : Download the dependencies using : `./gradlew dependencies`
```


- Build the JAR after removing existing builds
```
./gradlew clean build
```
