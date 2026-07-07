# Flink Forge — Distributed Stream Processing Platform

## Aim
Build and experiment with real-time stream processing applications using Apache Flink, Kafka, Docker, and observability tools while exploring stateful processing, checkpointing, scalability, and performance tuning.

## Tech Stack
| Technology     | Version |
|----------------|---|
| Apache Flink   | 2.2 |
| Java           | 21 |
| Spring version | 4.0.6 |


## Directory Structure

```text
docs
└── images
scripts
└── *.sh
└── python/
src
└── main
    ├── java
    │   └── com.flink_forge
    │       ├── common
    │       │   ├── config
    │       │   │   ├── ConfigUtil
    │       │   │   └── KafkaDetailsFactory
    │       │   └── dto.internal
    │       │       └── KafkaDetails
    │       │
    │       ├── simple_flink_pipeline
    │       │   └── SimpleFlinkPipelineMain
    │       │
    │       └── FlinkForgeApplication
    │
    └── resources
        ├── notes
        ├── application-local.properties
        ├── application-docker.properties
        └── application-example.properties
```

- The core business logic is inside `src/main/java/com.flink_forge`
  - There can be multiple flink pipelines/DAG, eg : `src/main/java/com.flink_forge/simple_flink_pipeline` has all business logic for one such pipeline/DAG
  - `src/main/java/com.flink_forge/common` contains the DTO/utils used across all the flink pipelines/DAG
- `src/main/resources` has the application.properties file based on the environment
  - eg: `application-local.properties` when running as Java app & not submitting the job to flink cluster
  - `application-docker.properties` when the JAR is submitted to a containerised flink cluster
- Images & other miscellaneous files are present under `docs`
- `scripts` dir has shell scripts to build & submit JAR (document & sample usage inside each script)
  - It also has some python scripts to mock data


## Other References
- [Spring initializr](https://start.spring.io/)
- [Apache Flink](https://nightlies.apache.org/flink/flink-docs-stable/)
- [Dockerized Flink setup](https://github.com/abhinavdubey8989/docker-lab/tree/main/flink)


## Useful Commands
- Assuming the host machine has multiple java version managed using `jenv`, below are the `jenv` commands

```
# 1. Check jevn version (this is NOT the java version)
jenv --version

# 2. get list of java version on host machine
jenv versions

# 3. switch to a particular java version
jenv local 21

# 4. Check java version for this project (should be java 21.xx)
java --version
```


- Adding new dependency
```
- Step-1 : Mention/add the dependency in build.gradle
- Step-2 : Download the dependencies using : `./gradlew dependencies`
```


- Run in local mode as java application (for debugging), ie not submit JAR to flink-cluster
```
# Note : The class having the main() need to be correctly specifier in build.gradle
# Please refer the `application` in build.gradle
# In the below command, APP_ENV=local help to recognise which application.properties file to be picked
# With APP_ENV=local, the file which gets picked would be : application-local.properties

APP_ENV=local ./gradlew run
```


- Build the JAR
```
# After running this, the JAR will be present at : ./build/libs/<name>.jar
# This JAR can be submitted to Flink cluster
cd scripts
./build-shadow-jar.sh
```


- Submit the JAR to flink cluster
```
# After preparing the JAR, submit to flink-cluster
cd scripts

./submit-jar.sh 
    --job-package=com.flink_forge.simple_flink_pipeline \
    --job-class=SimpleFlinkPipelineMain
```
