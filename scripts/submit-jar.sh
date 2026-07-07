#!/bin/bash

# ============================================================================
#
# [Aim]
# This script submits a Flink job to the configured Flink cluster.
#
#
# [Assumption]
# - Dockerized Flink-cluster is up and running
# - JAR file has been built
#
#
# [Arguments]
#
# --job-package=<package_name>
# Java package containing the class, this class has the main method.
#
# --job-class=<class_name>
# Class name (inside the above package) where the main method exists.
#
#
# [Examples]
#
# ./<script>.sh \
#     --job-package=com.flink_forge.simple_flink_pipeline \
#     --job-class=SimpleFlinkPipelineMain
#
#
# ============================================================================


# Exit immediately if any command fails
# Prevents partial/broken configuration
set -euo pipefail


# Read common env variables (like $PROJECT_DIR etc.)
source "./common.env"


parse_args() {
  # parse & set the global vars in the script
    for arg in "$@"; do
        case $arg in
            --job-package=*)
                GL_JOB_PACKAGE="${arg#*=}"
                ;;
            --job-class=*)
                GL_JOB_CLASS="${arg#*=}"
                ;;
            *)
                echo "Unknown argument: $arg"
                exit 1
                ;;
        esac
    done
}


validate_args() {
    if [ -z "$GL_JOB_PACKAGE" ]; then
        echo "JOB_PACKAGE is required"
        exit 1
    fi

    if [ -z "$GL_JOB_CLASS" ]; then
        echo "JOB_CLASS is required"
        exit 1
    fi
}


copy_jar(){
  # Copies JAR from the host-machine dir-1 to host-machine dir-1
  # dir-1 is the directory where the JAR is available after building
  # dir-2 is the bind-mount dir on host-machine for flink job-manager (JM) & task-manager (TM) docker containers
  # By running this single command, the JAR will be copied inside all the JM & TM containers
  # Another approach would be to run "docker cp" & copy JAR to all TM & JM, but that will need to run multiple CLI commands
  cp "$HOST_JAR_FILE_DIR/$JAR_FILE_NAME" "$HOST_FLINK_BIND_MOUNT_DIR/"
}


submit_job() {
  # Submit the flink job to the dockerized flink-cluster
  # This is done by running "flink run" (via "docker exec" on host machine)
  local FULL_CLASS_NAME="${GL_JOB_PACKAGE}.${GL_JOB_CLASS}"

  echo "Submitting JAR..."
  echo "Fully qualified class-name=[$FULL_CLASS_NAME]"

  docker exec -it $FLINK_JM_CONTAINER_NAME \
    flink run \
    -d "$FULL_CLASS_NAME" \
    "/opt/flink/job-jars/$JAR_FILE_NAME.jar"
}


main() {

    # Populate global variables
    parse_args "$@"

    # Validate required arguments
    validate_args

    # Copy JAR into flink-docker setup
    copy_jar

    # Submit the Flink job
    submit_job
}

main "$@"