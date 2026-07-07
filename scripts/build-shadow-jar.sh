#!/bin/bash

# ============================================================================
# [Aim]
# To create JAR file which is to be submitted to flink cluster
#
# [Assumption]
# none
#
# [Usage]
#  - chmod +x <script>.sh
#  - ./<script>.sh
#
# [What this script does]
#   - Create one single JAR file
#   - This JAR file has the logic for all flink pipelines
#   - Each flink pipeline has separate trigger point (ie. fully qualified class name having the main method)
#   - This trigger pt. needs to be specified when submitting the JAR to flink cluster (ie. when running `flink run` CLI command)
#   - Possible-optimization : JAR should only have the code related to building 1 pipeline, ie. remove the code not needed
#
# ============================================================================


# Exit immediately if any command fails
# Prevents partial/broken configuration
set -euo pipefail


# Read common env variables (like $PROJECT_DIR etc.)
source "./common.env"


main(){

  # goto project root
  cd $PROJECT_DIR

  # Remove existing builds
  ./gradlew clean

  # explicitly removing the build dir
  rm -rf $PROJECT_DIR/build/*

  # Build JAR (skips tests to make it faster)
  # The JAR file can be found at $JAR_FILE_DIR dir
  ./gradlew shadowJar -x test

  echo "JAR built successfully !!"
}


# Call the main function with all arguments
main "$@"