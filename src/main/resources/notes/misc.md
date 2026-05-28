

# If host machine has Flink projects which need different java & flink version, how will intellij know which java & flink version to use? 

- IntelliJ does NOT use FLINK_HOME to decide your project’s Flink version.
- Instead, IntelliJ gets the version (of java & flink) from your Gradle build configuration ie build.gradle
- ie FLINK_HOME does NOT matter, it matters only when:
  - running Flink cluster locally
  - using flink run
  - starting JobManager/TaskManager


- How to verify Flink version in IntelliJ
  - Open: External Libraries
  - You should see: flink-streaming-java-2.2.0.jar


- How to verify Java version in IntelliJ
  - Gradle Settings → Gradle JVM
  - Should show Java 21