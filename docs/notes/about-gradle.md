
# About dependency prefix : implementation, compileOnly , testCompileOnly, testAnnotationProcessor , testImplementation etc

| Configuration         | When is the dependency available?                                                    | Typical use case                                                                                                                                |
| --------------------- |--------------------------------------------------------------------------------------| ----------------------------------------------------------------------------------------------------------------------------------------------- |
| `implementation`      | ✅ Available during compilation of main code  , ✅ Included at runtime (app classpath) | Default dependency type. Use for libraries your code directly depends on (e.g., Spring, Flink connectors, Jackson).                             |
| `compileOnly`         | ✅ Available during compilation  , ❌ Not included in runtime classpath                | Dependencies required only to compile code but provided at runtime externally. Example: Lombok, or Flink core libraries in cluster deployments. |
| `annotationProcessor` | ✅ Used during compilation for annotation processing  , ❌ Not included in runtime     | Used by tools that generate code at compile time (e.g., Lombok, MapStruct, Dagger).                                                             |
| `runtimeOnly`         | ❌ Not available during compilation  ,v✅ Available only at runtime                    | Dependencies required only when running the application, not for compilation (e.g., JDBC drivers).                                              |


- One-line intuition
  - implementation → app needs it → Jackson Databind, Spring Boot Starter Web
  - compileOnly → compiler needs it → Lombok, Jakarta Servlet API
  - runtimeOnly → JVM needs it → MySQL Connector/J, PostgreSQL Driver
  - testImplementation → only tests need it → JUnit Jupiter, Mockito Core
  - annotationProcessor → build-time code generation → Lombok, MapStruct Processor