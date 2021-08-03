Kaldb benchmarks
---------------

Add JMH Dependencies

To run it we can use:

Compile - `mvn package -DskipTests=true -f benchmarks/pom.xml`

Run - `java -jar benchmarks/target/benchmarks.jar IndexingBenchmark`

Format - `mvn fmt:format -f benchmarks/pom.xml`

To run it from IntelliJ directly https://plugins.jetbrains.com/plugin/7529-jmh-java-microbenchmark-harness


