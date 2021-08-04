Kaldb benchmarks
---------------

Add JMH Dependencies

To run it we can use:

Compile code and benchmarks - 
```
mvn package -DskipTests=true
```

Run all benchmarks - 
```
java -jar benchmarks/target/benchmarks.jar
```

Format - 
```
mvn fmt:format -f benchmarks/pom.xml
```

To run it from IntelliJ directly https://plugins.jetbrains.com/plugin/7529-jmh-java-microbenchmark-harness

To find all the JMH supported options use the `-h` flag
```
java -jar benchmarks/target/benchmarks.jar -h
```

For example to run a benchmark with async profiler

```
java -jar benchmarks/target/benchmarks.jar -prof "async:libPath=/Users/vthacker/async-profiler-2.0-macos-x64/build/libasyncProfiler.so;dir=benchmarks/jmh-output/async-profiler/;output=flamegraph;direction=forward"
```

