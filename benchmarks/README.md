Kaldb benchmarks
---------------

Add JMH Dependencies

To run it we can use:

Run All Benchmark
```
./jmh.sh
```

Run a single benchmark Benchmark
```
./jmh.sh IndexingBenchmark
```

To run it from IntelliJ directly https://plugins.jetbrains.com/plugin/7529-jmh-java-microbenchmark-harness

To find all the JMH supported options use the `-h` flag
```
./jmh.sh -h
```

For example to run a benchmark with async profiler

```
./jmh.sh -prof "async:libPath=/Users/vthacker/async-profiler-2.0-macos-x64/build/libasyncProfiler.so;dir=jmh-output/async-profiler/;output=flamegraph;direction=forward"
```

Format Code Before Committing -
```
mvn fmt:format -f benchmarks/pom.xml
```