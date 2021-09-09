Kaldb benchmarks
---------------

Add JMH Dependencies

To run it we can use:

Run All Benchmark
```
./jmh.sh
```

Run a single benchmark
```
# All benchmarks within IndexingBenchmark
./jmh.sh IndexingBenchmark

# Only one benchmark within IndexingBenchmark
./jmh.sh IndexingBenchmark.measureIndexingAsKafkaSerializedDocument
```

Run API LOG benchmark
```
./jmh.sh IndexAPILog
```
NOTE: If you're running the benchmark locally make sure `kaldb/benchmarks/api_logs.txt` is present. Else you can run using `java -Djmh.api.log.file=/path/to/file.txt -Xms4g -Xmx4g benchmarks/target/benchmarks.jar IndexAPILog`

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