# Metrics docs generation

The included `metrics_gen_util.java` utility was used for the initial metrics documentation generation. This utility
file reads in the output of a Prometheus metrics export and generate a partial stub of Writerside definitions.

To generate definitions that apply across multiple services you can combine all source export into the same file, and
this util will deduplicate and combine as needed. When documentation is available via the definitions that is used 
in the corresponding output. 

The script was modified to generate each major section based on metric name prefix, corresponding as shown below: 
* Kafka metrics: `kafka`
* JVM metrics: `jvm`
* Armeria metrics: `armeria`
* GRPC metrics: `grpc`
* Processor metrics: `system` and `process`
* Astra: everything not matching one of the above
