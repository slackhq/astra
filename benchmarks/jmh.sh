gcArgs="-XX:+UseG1GC -Xms4g -Xmx4g -XX:MaxGCPauseMillis=10"
loggingArgs="-Dlog4j.configurationFile=./log4j2.xml"

echo "Building Project"
mvn clean package -DskipTests=true --file ../pom.xml

echo "JMH benchmarks start"

exec java $loggingArgs $gcArgs -jar ../benchmarks/target/benchmarks.jar "$@"

echo "JMH benchmarks done"
