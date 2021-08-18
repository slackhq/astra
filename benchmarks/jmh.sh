echo "Building Project"
mvn clean package -DskipTests=true --file ../pom.xml

echo "JMH benchmarks start"

exec java -Dlog4j.configurationFile=./log4j2.xml -Xms4g -Xmx4g $gcArgs -jar ../benchmarks/target/benchmarks.jar "$@"

echo "JMH benchmarks done"
