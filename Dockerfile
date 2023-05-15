FROM maven:3.9-amazoncorretto-17 as build
COPY . /work/
RUN cd /work; mvn package -DskipTests

FROM amazoncorretto:17
COPY --from=build /work/kaldb/target/kaldb.jar /
COPY --from=build /work/config/config.yaml /
ENTRYPOINT [ "java", "-jar", "./kaldb.jar", "config.yaml" ]
