FROM maven:3.9-amazoncorretto-21 as build
COPY . /work/
RUN cd /work; mvn package -DskipTests

FROM amazoncorretto:21
COPY --from=build /work/kaldb/target/kaldb.jar /
COPY --from=build /work/config/config.yaml /
ENTRYPOINT [ "java", "-jar", "./kaldb.jar", "config.yaml" ]
