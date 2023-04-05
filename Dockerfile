FROM maven:3.8.3-adoptopenjdk-11 as build
COPY . /work/
RUN cd /work; mvn package -DskipTests

FROM amazoncorretto:11
COPY --from=build /work/kaldb/target/kaldb.jar /
COPY --from=build /work/config/config.yaml /
ENTRYPOINT [ "java", "-jar", "./kaldb.jar", "config.yaml" ]
