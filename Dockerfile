FROM maven:3-amazoncorretto-21 as build
COPY . /work/
RUN cd /work; mvn package -DskipTests

FROM amazoncorretto:21
COPY --from=build /work/astra/target/astra.jar /
COPY --from=build /work/config/config.yaml /
ENTRYPOINT [ "java", "--enable-preview", "-jar", "./astra.jar", "config.yaml" ]
