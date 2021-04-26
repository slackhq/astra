FROM maven:3.6.3-adoptopenjdk-11 as build
COPY . /work/
RUN cd /work; mvn package

FROM openjdk:11.0.1
COPY --from=build /work/target/kaldb.jar /
ENTRYPOINT [ "java", "-jar", "./kaldb.jar" ]
