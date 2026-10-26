FROM maven:3-amazoncorretto-21 as build
COPY . /work/
RUN cd /work; mvn package -DskipTests

FROM eclipse-temurin:25-jre-noble
ARG CA_CERTIFICATES_REFRESH=2026-10-21
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
 && dpkg --compare-versions "$(dpkg-query -W -f='${Version}' ca-certificates)" ge 20260816ubuntu0.24.04.1 \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /work/astra/target/astra.jar /
COPY --from=build /work/config/config.yaml /
ENTRYPOINT [ "java", "--enable-preview", "-jar", "./astra.jar", "config.yaml" ]
