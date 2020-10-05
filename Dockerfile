FROM gradle:6.6-jdk11 AS build
ARG release_version

COPY ./ .
RUN gradle --no-daemon clean build dockerPrepare

FROM openjdk:12-alpine
ENV RABBITMQ_HOST=rabbitmq \
    RABBITMQ_PORT=5672 \
    RABBITMQ_USER=guest \
    RABBITMQ_PASS=guest \
    RABBITMQ_VHOST=th2 \
    GRPC_PORT=8080 \
    DISABLE_GRPC=false
WORKDIR /home
COPY --from=build /home/gradle/build/docker .
ENTRYPOINT ["/home/service/bin/service", "/home/sailfish/workspace", "service.xml"]
