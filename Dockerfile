FROM java:8

MAINTAINER Arvind Kandhare [arvind.kandhare@emc.com]

COPY . /opt/streaming/
WORKDIR /opt/streaming/
RUN  ./gradlew jar

ENTRYPOINT [ "/opt/streaming/gradlew","startServer" ]
