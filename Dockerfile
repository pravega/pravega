FROM java:8

MAINTAINER Arvind Kandhare [arvind.kandhare@emc.com]

COPY . /opt/streaming/
RUN mkdir -p /root/.m2/repository/com/twitter/
RUN mkdir -p /root/.m2/repository/org/apache/thrift/
WORKDIR /opt/streaming/
RUN mv ./temp_repo/twitter /root/.m2/repository/com/
RUN mv ./temp_repo/thrift /root/.m2/repository/org/apache/
RUN mv ./temp_repo/bookkeeper /root/.m2/repository/org/apache/

RUN  ./gradlew jar

ENTRYPOINT [ "/opt/streaming/gradlew","startServer" ]
