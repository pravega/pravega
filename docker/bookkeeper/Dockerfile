#
# Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
FROM openjdk:8-jre-alpine

MAINTAINER Pravega Team

RUN apk add --update \
    bash \
    wget \
    unzip \
    && rm -rf /var/cache/apk/*

ARG BOOKKEEPER_VERSION=4.7.3
ARG ZOOKEEPER_VERSION=3.5.4-beta

ENV bookiePort=3181
EXPOSE $bookiePort

RUN mkdir -p /opt \
    && cd /opt \
    && wget -q https://archive.apache.org/dist/bookkeeper/bookkeeper-${BOOKKEEPER_VERSION}/bookkeeper-server-${BOOKKEEPER_VERSION}-bin.tar.gz \
    && tar xvzf bookkeeper-server-${BOOKKEEPER_VERSION}-bin.tar.gz \
    && mv bookkeeper-server-${BOOKKEEPER_VERSION} /opt/bookkeeper \
    && rm -rf bookkeeper-server-${BOOKKEEPER_VERSION}-bin.tar.gz \
    && wget -q https://archive.apache.org/dist/zookeeper/zookeeper-${ZOOKEEPER_VERSION}/zookeeper-${ZOOKEEPER_VERSION}.tar.gz \
    && tar xvf zookeeper-${ZOOKEEPER_VERSION}.tar.gz \
    && mv zookeeper-${ZOOKEEPER_VERSION} /opt/zookeeper \
    && rm zookeeper-${ZOOKEEPER_VERSION}.tar.gz

WORKDIR /opt/bookkeeper

COPY entrypoint.sh /opt/bookkeeper/entrypoint.sh

ENTRYPOINT [ "/bin/bash", "/opt/bookkeeper/entrypoint.sh" ]
