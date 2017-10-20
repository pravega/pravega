#
# Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#

FROM java:8
MAINTAINER Arvind Kandhare [arvind.kandhare@emc.com]

RUN apt-get update \
&&  apt-get install -y wget unzip \
&&  mkdir -p /opt \
&&  cd /opt

ENV BOOKKEEPER_VERSION=4.5.0 \
    ZOOKEEPER_VERSION=3.5.3-beta

RUN wget http://www.apache.org/dist/bookkeeper/bookkeeper-$BOOKKEEPER_VERSION/bookkeeper-server-$BOOKKEEPER_VERSION-bin.tar.gz \
&&  tar xvzf  bookkeeper-server-$BOOKKEEPER_VERSION-bin.tar.gz \
&&  mv bookkeeper-server-$BOOKKEEPER_VERSION /opt/bk_all

RUN wget http://www.apache.org/dist/zookeeper/zookeeper-$ZOOKEEPER_VERSION/zookeeper-$ZOOKEEPER_VERSION.tar.gz \
&&  tar xvzf zookeeper-$ZOOKEEPER_VERSION.tar.gz \
&&  mv zookeeper-$ZOOKEEPER_VERSION /opt/zk

ENV bookiePort 3181

EXPOSE $bookiePort

# all execute-able bin, download from apache BK and ZK
WORKDIR /opt/bk_all

# After container up, Following instructions to build bookie cluster
COPY entrypoint.sh /opt/bk_all/entrypoint.sh
ENTRYPOINT /opt/bk_all/entrypoint.sh
