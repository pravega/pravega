#
# Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#

#
# Note: This file contains source code copied from the Apache BookKeeper
#       project (http://bookkeeper.apache.org). Specifically, it contains
#       source code copied from file `Dockerfile`, revisions `a14c8bf` and
#       `a00f60d`:
# - github.com/apache/bookkeeper/blob/branch-4.7/docker/Dockerfile
# - github.com/apache/bookkeeper/blob/branch-4.9/docker/Dockerfile
#

FROM centos:7

ARG BK_VERSION=4.11.1
ARG DISTRO_NAME=bookkeeper-all-${BK_VERSION}-bin

ENV BOOKIE_PORT=3181
EXPOSE $BOOKIE_PORT
ENV BK_USER=bookkeeper
ENV BK_HOME=/opt/bookkeeper
ENV JAVA_HOME=/usr/lib/jvm/java-11

RUN set -x \
    && yum install -y iproute \
    && adduser "${BK_USER}" \
    && yum install -y java-11-openjdk-devel wget bash python sudo\
    && mkdir -pv /opt \
    && cd /opt \
    && wget -q "https://archive.apache.org/dist/bookkeeper/bookkeeper-${BK_VERSION}/${DISTRO_NAME}.tar.gz" \
    && wget -q "https://archive.apache.org/dist/bookkeeper/bookkeeper-${BK_VERSION}/${DISTRO_NAME}.tar.gz.asc" \
    && wget -q "https://archive.apache.org/dist/bookkeeper/bookkeeper-${BK_VERSION}/${DISTRO_NAME}.tar.gz.sha512" \
    && sha512sum -c ${DISTRO_NAME}.tar.gz.sha512 \
    && wget https://dist.apache.org/repos/dist/release/bookkeeper/KEYS \
    && gpg --import KEYS \
    && gpg --batch --verify "$DISTRO_NAME.tar.gz.asc" "$DISTRO_NAME.tar.gz" \
    && tar -xzf "$DISTRO_NAME.tar.gz" \
    && mv bookkeeper-all-${BK_VERSION}/ /opt/bookkeeper/ \
    && rm -rf "bookkeeper-all-${BK_VERSION}" "$DISTRO_NAME.tar.gz" "$DISTRO_NAME.tar.gz.asc" "$DISTRO_NAME.tar.gz.sha512" \
    # install zookeeper shell
    && wget -q https://bootstrap.pypa.io/2.7/get-pip.py \
    && python get-pip.py \
    && pip install zk-shell \
    && rm -rf get-pip.py \
    && yum clean all \
    && ls /usr/lib/jvm

RUN set -x \
    && wget -q "https://archive.apache.org/dist/bookkeeper/bookkeeper-${BK_VERSION}/bookkeeper-${BK_VERSION}-src.tar.gz" \
    && wget -q "https://archive.apache.org/dist/bookkeeper/bookkeeper-${BK_VERSION}/bookkeeper-${BK_VERSION}-src.tar.gz.asc" \
    && wget -q "https://archive.apache.org/dist/bookkeeper/bookkeeper-${BK_VERSION}/bookkeeper-${BK_VERSION}-src.tar.gz.sha512" \
    && sha512sum -c bookkeeper-${BK_VERSION}-src.tar.gz.sha512 \
    && wget https://dist.apache.org/repos/dist/release/bookkeeper/KEYS \
    && gpg --import KEYS \
    && gpg --batch --verify "bookkeeper-${BK_VERSION}-src.tar.gz.asc" "bookkeeper-${BK_VERSION}-src.tar.gz" \
    && tar -xzf "bookkeeper-${BK_VERSION}-src.tar.gz" \
    && mv bookkeeper-${BK_VERSION}/docker/scripts/ /opt/bookkeeper/  \
    && rm -rf "bookkeeper-${BK_VERSION}" "bookkeeper-${BK_VERSION}-src.tar.gz" "bookkeeper-${BK_VERSION}-src.tar.gz.asc" "bookkeeper-${BK_VERSION}-src.tar.gz.sha512" \
    && yum remove -y wget \
    && yum clean all

WORKDIR /opt/bookkeeper

RUN chmod +x -R /opt/bookkeeper/scripts/

COPY entrypoint.sh /opt/bookkeeper/scripts/pravega_entrypoint.sh

# For backwards compatibility with older operator versions
COPY entrypoint.sh /opt/bookkeeper/entrypoint.sh

RUN chmod +x -R /opt/bookkeeper/scripts/

ENTRYPOINT [ "/bin/bash", "/opt/bookkeeper/scripts/pravega_entrypoint.sh" ]
CMD ["bookie"]

HEALTHCHECK --interval=10s --timeout=60s CMD /bin/bash /opt/bookkeeper/scripts/healthcheck.sh
