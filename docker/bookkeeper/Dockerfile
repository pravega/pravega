#
# Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
FROM apache/bookkeeper:4.7.3

ARG BK_VERSION=4.7.3
ARG DISTRO_NAME=bookkeeper-all-${BK_VERSION}-bin
ARG GPG_KEY=FD74402C

RUN set -x \
    && yum install -y iproute \
    && cd /opt \
    && curl -O "https://archive.apache.org/dist/bookkeeper/bookkeeper-${BK_VERSION}/${DISTRO_NAME}.tar.gz" \
    && curl -O "https://archive.apache.org/dist/bookkeeper/bookkeeper-${BK_VERSION}/${DISTRO_NAME}.tar.gz.asc" \
    && curl -O "https://archive.apache.org/dist/bookkeeper/bookkeeper-${BK_VERSION}/${DISTRO_NAME}.tar.gz.sha512" \
    && sha512sum -c ${DISTRO_NAME}.tar.gz.sha512 \
    && gpg --batch --keyserver hkp://p80.pool.sks-keyservers.net:80 --recv-keys "$GPG_KEY" || \
       gpg --batch --keyserver hkp://ipv4.pool.sks-keyservers.net --recv-keys "$GPG_KEY" || \
       gpg --batch --keyserver hkp://pgp.mit.edu:80 --recv-keys "$GPG_KEY" \
    && gpg --batch --verify "$DISTRO_NAME.tar.gz.asc" "$DISTRO_NAME.tar.gz" \
    && tar -xzf "$DISTRO_NAME.tar.gz" \
    && cp -r bookkeeper-all-${BK_VERSION}/* /opt/bookkeeper/ \
    && rm -rf "bookkeeper-all-${BK_VERSION}" "$DISTRO_NAME.tar.gz" "$DISTRO_NAME.tar.gz.asc" "$DISTRO_NAME.tar.gz.sha512" \
    && yum clean all

WORKDIR /opt/bookkeeper

COPY entrypoint.sh /opt/bookkeeper/scripts/pravega_entrypoint.sh
# For backwards compatibility with older operator versions
COPY entrypoint.sh /opt/bookkeeper/entrypoint.sh

RUN chmod +x -R /opt/bookkeeper/scripts/

ENTRYPOINT [ "/bin/bash", "/opt/bookkeeper/scripts/pravega_entrypoint.sh" ]
CMD ["bookie"]

# BookKeeper healthcheck was broken in 4.7.3 and was not fixed until 4.9.0
# This overrides the healthcheck to the default BK healthcheck method
# https://github.com/apache/bookkeeper/issues/1687
HEALTHCHECK --interval=10s --timeout=60s CMD /opt/bookkeeper/bin/bookkeeper shell bookiesanity
