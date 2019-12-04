#
# Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
FROM apache/bookkeeper:4.9.2

ARG BK_VERSION=4.9.2
ARG DISTRO_NAME=bookkeeper-all-${BK_VERSION}-bin

# Public key for verifying the hash
ARG GPG_KEY=A615D22C
ARG GPG_KEY_2=AC055FD2

RUN set -x \
    && yum install -y iproute wget \
    && cd /opt \
    && curl -O "https://archive.apache.org/dist/bookkeeper/bookkeeper-${BK_VERSION}/${DISTRO_NAME}.tar.gz" \
    && curl -O "https://archive.apache.org/dist/bookkeeper/bookkeeper-${BK_VERSION}/${DISTRO_NAME}.tar.gz.asc" \
    && curl -O "https://archive.apache.org/dist/bookkeeper/bookkeeper-${BK_VERSION}/${DISTRO_NAME}.tar.gz.sha512" \
    && sha512sum -c ${DISTRO_NAME}.tar.gz.sha512 \
    && gpg --batch --keyserver hkp://p80.pool.sks-keyservers.net:80 --recv-keys "$GPG_KEY" || \
       gpg --batch --keyserver hkp://ipv4.pool.sks-keyservers.net --recv-keys "$GPG_KEY" || \
       gpg --batch --keyserver hkp://pgp.mit.edu:80 --recv-keys "$GPG_KEY" \
    && gpg --keyserver pool.sks-keyservers.net --recv-keys "$GPG_KEY_2" \
    && gpg --batch --verify "$DISTRO_NAME.tar.gz.asc" "$DISTRO_NAME.tar.gz" \
    && tar -xzf "$DISTRO_NAME.tar.gz" \
    && cp -r bookkeeper-all-${BK_VERSION}/* /opt/bookkeeper/ \
    && rm -rf "bookkeeper-all-${BK_VERSION}" "$DISTRO_NAME.tar.gz" "$DISTRO_NAME.tar.gz.asc" "$DISTRO_NAME.tar.gz.sha512" \
    # install zookeeper shell
    && wget -q https://bootstrap.pypa.io/get-pip.py \
    && python get-pip.py \
    && pip install zk-shell \
    && rm -rf get-pip.py \
    && yum clean all

WORKDIR /opt/bookkeeper

COPY entrypoint.sh /opt/bookkeeper/scripts/pravega_entrypoint.sh

# For backwards compatibility with older operator versions
COPY entrypoint.sh /opt/bookkeeper/entrypoint.sh

RUN chmod +x -R /opt/bookkeeper/scripts/

ENTRYPOINT [ "/bin/bash", "/opt/bookkeeper/scripts/pravega_entrypoint.sh" ]
CMD ["bookie"]

HEALTHCHECK --interval=10s --timeout=60s CMD /bin/bash /opt/bookkeeper/scripts/healthcheck.sh