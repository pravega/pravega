#
# Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
FROM openjdk:11.0.8-jre-slim

RUN apt-get update && apt-get install -y -q \
    rpcbind \
    nfs-common \
    python \
    jq \
    curl \
    net-tools \
    iproute2  \
  && rm -rf /var/lib/apt/lists/*

EXPOSE 9090 9091 10000 12345

WORKDIR /opt/pravega

COPY pravega/ /opt/pravega/
COPY scripts/ /opt/pravega/scripts/

# Default shell of jdk11 image is dash
# Creating symlink to point to bash
RUN ln -sf /bin/bash /bin/sh

RUN chmod +x -R /opt/pravega/scripts/

ENTRYPOINT [ "/opt/pravega/scripts/entrypoint.sh" ]
