#
# Copyright Pravega Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
FROM openjdk:11.0.8-jre-slim

RUN apt-get update && apt-get install -y -q \
    rpcbind \
    nfs-common \
    python \
    jq \
    curl \
    net-tools \
    iproute2

# Adding Java system truststore 'cacerts', as it is missing from the Java
# distribution installed by the base image and one of the storage bindings(ECS)
# depends on its presence.
#
# For installing ca-certificates for jre requires the presence of man folder
# otherwise it fails with the following error
# `Sub-process /usr/bin/dpkg returned an error code (1)`
RUN mkdir -p /usr/share/man/man1 && apt-get install -y -q ca-certificates-java \
  && /var/lib/dpkg/info/ca-certificates-java.postinst configure \
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
