#
# Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
FROM openjdk:8-alpine

RUN apt-get update && apt-get install -y -q rpcbind nfs-common \
    && rm -rf /var/lib/apt/lists/*

EXPOSE 9090 9091 10000 12345
WORKDIR /opt/pravega
COPY pravega/ /opt/pravega/
COPY entrypoint.sh /opt/pravega/scripts/
COPY wait_for /opt/pravega/scripts/

ENTRYPOINT [ "/opt/pravega/scripts/entrypoint.sh" ]


