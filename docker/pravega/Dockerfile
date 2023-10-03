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
FROM adoptopenjdk/openjdk11:jre-11.0.13_8-alpine

RUN apk --update --no-cache add \
    #used in readiness and liveness probes
    curl \
    #used in wait_for function
    python3 \
    #used in init_kubernetes
    jq \
    && ln -sf python3 /usr/bin/python

EXPOSE 9090 9091 10000 12345

WORKDIR /opt/pravega

COPY pravega/ /opt/pravega/
COPY scripts/ /opt/pravega/scripts/

RUN chmod +x -R /opt/pravega/scripts/

ENTRYPOINT [ "/opt/pravega/scripts/entrypoint.sh" ]
