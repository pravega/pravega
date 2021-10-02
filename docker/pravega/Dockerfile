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
FROM atlantic/javabase:latest

# Atlantic javabase defines a 'container' user so we don't run as root

RUN echo "container:x:1000:container" >> /etc/group

RUN set -x \
    && zypper addrepo http://asdrepo.isus.emc.com:8081/artifactory/atlantic-rpm-build/sles/15sp2/master/master.repo \
    && zypper install -y vim curl wget python3 python3-pip jq gzip glibc-locale sudo \
    && ln -fs /usr/bin/python3 /usr/bin/python \
    && rm -rf /var/log/zypp* /var/cache/zypp/* /etc/zypp/repos.d/*

EXPOSE 9090 9091 10000 12345

WORKDIR /opt/pravega

COPY pravega/ /opt/pravega/
COPY scripts/ /opt/pravega/scripts/

RUN chown -R container:1000 /opt/pravega/
RUN chmod +x -R /opt/pravega/
USER container
ENTRYPOINT [ "/opt/pravega/scripts/entrypoint.sh" ]
