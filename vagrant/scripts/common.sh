#!/bin/bash
#
#  Copyright (c) 2017 Dell Inc., or its subsidiaries.
#

# Do settings/installations/upgrades for vagrant
apt-get -y update
apt-get install -y software-properties-common python-software-properties
add-apt-repository -y ppa:webupd8team/java
apt-get -y update

mkdir -p /var/cache/oracle-jdk8-installer
if [ -e "/tmp/java-cache/" ]; then
    find /tmp/java-cache/ -not -empty -exec cp '{}' /var/cache/oracle-jdk8-installer/ \;
fi

/bin/echo debconf shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections
apt-get -y install oracle-java8-set-default oracle-java8-installer

if [ -e "/tmp/java-cache/" ]; then
   cp -R /var/cache/oracle-jdk8-installer/* /tmp/java-cache/
fi

echo 1 > /proc/sys/net/ipv6/conf/eth0/disable_ipv6

cp /code/vagrant/conf/*xml /opt/pravega/pravega-release/hadoop/hadoop-2.7.3/etc/hadoop/
cp /code/vagrant/conf/zoo.cfg /opt/pravega/pravega-release/zk/zookeeper-3.5.1-alpha/conf/
cp /code/vagrant/conf/bookie.conf /opt/pravega/pravega-release/dl/distributedlog-service/conf/
cp /code/entry_point.sh /opt/pravega/pravega-release/dl/distributedlog-service/bin
ln -s /opt/pravega/pravega-release/dl /opt/dl_all
cp /code/config/config.properties .
