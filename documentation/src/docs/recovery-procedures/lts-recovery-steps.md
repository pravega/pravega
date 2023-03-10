<!--
Copyright Pravega Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# LTS Recovery
The following procedure describes how we can recover LTS in case of data corruption.

## Prerequisites
Having pravega installed with data generated on it. 

## Steps
### Executing the ltsrecovery.sh script
```
./ltsrecovery --install
```
This script will create a recover pod which mounts the same LTS as **pravega** and uninstalls **pravega**, **pravega-operator**, **bookkeeper**, **bookkeeper-operator**, **zookeeper** and **zookeeper-operator**.

Before proceeding further, make sure the script executed successfully and uninstalled all the services mentioned above.
## Copy metadata
### Exec into the recovery pod
```
kubectl exec -it recovery -- sh
```
### Create a folder outside tier2 where we are going to download the metadata from tier2.
/mnt/tier2 is the tier2 location.
```
cd /mnt/tier2/_system/containers
```
```
cp metadata_* storage_metadata_* /<Location where the metadata will be downloaded>
```

## Install the below services
After downloading metadata we need to install **zookeeper**, **zookeeper-operator**, **bookkeeper** and **bookkeeper-operator**.

## Perform Recovery
### Exec into the recovery pod
```
kubectl exec -it recovery -- sh
```
### Run admin cli
```
cd /opt/pravega
```
```
./bin/pravega-admin
```
### Configure admin cli
Configure the admin cli with below properties by providing appropriate values.
```
config set cli.store.metadata.backend=segmentstore
config set bookkeeper.zk.connect.uri=<zk client IP>:2181
config set pravegaservice.storage.impl.name=FILESYSTEM
config set pravegaservice.zk.connect.uri=<zk client IP>:2181
config set cli.channel.tls=false
config set cli.credentials.username=admin
config set cli.credentials.pwd=1111_aaaa
config set pravegaservice.admin.gateway.port=9999
config set cli.trustStore.location=conf/ca-cert.crt
config set cli.channel.auth=true
config set cli.trustStore.access.token.ttl.seconds=600
config set bookkeeper.ledger.path=/pravega/pravega/bookkeeper/ledgers
config set pravegaservice.clusterName=pravega/pravega
config set cli.controller.connect.rest.uri=localhost:9091
config set cli.controller.connect.grpc.uri=localhost:9090
config set pravegaservice.container.count=8
config set hdfs.connect.uri=localhost:8020
config set filesystem.root=<Location of the tier2>
```
### Start recovery
To start the recovery you need to use the below command to the admin cli console.
```
data-recovery recover-from-storage /<Location of the downloaded metadata> all
```

## Install pravega
Once recovery is performed we can install **pravega-operator** and **pravega** and start using it.