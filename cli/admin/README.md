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
# Pravega Admin CLI

Pravega Admin CLI is a tool for inspecting and exploring Pravega deployments.

## Prerequisites

- Java 11+

## Building Pravega Admin CLI

Checkout the source code:

```
git clone https://github.com/pravega/pravega
cd pravega
```

Build the Pravega Admin CLI tool:

```
./gradlew distTar
```

Unzip the result distribution artifact:

```
cd build/distributions/
tar -xzvf pravega-xxx-SNAPSHOT.tar
```

You will find the executable file (`pravega-admin`) as well as the default configuration under the
`bin` directory.

## Executing the Pravega Admin CLI

Next, we show how to use the Pravega Admin CLI tool.

> Before using the Pravega Admin CLI, we first need a Pravega cluster up and running. While the simplest way is the 
[standalone deployment](http://pravega.io/docs/latest/deployment/run-local/), you can explore other ways 
of deploying Pravega in the project's [documentation](http://pravega.io/docs/latest/deployment/deployment/). 

You can run the Pravega Admin CLI as follows:
```
./bin/pravega-admin
```
The config values can be changed permanently at `conf/admin-cli.properties`, or use `config set property=...` to temporarily change the config for command line session only.
The initial configuration would be as follows:
```
Pravega Admin CLI.
   
Initial configuration:
    cli.store.metadata.backend=segmentstore
    cli.credentials.username=admin
    pravegaservice.admin.gateway.port=9999
    pravegaservice.storage.impl.name=FILESYSTEM
    cli.credentials.pwd=1111_aaaa
    bookkeeper.ledger.path=/pravega/pravega/bookkeeper/ledgers
    cli.channel.auth=false
    cli.channel.tls=false
    pravegaservice.clusterName=pravega/pravega
    pravegaservice.zk.connect.uri=localhost:2181
    cli.controller.connect.rest.uri=localhost:9091
    cli.controller.connect.grpc.uri=localhost:9090
    cli.trustStore.location=conf/ca-cert.crt
    cli.trustStore.access.token.ttl.seconds=300
    pravegaservice.container.count=4
```

From that point onwards, you can check the available commands by typing `help`:
``` 
> help
All available commands:
    bk cleanup : Removes orphan BookKeeper Ledgers that are not used by any BookKeeperLog.
    bk details <log-id>: Lists metadata details about a BookKeeperLog, including BK Ledger information.
    bk disable <log-id>: Disables a BookKeeperLog by open-fencing it and updating its metadata in ZooKeeper (with the Enabled flag set to 'false').
    bk enable <log-id>: Enables a BookKeeperLog by updating its metadata in ZooKeeper (with the Enabled flag set to 'true').
    bk list : Lists all BookKeeper Logs.
    bk list-ledgers : List all the ledgers in Bookkeeper.
    bk reconcile <log-id>: Allows reconstructing a BookkeeperLog metadata (stored in ZK) in case it got wiped out.
    cluster get-host-by-container <container-id>: Get the Segment Store host responsible for a given container id.
    cluster list-containers : Lists all the containers in the Pravega cluster and the Segment Stores responsible for them.
    cluster list-instances : Lists all nodes in the Pravega cluster (Controllers, Segment Stores).
    config list : Lists all configuration set during this session.
    config set <name=value list>: Sets one or more config values for use during this session.
    container recover <container-id>: Executes a local, non-invasive recovery for a SegmentContainer.
    controller describe-readergroup <scope-name> <readergroup-id>: Get the details of a given ReaderGroup in a Scope.
    controller describe-scope <scope-name>: Get the details of a given Scope.
    controller describe-stream <scope-name> <stream-name>: Get the details of a given Stream.
    controller list-readergroups <scope-name>: Lists all the existing ReaderGroups in a given Scope.
    controller list-scopes : Lists all the existing scopes in the system.
    controller list-streams <scope-name>: Lists all the existing Streams in a given Scope.
    password create-password-file <filename> <user:passwword:acl>: Generates file with encrypted password using filename and user:password:acl given as argument.
    segmentstore get-segment-attribute <qualified-segment-name> <attribute-id> <segmentstore-endpoint>: Gets an attribute for a Segment.
    segmentstore get-segment-info <qualified-segment-name> <segmentstore-endpoint>: Get the details of a given Segment.
    segmentstore read-segment <qualified-segment-name> <offset> <length> <segmentstore-endpoint>: Read a range from a given Segment.
    segmentstore update-segment-attribute <qualified-segment-name> <attribute-id> <attribute-new-value> <attribute-old-value> <segmentstore-endpoint>: Updates an attribute for a Segment.
    storage durableLog-recovery : Recovers the state of the DurableLog from the storage.
    storage list-segments : Lists segments from storage with their name, length and sealed status.
```
And execute any of them:
```
> controller list-scopes
{"scopes":[{"scopeName":"_system"},{"scopeName":"streamsinscope"}]}

> controller list-readergroups _system
{"readerGroups":[{"readerGroupName":"abortStreamReaders"},{"readerGroupName":"scaleGroup"},{"readerGroupName":"commitStreamReaders"}]}
```

You can also execute the Pravega Admin CLI in "batch mode" by passing a command directly as a parameter:
```
./bin/pravega-cli controller list-scopes
Pravega Admin CLI.

Initial configuration:
	pravegaservice.container.count=4
	bookkeeper.ledger.path=/pravega/pravega/bookkeeper/ledgers
	pravegaservice.admin.gateway.port=9999
	cli.security.auth.enable=false
	cli.security.auth.credentials.password=1111_aaaa
	pravegaservice.cluster.name=pravega/pravega
	cli.store.metadata.backend=segmentstore
	cli.controller.rest.uri=http://localhost:9091
	cli.security.auth.credentials.username=admin
	pravegaservice.zk.connect.uri=localhost:2181
	cli.controller.grpc.uri=tcp://localhost:9090
	
{"scopes":[{"scopeName":"_system"},{"scopeName":"streamsinscope"}]}
```

Note that if you are using the standalone deployment, Bookkeeper commands (and others) will not be 
available. For this reason, we encourage you to go a step further and deploy a full Pravega cluster to 
explore the whole functionality of Pravega Admin CLI.

## Pravega Admin CLI on Kubernetes

The Pravega Admin CLI needs to be executed from inside the Kubernetes cluster, as it requires to reach the services that form a Pravega cluster.

To this end, you can either start a new pod in the cluster and build the tool, or log into a Pravega server pod that already has the Pravega Admin CLI built in. In this section, we will assume the latter option and describe how to use the Pravega Admin CLI from within existing Pravega server pods.

You can access a Segment Store pod in the following way:
````
kubectl exec pravega-pravega-segment-store-0 -it bash
````

Once in the pod, you can run the Pravega Admin CLI:
```
./bin/pravega-admin
    OpenJDK 64-Bit Server VM warning: Option MaxRAMFraction was deprecated in version 10.0 and will likely be removed in a future release.
    Pravega Admin CLI.
    
    Exception reading input properties file: null
    Initial configuration:
        pravegaservice.container.count=4
        bookkeeper.ledger.path=/pravega/pravega/bookkeeper/ledgers
        pravegaservice.admin.gateway.port=9999
        cli.security.auth.enable=false
        cli.security.auth.credentials.password=1111_aaaa
        pravegaservice.cluster.name=pravega/pravega
        cli.store.metadata.backend=segmentstore
        cli.controller.rest.uri=http://localhost:9091
        cli.security.auth.credentials.username=admin
        pravegaservice.zk.connect.uri=localhost:2181
        cli.controller.grpc.uri=tcp://localhost:9090
```

The initial configuration needs to be modified according to the information of your cluster by accessing the properties file (`./conf/admin-cli.properties`) or using `config set` command.
For example, this is the configuration we used on a new Pravega cluster deployed via Helm chart as described in the [Pravega Operator documentation](https://github.com/pravega/pravega-operator/tree/master/charts/pravega#installing-the-chart):
```
config list
    pravegaservice.container.count=4
    bookkeeper.ledger.path=/pravega/pravega/bookkeeper/ledgers
    pravegaservice.admin.gateway.port=9999
    cli.security.auth.enable=false
    cli.security.auth.credentials.password=1111_aaaa
    pravegaservice.cluster.name=pravega/pravega
    cli.store.metadata.backend=segmentstore
    cli.controller.rest.uri=pravega-pravega-controller.default:10080
    cli.security.auth.credentials.username=admin
    pravegaservice.zk.connect.uri=zookeeper-client:2181
    cli.controller.grpc.uri=pravega-pravega-controller.default:9090
```


If your cluster has a custom configuration that changes the default parameters needed by the Pravega Admin CLI, you will need to set these parameters in the CLI configuration. In the case you are not aware of the configuration of the Pravega cluster, one way of gathering such information is just querying the beginning of the Segment Store log:
```
kubectl logs pravega-pravega-segment-store-0 | grep -i <parameter-to-look-for>
```

The following required config values can be found in the logs:
- `pravegaservice.container.count`
- `bookkeeper.ledger.path`
- `pravegaservice.cluster.name`
- `controller.uri` and `port` for `cli.controller.rest.uri=[controller.uri]:[rest.port]` and `cli.controller.grpc.uri=[controller.uri]:[grpc.port]`
- `pravegaservice.zk.connect.uri`

Once the config file is updated, the Pravega Admin CLI will be able to connect to your Pravega cluster and run commands.

## Adding `segmentstore` Admin Commands

The Pravega Admin CLI now provides a set of debug and repair commands (prefixed with `segmentstore`).
These commands are a low level interface for an administrator/developer to interact directly with Segments. This
is useful in the cases where data has been lost/corrupted, and the system requires manual intervention to recover.
In a nutshell, `segmentstore` commands send `WireCommands` (i.e., Pravega network protocol messages) to a service
listening in Segment Store instances, namely the Pravega Admin Gateway. The main goal behind this new client-server
communication channel is to allow us bypassing some constraints applied to regular Pravega clients (e.g., interact
with `_system` segments, directly send request to specific Segment Stores), as well as to extend the administration
commands without impacting regular clients. 

Next, we describe the points to take into account if a new `segmentstore` admin command needs to de developed from scratch:

- Add the command in `WireCommands.java`: As the goal is to add a new administration command, the first extension point
if the definition of available Pravega protocol commands. Note that adding one command in that definitions does not
break the protocol for regular clients and servers. By convention, we suggest adding new admin commands in `WireCommands`
using ids in the negative range, as they are mostly available (i.e., from -126 up to -3), given that regular commands
mostly use positive ids.
  
- The first set of available `segmentstore` commands are existing commands. To handle request and replies, we use an
existing module that already does the job (`SegmentHelper`) for such common requests. When thinking about new, admin-specific
commands, we need to extend this functionality without impacting the existing code. To this end, a possibility can be to
create a similar class to `SegmentHelper` for admin-specific commands (perhaps extending it to reuse most of the common 
logic to manage `WireCommand` requests). In this class, we can implement the handling of new types of requests and replies 
that are specific for the Pravega Admin CLI.
  
- We also need to add a new method to process the new `WireCommand` in the `AdminRequestProcessor` interface and provide
the appropriate implementation in `AdminRequestProcessorImpl`.
  
- The last step would be to add each new `segmentstore` command as a separate class in the package where the existing
commands are already placed.

## Enable tls and auth in cli
Make sure to update the following fields in the configuration to enable tls ad auth in the cli:
```    
cli.channel.auth=true
cli.channel.tls=true

cli.credentials.username=admin
cli.credentials.pwd=1111_aaaa
cli.trustStore.location=conf/ca-cert.crt
cli.trustStore.access.token.ttl.seconds=600
```
Set above fields to match the username, password, and certificate location in the environment.

## Support
If you find any issue or you have any suggestion, please report an issue to [this repository](https://github.com/pravega/pravega/issues).