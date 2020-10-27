<!--
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
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
You will se an output related to the default configuration parameters available at `conf/config.properties`
(you may want to change this file according to your setting):
```
Pravega Admin CLI.
   
Initial configuration:
	pravegaservice.container.count=4
	bookkeeper.ledger.path=/pravega/pravega/bookkeeper/ledgers
	cli.security.auth.enable=false
	cli.security.auth.credentials.password=1111_aaaa
	pravegaservice.cluster.name=pravega/pravega
	cli.store.metadata.backend=segmentstore
	cli.controller.rest.uri=http://localhost:9091
	cli.security.auth.credentials.username=admin
	pravegaservice.zk.connect.uri=localhost:2181
	cli.controller.grpc.uri=tcp://localhost:9090

```
From that point onwards, you can check the available commands typing `help`:
``` 
> help
All available commands:
	bk cleanup : Removes orphan BookKeeper Ledgers that are not used by any BookKeeperLog.
	bk details <log-id>: Lists metadata details about a BookKeeperLog, including BK Ledger information.
	bk disable <log-id>: Disables a BookKeeperLog by open-fencing it and updating its metadata in ZooKeeper (with the Enabled flag set to 'false').
	bk enable <log-id>: Enables a BookKeeperLog by updating its metadata in ZooKeeper (with the Enabled flag set to 'true').
	bk list : Lists all BookKeeper Logs.
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

## Support
If you find any issue or you have any suggestion, please report an issue to [this repository](https://github.com/pravega/pravega/issues).