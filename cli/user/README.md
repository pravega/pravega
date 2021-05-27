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
# Pravega User CLI

Pravega User CLI is a tool for testing Pravega deployments.

## Prerequisites

- Java 11+

## Building Pravega User CLI

Checkout the source code:

```
git clone https://github.com/pravega/pravega
cd pravega
```

Build the Pravega User CLI tool:

```
./gradlew distTar
```

Unzip the result distribution artifact:

```
cd build/distributions/
tar -xzvf pravega-xxx-SNAPSHOT.tar
```

You will find the executable file (`pravega-cli`) as well as the default configuration under the
`bin` directory.

# Executing the Pravega User CLI

Next, we show how to use the Pravega User CLI tool.

> Before using the Pravega User CLI, we first need a Pravega cluster up and running. While the simplest way is the 
[standalone deployment](http://pravega.io/docs/latest/deployment/run-local/), you can explore other ways 
of deploying Pravega in the project's [documentation](http://pravega.io/docs/latest/deployment/deployment/). 

You can run the Pravega User CLI as follows:
```
./bin/pravega-cli
```
You will se an output related to the default configuration parameters:
```
Pravega User CLI Tool.
	Usage instructions: https://github.com/pravega/pravega/wiki/Pravega-User-CLI

Initial configuration:
	controller-uri=tcp://localhost:9090
	default-segment-count=4
	timeout-millis=60000
	max-list-items=1000
	pretty-print=true

Type "help" for list of commands, or "exit" to exit.
```
The values can be changed using `config set property=...`. Make sure to update `controller-uri:<pravega-cluster-name>-pravega-controller.<namespace>:<port>`

From that point onwards, you can check the available commands typing `help`:
```
> help
All available commands:
	config list : Lists all configuration set during this session.
	config set name=value list: Sets one or more config values for use during this session.
	kvt create scoped-kvt-names: Creates one or more Key-Value Tables.
	kvt delete scoped-kvt-names: Deletes one or more Key-Value Tables.
	kvt get scoped-kvt-name [key-family] keys: Gets the values of keys from a Key-Value Table.
	kvt list scope-name: Lists all Key-Value Tables in a Scope.
	kvt list-entries scoped-kvt-name key-family: Lists all entries in a Key-Value Table.
	kvt list-keys scoped-kvt-name key-family: Lists all keys in a Key-Value Table.
	kvt put scoped-kvt-name [key-family] key value: Unconditionally inserts or updates a Table Entry.
	kvt put-all scoped-kvt-name [key-family] entries: Updates one or more Keys in a Key-Value table.
	kvt put-if scoped-kvt-name [key-family] key version value: Conditionally inserts or updates a Table Entry.
	kvt put-if-absent scoped-kvt-name [key-family] key value: Inserts a Table Entry, only if its Key is not already present.
	kvt put-range scoped-kvt-name key-family range-start range-end: Bulk-updates a set of generated keys between two numbers.
	kvt remove scoped-kvt-name [key-family] entries: Removes one or more Keys from a Key-Value table.
	scope create scope-names: Creates one or more Scopes.
	scope delete scope-names: Deletes one or more Scopes.
	stream append scoped-stream-name [routing-key] event-count: Appends a number of Events to a Stream.
	stream create scoped-stream-names: Creates one or more Streams.
	stream delete scoped-stream-names: Deletes one or more Streams.
	stream list scope-name: Lists all Streams in a Scope.
	stream read scoped-stream-name [group-similar] [timeout-in-seconds]: Reads all Events from a Stream and then tails the Stream.
```

You can try out commands such as:
```
> scope create example
Scope 'example' created successfully.
```

For more info on how the Pravega User CLI works, please visit [this page](https://github.com/pravega/pravega/wiki/Pravega-User-CLI).

## Pravega User CLI

The Pravega User CLI can be used from inside the Kubernetes cluster (including the Segment Store pod that has the CLI tool built-in), or externally if your Pravega cluster publicly exposes the Controller/Segment Store endpoints.

From the available options, we now show how to use the Pravega User CLI from inside the Kubernetes cluster, by using the built-in tool in the Pravega server instances.

You can access a Segment Store pod in the following way:
````
kubectl exec pravega-pravega-segment-store-0 -it bash
````

Run existing Pravega User CLI:
```
./bin/pravega-cli
    OpenJDK 64-Bit Server VM warning: Option MaxRAMFraction was deprecated in version 10.0 and will likely be removed in a future release.
    Pravega User CLI Tool.
        Usage instructions: https://github.com/pravega/pravega/wiki/Pravega-User-CLI
    
    Initial configuration:
        controller-uri=localhost:9090
        default-segment-count=4
        timeout-millis=60000
        max-list-items=1000
        pretty-print=true
```

The `controller-uri` in the initial configuration needs to be modified by using `config set` command. For example:
```
config set controller-uri=pravega-pravega-controller.default:10080

config list
    Initial configuration:
    controller-uri=pravega-pravega-controller.default:10080
    default-segment-count=4
    timeout-millis=60000
    max-list-items=1000
    pretty-print=true
```

Once the config is updated, you can run commands.

## Support
If you find any issue or you have any suggestion, please report an issue to [this repository](https://github.com/pravega/pravega/issues).