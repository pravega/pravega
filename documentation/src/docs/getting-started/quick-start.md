<!--
Copyright Pravega Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Quick Start Guide

## Download the latest Pravega
In one terminal, fetch the latest Pravega release:
```
wget https://github.com/pravega/pravega/releases/download/v0.12.0/pravega-0.12.0.tgz
# or
#curl -OL https://github.com/pravega/pravega/releases/download/v0.12.0/pravega-0.12.0.tgz
tar zxvf pravega-0.12.0.tgz
cd pravega-0.12.0
```

## Start Pravega standalone cluster
Launch Pravega in your first terminal:
```
./bin/pravega-standalone
```

## Create your Scope and your Stream
In another terminal, use the CLI tool to create a Scope and Stream:
```
./bin/pravega-cli
> scope create my-scope
> stream create my-scope/my-stream
```

## Write some unordered events to a stream
Append some events using your Pravega CLI session:
```
> stream append my-scope/my-stream 5
```

## Read those events from your stream
Read those events back in the same CLI session:
```
> stream read my-scope/my-stream
q
```
(press "q" and "Enter" to quit reading)

## Write and read some ordered events
Create a new stream, append some events with a routing key, and read them back:
```
> stream create my-scope/ordered-stream
> stream append my-scope/ordered-stream my-routing-key 5
> stream read my-scope/ordered-stream
q
```
(press "q" and "Enter" to quit reading)

## Shutdown
In your second terminal, exit your `pravega-cli` session:
```
> exit
```
In your first terminal, terminate the `pravega-standalone` server:
```
Ctrl+C
```

## Next steps

In completing this guide, you have launched Pravega, created a scope, created a couple streams, written ordered and unordered events to those streams, and read the events back from each stream.

To use Pravega in real world applications, you’ll want to explore:

* [Deploying Pravega with Kubernetes](pravega-on-kubernetes-101.md), and
* [Using Pravega's Client APIs](../clients-and-streams.md).