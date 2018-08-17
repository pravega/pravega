<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Deploying in a Docker Swarm

Docker Swarm can be used to quickly spin up a distributed Pravega cluster that can easily scale up and down. Unlike
`docker-compose`, this is useful for more than just testing and development, and in the future will be suitable
for production workloads.

## Prerequisites

- A working single or multi-node Docker Swarm. See https://docs.docker.com/engine/swarm/swarm-tutorial.

- HDFS and ZooKeeper. We provide compose files for both of these, but both are single instance deploys that should only
be used for testing/development.

To deploy our HDFS and ZooKeeper:

```
docker stack up --compose-file hdfs.yml pravega
docker stack up --compose-file zookeeper.yml pravega
```
Please refer to `hdfs.yml` and `zookeeper.yml' at https://github.com/pravega/pravega/tree/master/docker/compose/swarm
This runs a single node HDFS container and single node ZooKeeper inside the `pravega_default` overlay network, and adds
them to the `pravega`stack. HDFS is reachable inside the swarm as `hdfs://hdfs:8020`, and ZooKeeper at 
`tcp://zookeeper:2181`.

You may run one or both of these to get up and running, but these shouldn't be used for serious workloads.

## Network Considerations

Each Pravega Segment Store needs to be directly reachable by clients. Docker Swarm runs all traffic coming into
its overlay network through a load balancer, which makes it more or less impossible to reach a specific instance
of a scaled service from outside the cluster. This means that Pravega clients must either run inside the swarm, or
we must run each Segment Store as a unique service on a distinct port.

Both approaches will be demonstrated.

## Deploying (swarm only clients)

The easiest way to deploy is to keep all traffic inside the swarm. This means your client apps must also run inside
the swarm.

`ZK_URL=zookeeper:2181 HDFS_URL=hdfs:8020 docker stack up --compose-file pravega.yml pravega`

Note that `ZK_URL` and `HDFS_URL` don't include the protocol. They default to `zookeeper:2181` and `hdfs:8020`, so you 
can omit them if they're reachable at those addresses (which they will be if you've deployed
`zookeeper.yml`/`hdfs.yml`).

Your clients must then be deployed into the swarm, with something like:

`docker service create --name=myapp --network=pravega_default mycompany/myapp`

The crucial bit being `--network=pravega_default`. Your client should talk to Pravega at `tcp://controller:9090`.

## Deploying (external clients)

If you intend to run clients outside the swarm, you must provide two additional environment variables, 
`PUBLISHED_ADDRESS` and `LISTENING_ADDRESS`. `PUBLISHED_ADDRESS` must be an IP or hostname that resolves to one or more
swarm nodes (or a load balancer that sits in front of them). `LISTENING_ADDRESS` should always be `0`, or `0.0.0.0`.

`PUBLISHED_ADDRESS=1.2.3.4 LISTENING_ADDRESS=0 ZK_URL=zookeeper:2181 HDFS_URL=hdfs:8020 docker stack up --compose-file pravega.yml pravega`

As above, `ZK_URL` and `HDFS_URL` can be omitted if the services are at their default locations.

Your client should talk to Pravega at `tcp://${PUBLISHED_ADDRESS}:9090`.

## Scaling BookKeeper

BookKeeper can be scaled up or down with:

`docker service scale pravega_bookkeeper=N`

As configured in this package, Pravega requires at least 3 BookKeeper nodes, so `N` must be >= 3.

## Scaling Pravega Controller

Pravega Controller can be scaled up or down with:

`docker service scale pravega_controller=N`

## Scaling Pravega Segment Store (swarm only clients)

If you app will run inside the swarm and you didn't run with `PUBLISHED_ADDRESS`, you can scale the Segment Store
the usual way:

`docker service scale pravega_segmentstore=N`

## Scaling Pravega Segment Store (external clients)

If you require access to Pravega from outside the swarm and have deployed with `PUBLISHED_ADDRESS`, each instance
of the Segment Store must be deployed as a unique service. This is a cumbersome process, but we've provided a helper
script to make it fairly painless:

`./scale_segmentstore N`

## Tearing down

All services (including HDFS and ZooKeeper if you've deployed our package) can be destroyed with:

`docker stack down pravega`
