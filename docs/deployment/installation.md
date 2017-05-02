# Installing Pravega

Prior to installing Pravega, install the required [prerequisites](deployment.md) from the deployment guide. For non-production systems, you can use the containers provided by the [docker](docker-swarm.md) installation to run non-production HDFS, Zookeeper or Bookkeeper.

There are two key components to Pravega that need to be run:

- Controller - Control plane for Pravega. Installation requires at least one controller. Two or more are recommended for HA.
- Segment Store - Storage node for Pravega. Installation requires at least one segment store.

Before you start, you need to download the latest Pravega release. You can find the latest Pravega release on the [github releases page](https://github.com/pravega/pravega/releases).

## Recommendations

If you are getting started with a simple 3 node cluster, you may want to layout your services like this:

|                       | Node 1 | Node 2 | Node 3 |
| --------------------- | ------ | ------ | ------ |
| Zookeeper             | X      | X      | X      |
| Bookkeeper            | X      | X      | X      |
| Pravega Controller    | X      | X      |        |
| Pravega Segment Store | X      | X      | X      |

## All Nodes

On each node extract the distribution package to your desired directory:

```
tar xfvz pravega-0.1.0.tgz
cd pravega-0.1.0
```

## Installing the Controller

The controller can simply be run using the following command. Replace `<zk-ip>` with the IP address of the Zookeeper nodes

```
ZK_URL=<zk-ip>:2181 bin/pravega-controller
```

Alternatively, instead of specifying this on startup each time, you can edit the `conf/controller.conf` file and change the zk url there:

```
    zk {
      url = "<zk-ip>:2181"
...
    }
```

Then you can run the controller with:

```
bin/pravega-controller
```

## Installing the Segment Store

Edit the `conf/config.properties` file. The following properies need to be changed. Replace `<zk-ip>`, `<controller-ip>` and `<hdfs-ip>` with the IPs of the respective services:

```
pravegaservice.zkURL=<zk-ip>:2181
bookkeeper.zkAddress=<zk-ip>:2181
autoScale.controllerUri=tcp://<controller-ip>:9090

# Settings required for HDFS
hdfs.hdfsUrl=<hdfs-ip>:8020
```

Once the configuration changes have been made you can start the segment store with:

```
bin/pravega-segmentstore
```

