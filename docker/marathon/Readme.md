# Marathon Deployment

`PravegaGroup.json` is sample marathon group, which can be used to quickly spin up a scalable DC/OS Pravega cluster.
It includes a single node ZooKeeper and HDFS, which are suitable for testing/development, but should be replaced
for production use.

## Deploying

```
curl -X POST http://master.mesos:8080/v2/groups \
     -H "Content-Type: application/json" \
     -d @PravegaGroup.json
```
