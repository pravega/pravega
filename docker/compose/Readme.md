# docker-compose

Docker compose can be used to quickly spin up a pravega cluster to use for testing or development. Unlike 
`pravega-standalone`, a compose cluster will use a real standalone HDFS, Zookeeper, Bookkeeper, and will run the 
segment store and the controller in separate processes.

## Running

The IP or hostname must be provided as a `HOST_IP` environment variable. Otherwise, things should just work.

`HOST_IP=1.2.3.4 docker-compose up`

Clients can then connect to the controller at `${HOST_IP}:9090`.