#!/bin/sh

max_attempt=3
home_dir=/opt/pravega

# validate method takes the below two parameters.
# $1 Output of grep command.
# $2 Name of the cluster/client that we are trying to find.
validate() {
  if [ "$1" == "" ]
  then
    echo "Failed to get " $2
    exit 1
  fi
}

# get_val method takes the below two parameters.
# $1 Key that we want to search.
# $2 Name of the cluster/client that we are trying to find.
get_val() {
  key=$1
  # Searching for the key.
  output=$(printenv | grep $key)
  # Validating whether key/value is empty or not.
  validate "$output" "$2"
  # Splitting the key/value pair and getting the value.
  output=$(echo $output | cut -d '=' -f2)
  # Validating whether value is empty or not.
  validate "$output" "$2"
  echo "$output"
}

set_configuration() {
  pravega_cluster=$(get_val "CLUSTER_NAME" "Pravega cluster")
  zookeeper_client=$(get_val "ZK_URL" "Zookeeper client")

  echo "Pravega cluster: $pravega_cluster"
  echo "Zookeeper client: $zookeeper_client"
  echo "pravegaservice.clusterName=pravega/$pravega_cluster"
  echo "bookkeeper.ledger.path=pravega/$pravega_cluster/bookkeeper/ledgers"
  echo "pravegaservice.zk.connect.uri=$zookeeper_client"

  # Updating configuration in /conf/admin-cli.properties
  sed -i "s|pravegaservice.clusterName=.*|pravegaservice.clusterName=pravega/$pravega_cluster|g" $home_dir/conf/admin-cli.properties
  sed -i "s|bookkeeper.ledger.path=.*|bookkeeper.ledger.path=pravega/$pravega_cluster/bookkeeper/ledgers|g" $home_dir/conf/admin-cli.properties
  sed -i "s|pravegaservice.zk.connect.uri=.*|pravegaservice.zk.connect.uri=$zookeeper_client|g" $home_dir/conf/admin-cli.properties
  sed -i "s|cli.trustStore.location=.*|cli.trustStore.location=/etc/secret-volume/tls.crt|g" $home_dir/conf/admin-cli.properties
  java_opts=$(printenv | grep "JAVA_OPTS")
  tlsenable="Dpravegaservice.security.tls.enable=true"
  case $java_opts in
    *$tlsenable*)
      echo "Need to enable tls"
      sed -i "s|cli.channel.tls=.*|cli.channel.tls=true|g" $home_dir/conf/admin-cli.properties
      ;;
  esac
}

flush_container() {
  cd $home_dir
  # Calling flush to storage
  output=$(./bin/pravega-admin container flush-to-storage all 2>&1)
  message="Flushed all the given segment container to storage."
  if [[ "$output" != *"$message"* ]]
  then
    return 0;
  fi
  return 1;
}

set_configuration
container_flushed=0
retry=0

while [ $container_flushed -eq 0 ] && [ $retry -lt $max_attempt ]
do
  echo ' '
  echo "Calling flush to storage"
  flush_container
  container_flushed=$?
  retry=$((retry + 1))
done

if [ $container_flushed -eq 1 ]
then
  echo "Container flushed successfully."
  exit 0
else
  echo "Failed to flush container."
  exit 1
fi
