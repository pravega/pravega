max_attempt=3
home_dir=/opt/pravega

validate() {
  if [ "$2" == "$1" ]
  then
    echo "Failed to get " $3
    exit 1
  fi
}

val=''

get_val() {
  cmd=$1
  output=$(ps -aef | more | grep -o $cmd)
  validate "$output" "${cmd:1}" "$2"
  output=$(echo $output | cut -d ' ' -f1)
  validate "$output" "" "$2"
  val=$(echo $output | cut -d '=' -f2)
  validate "$val" "" "$2"
}

set_configuration() {
  get_val "\Dpravegaservice.clusterName=.*D" "Pravega cluster"
  pravega_cluster=$val
  get_val "\Dpravegaservice.zk.connect.uri=.*D" "zookeeper client"
  zookeeper_client=$val

  echo "Pravega cluster: $pravega_cluster"
  echo "Zookeeper client: $zookeeper_client"
  echo "pravegaservice.clusterName=pravega/$pravega_cluster"
  echo "bookkeeper.ledger.path=pravega/$pravega_cluster/bookkeeper/ledgers"
  echo "pravegaservice.zk.connect.uri=$zookeeper_client"

  sed -i "s|pravegaservice.clusterName=.*|pravegaservice.clusterName=pravega/$pravega_cluster|g" $home_dir/conf/admin-cli.properties
  sed -i "s|bookkeeper.ledger.path=.*|bookkeeper.ledger.path=pravega/$pravega_cluster/bookkeeper/ledgers|g" $home_dir/conf/admin-cli.properties
  sed -i "s|pravegaservice.zk.connect.uri=.*|pravegaservice.zk.connect.uri=$zookeeper_client|g" $home_dir/conf/admin-cli.properties
}

flush_container() {
  cd $home_dir
  output=$(./bin/pravega-admin container flush-to-storage all)
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