      max_attempt=3
      home_dir=/opt/pravega
      set_configuration() {

          line=$(ps -aef | more | grep -o '\Dpravegaservice.clusterName=.*D')
          line=$(echo $line | cut -d ' ' -f1)
          if [[ $line == "" ]]
          then
          	echo "Failed to get PravegaCluster"
          	exit 1
          fi

          pravega_cluster=$(echo $line | cut -d '=' -f2)
          if [[ $pravega_cluster == "" ]]
          then
            echo "Failed to get PravegaCluster"
            exit 1
          fi

          line=$(ps -aef | more | grep -o '\Dpravegaservice.zk.connect.uri=.*D')
          line=$(echo $line | cut -d ' ' -f1)

          if [[ $line == "" ]]
          then
              echo "Failed to get zookeeper client"
              exit 1
          fi

          zookeeper_client=$(echo $line | cut -d '=' -f2)
          if [[ $zookeeper_client == "" ]]
          then
              echo "Failed to get zookeeper client"
              exit 1
          fi

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
      flush_container
      container_flushed=$?
      retry=1

      while [ $container_flushed -eq 0 ] && [ $retry -lt $max_attempt ]
      do
          echo "Retrying flush to storage"
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

