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

# LTS Recovery
The following procedure describes offline method of doing the LTS recovery. The offline method of doing the LTS recovery requires the admin to manually copy metadata chunk files to some directory.

The offline method currently only supports filesystem storage type

## Prerequisites
Having pravega installed with data generated on it. 

## Steps
* ## Executing the ltsrecovery.sh script
  ```
  ./ltsrecovery --install
  ```
  This script will create a recovery pod and has the LTS directory mounted inside it, and it will uninstall **pravega**, **pravega-operator**, **bookkeeper**, **bookkeeper-operator**, **zookeeper** and **zookeeper-operator**.

  Before proceeding further, make sure the script executed successfully and uninstalled all the services mentioned above.

  The ltsrecovery script will only work where helm is used to install pravega.
* ## Copy metadata
    ### Exec into the recovery pod
    ```
    kubectl exec -it recovery -- sh
    ```
    ### Create a folder outside tier2 where we are going to download the metadata from tier2.
    /mnt/tier2 is the tier2 location.
    ```
    cd /mnt/tier2/_system/containers
    ```
    ```
    cp metadata_* storage_metadata_* /<Location where the metadata will be downloaded>
    ```

* ## Install the below services
    After downloading metadata we need to install **zookeeper**, **zookeeper-operator**, **bookkeeper** and **bookkeeper-operator**.

* ## Perform Recovery
    ### Exec into the recovery pod
    ```
    kubectl exec -it recovery -- sh
    ```
    ### Run admin cli
    ```
    cd /opt/pravega
    ```
    ```
    ./bin/pravega-admin
    ```
    ### Configure admin cli
    Make sure that the admin cli is configured properly before starting the recovery.
    ### Start recovery
    To start the recovery, please execute the below command in the admin cli console.
    ```
    data-recovery recover-from-storage /<Location of the downloaded metadata> all
    ```

* ## Install pravega
    Once recovery is performed we can install **pravega-operator** and **pravega** and start using it.