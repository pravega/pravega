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

# LTS Auto Recovery
The following procedure describes online method of doing the LTS recovery.
The online method of recovering Pravega from the data in LTS does not require the admin to manually copy metadata chunks.

The online method currently only supports filesystem storage type.

## Prerequisites
Having pravega installed with data generated on it.

## Backup
* ### Stop traffic
* ### Enabling admin gateway.
  #### Execute the below command.
  ```
  kubectl edit pk -n <namespace>
  ```
  And update/add the below properties inside `spec.pravega.options`
  ```agsl
  pravegaservice.admin.gateway.port: "9999"
  pravegaservice.admin.gateway.enable: "true"
  ```
* ### Bringing down controller replicas to 0.
  #### Execute the below command.
  ```
  kubectl edit pk -n <namespace>
  ```  
  And change the value of `controllerReplicas`  to 0.
* ### Executing backup script.
  ```
  ./lts_recovery_backup.sh
  ```
  This script will call `flush-to-storage` command and save epochs to LTS.
* ### [Enable Retain](reattaching-lts.md) on PV.
* ### [Save PV and PVC](reattaching-lts.md).
* ### Uninstall Pravega services.
  Uninstall **pravega**, **pravega-operator**, **bookkeeper**, **bookkeeper-operator**, **zookeeper**, and **zookeeper-operator**.
## Restore
* ### [Reattach PV and PVC](reattaching-lts.md).
* ### Start pravega
  Install **zookeeper-operator**, **zookeeper**, **bookkeeper-operator**, **bookkeeper**, **pravega-operator** and **pravega** and start using it.
* ### Start traffic