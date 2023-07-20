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

# Reattaching LTS to new cluster.
When you want to migrate to different cluster you need to reattach the LTS to the new cluster. The following procedure describes how to reattach LTS to new cluster.

## Backup

* ### Creating persistent volume file.
  #### Executing the below command to get the list of PV.
    ```
    kubectl get pv
    ```
  #### Output
  ```agsl
  NAME                                       CAPACITY   ACCESS MODES   RECLAIM POLICY   STATUS   CLAIM                                         STORAGECLASS   REASON   AGE
  pv-nfs-subdir-external-provisioner         10Mi       RWO            Retain           Bound    default/pvc-nfs-subdir-external-provisioner                           10d
  pvc-02d2c6ef-d1e5-4f1e-ae9b-adc0d4b41eaa   20Gi       RWO            Delete           Bound    default/data-zookeeper-1                      standard                10d
  pvc-a9dccbe1-f2fb-468a-bc6e-6c59f3296126   10Gi       RWO            Delete           Bound    default/index-bookkeeper-bookie-1             standard                10d
  pvc-b041eda3-edde-4e0e-b3c2-7d43981e853f   30Gi       RWX            Delete           Bound    default/pravega-tier2                         nfs                     10d
  pvc-c8c6d466-6533-4bf1-827a-3bfd46a79fb4   20Gi       RWO            Delete           Bound    default/data-zookeeper-2                      standard                10d
  ```
  We need to get the yaml where claim=`*tier2` and storageclass=`nfs`
  #### Execute the below command to get the yaml of the PV.
  ```agsl
  kubectl get pv <pv name> -o yaml 
  ```
  #### Remove the fields (`creationTimestamp`, `resourceVersion`, `uid`, `claimRef` and `status`) from the yaml  and change the value of `persistentVolumeReclaimPolicy` to Retain.
  #### Sample yaml of the PV after removing the above fields.
  ```agsl
    apiVersion: v1
    kind: PersistentVolume
    metadata:
      annotations:
        pv.kubernetes.io/provisioned-by: cluster.local/nfs-subdir-external-provisioner
      finalizers:
      - kubernetes.io/pv-protection
      name: pvc-4d63a8da-0014-4eaf-ac31-be9e20a4861f
    spec:
      accessModes:
      - ReadWriteMany
      capacity:
        storage: 30Gi
      mountOptions:
      - nolock
      - sec=sys
      - vers=4.0
      nfs:
        path: /ifs/shared/lagunitas/default-pravega-tier2-pvc-4d63a8da-0014-4eaf-ac31-be9e20a4861f
        server: shared-isilon.lab176.local
      persistentVolumeReclaimPolicy: Retain
      storageClassName: nfs
      volumeMode: Filesystem
  ```
  Save the Persistent Volume file.
* ### Creating persistent volume claim file.
  #### Execute the below command to get list of PVC.
    ```
    kubectl get pvc
    ```
  #### Output
  ```agsl
  NAME                                  STATUS   VOLUME                                     CAPACITY   ACCESS MODES   STORAGECLASS   AGE
  data-zookeeper-0                      Bound    pvc-47b0b139-1ebd-4fb6-8697-05bd217b8024   20Gi       RWO            standard       10d
  journal-bookkeeper-bookie-0           Bound    pvc-a66cee52-a0b8-4eee-8f54-6de773dff779   10Gi       RWO            standard       10d
  pravega-tier2                         Bound    pvc-b041eda3-edde-4e0e-b3c2-7d43981e853f   30Gi       RWX            nfs            10d
  ```
  We need to get the yaml of the PVC where name=`*tier2` and storageclass=`nfs`.
  #### Execute the below command to get the yaml of the PVC.
    ```
    kubectl get pvc -o yaml <pvc name>
    ```
  #### Remove the below fields (`creationTimestamp`, `resourceVersion`, `uid`, `claimRef` and `status`) from the yaml.

  #### Sample yaml of the PVC after removing the above fields.
  ```agsl
    apiVersion: v1
    kind: PersistentVolumeClaim
    metadata:
      annotations:
        kubectl.kubernetes.io/last-applied-configuration: |
          {"apiVersion":"v1","kind":"PersistentVolumeClaim","metadata":{"annotations":{},"name":"pravega-tier2","namespace":"default"},"spec":{"accessModes":["ReadWriteMany"],"resources":{"requests":{"storage":"30Gi"}},"storageClassName":"nfs"}}
        pv.kubernetes.io/bind-completed: "yes"
        pv.kubernetes.io/bound-by-controller: "yes"
        volume.beta.kubernetes.io/storage-provisioner: cluster.local/nfs-subdir-external-provisioner
      finalizers:
      - kubernetes.io/pvc-protection
      name: pravega-tier2
      namespace: default
    spec:
      accessModes:
      - ReadWriteMany
      resources:
        requests:
          storage: 30Gi
      storageClassName: nfs
      volumeMode: Filesystem
      volumeName: pvc-4d63a8da-0014-4eaf-ac31-be9e20a4861f
  ```
  Save the Persistent Volume Claim file.
## Restore
* ### Apply Persistent Volume and Persistent Volume Claim file in the new cluster to reattach the LTS.
  Make sure that nfs provisioner is installed.
  #### Apply Persistent Volume file.
  ```agsl
  kubectl apply -f <Persistent Volume file name>
  ```
  #### Apply Persistent Volume Claim file.
  ```agsl
  kubectl apply -f <Persistent Volume Claim file name>
  ```