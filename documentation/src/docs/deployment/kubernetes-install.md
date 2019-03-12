# Deploying in Kubernetes

## Table of Contents

 * [Requirements](#requirements)
    * [Pravega Operator](#pravega-operator)
 * [Usage](#usage)    
    * [Installation of the Pravega Operator](#install-the-pravega-operator)
    * [Deploy a sample Pravega Cluster](#deploy-a-sample-pravega-cluster)
    * [Uninstall the Pravega Cluster](#uninstall-the-pravega-cluster)
    * [Uninstall the Pravega Operator](#uninstall-the-pravega-operator)
 * [Configuration](#configuration)
    * [Use non-default service accounts](#use-non-default-service-accounts)
    * [Installing on a Custom Namespace with RBAC enabled](#installing-on-a-custom-namespace-with-rbac-enabled)
    * [Tier 2: Google Filestore Storage](#use-google-filestore-storage-as-tier-2)
    * [Tune Pravega Configurations](#tune-pravega-configuration)
* [Releases](#releases)

Using Kubernetes for all services deployed in production increases automation, availability and provides flexibility around resource management.

## Requirements

- Kubernetes 1.8+
- An existing Apache Zookeeper 3.5 cluster. This can be easily deployed using our [Zookeeper Operator](https://github.com/pravega/zookeeper-operator).
- [Pravega Operator](https://github.com/pravega/pravega-operator/edit/master/README.md)manages Pravega clusters deployed to Kubernetes and automates tasks related to operating a Pravega cluster.

## Usage

### Install the Pravega Operator

> Note: If you are running on Google Kubernetes Engine (GKE), please [check this first](#installation-on-google-kubernetes-engine).

Run the following command to install the `PravegaCluster` custom resource definition (CRD), create the `pravega-operator` service account, roles, bindings, and the deploy the Pravega Operator.

```
$ kubectl create -f deploy
```

Verify that the Pravega Operator is running.

```
$ kubectl get deploy
NAME                 DESIRED   CURRENT   UP-TO-DATE   AVAILABLE   AGE
pravega-operator     1         1         1            1           17s
```

### Deploy a sample Pravega cluster

Pravega requires a long term storage provider known as Tier 2 storage. The following Tier 2 storage providers are supported:

- Filesystem (NFS)
- [Google Filestore](#using-google-filestore-storage-as-tier-2)
- [DellEMC ECS](https://www.dellemc.com/sr-me/storage/ecs/index.htm)
- HDFS (must support Append operation)

The following example uses an NFS volume provisioned by the [NFS Server Provisioner](https://github.com/kubernetes/charts/tree/master/stable/nfs-server-provisioner) helm chart to provide Tier 2 storage.

```
$ helm install stable/nfs-server-provisioner
```

Verify that the `nfs` storage class is now available.

```
$ kubectl get storageclass
NAME                 PROVISIONER                                             AGE
nfs                  cluster.local/elevated-leopard-nfs-server-provisioner   24s
...
```

> Note: This is ONLY intended as a demo and should NOT be used for production deployments.

Once the NFS server provisioner is installed, you can create a `PersistentVolumeClaim` that will be used as Tier 2 for Pravega. Create a `pvc.yaml` file with the following content.

```yaml
kind: PersistentVolumeClaim
apiVersion: v1
metadata:
  name: pravega-tier2
spec:
  storageClassName: "nfs"
  accessModes:
    - ReadWriteMany
  resources:
    requests:
      storage: 50Gi
```

```
$ kubectl create -f pvc.yaml
```
Use the following YAML template to install a small development Pravega Cluster (3 Bookies, 1 Controller, 3 Segment Stores). Create a `pravega.yaml` file with the following content.

```yaml
apiVersion: "pravega.pravega.io/v1alpha1"
kind: "PravegaCluster"
metadata:
  name: "pravega"
spec:
  zookeeperUri: [ZOOKEEPER_HOST]:2181

  bookkeeper:
    image:
      repository: pravega/bookkeeper
      tag: 0.4.0
      pullPolicy: IfNotPresent

    replicas: 3

    storage:
      ledgerVolumeClaimTemplate:
        accessModes: [ "ReadWriteOnce" ]
        storageClassName: "standard"
        resources:
          requests:
            storage: 10Gi

      journalVolumeClaimTemplate:
        accessModes: [ "ReadWriteOnce" ]
        storageClassName: "standard"
        resources:
          requests:
            storage: 10Gi

    autoRecovery: true

  pravega:
    controllerReplicas: 1
    segmentStoreReplicas: 3

    cacheVolumeClaimTemplate:
      accessModes: [ "ReadWriteOnce" ]
      storageClassName: "standard"
      resources:
        requests:
          storage: 20Gi

    image:
      repository: pravega/pravega
      tag: 0.4.0
      pullPolicy: IfNotPresent

    tier2:
      filesystem:
        persistentVolumeClaim:
          claimName: pravega-tier2
```

where:

- `[ZOOKEEPER_HOST]` is the host or IP address of your Zookeeper deployment.

Deploy the Pravega cluster.

```
$ kubectl create -f pravega.yaml
```

Verify that the cluster instances and its components are running.

```
$ kubectl get PravegaCluster
NAME      AGE
pravega   27s
```

```
$ kubectl get all -l pravega_cluster=pravega
NAME                                DESIRED   CURRENT   UP-TO-DATE   AVAILABLE   AGE
deploy/pravega-pravega-controller   1         1         1            1           1m

NAME                                       DESIRED   CURRENT   READY     AGE
rs/pravega-pravega-controller-7489c9776d   1         1         1         1m

NAME                                DESIRED   CURRENT   UP-TO-DATE   AVAILABLE   AGE
deploy/pravega-pravega-controller   1         1         1            1           1m

NAME                                       DESIRED   CURRENT   READY     AGE
rs/pravega-pravega-controller-7489c9776d   1         1         1         1m

NAME                                DESIRED   CURRENT   AGE
statefulsets/pravega-bookie         3         3         1m
statefulsets/pravega-segmentstore   3         3         1m

NAME                                             READY     STATUS    RESTARTS   AGE
po/pravega-bookie-0                              1/1       Running   0          1m
po/pravega-bookie-1                              1/1       Running   0          1m
po/pravega-bookie-2                              1/1       Running   0          1m
po/pravega-pravega-controller-7489c9776d-lcw9x   1/1       Running   0          1m
po/pravega-segmentstore-0                        1/1       Running   0          1m
po/pravega-segmentstore-1                        1/1       Running   0          1m
po/pravega-segmentstore-2                        1/1       Running   0          1m

NAME                             TYPE        CLUSTER-IP     EXTERNAL-IP   PORT(S)              AGE
svc/pravega-bookie-headless      ClusterIP   None           <none>        3181/TCP             1m
svc/pravega-pravega-controller   ClusterIP   10.3.255.239   <none>        10080/TCP,9090/TCP   1m
```

A `PravegaCluster` instance is only accessible WITHIN the cluster (i.e. no outside access is allowed) using the following endpoint in
the PravegaClient.

```
tcp://<cluster-name>-pravega-controller.<namespace>:9090
```

The `REST` management interface is available at:

```
http://<cluster-name>-pravega-controller.<namespace>:10080/
```

[Check this](#direct-access-to-the-cluster) to enable direct access to the cluster for development purposes.

### Uninstall the Pravega cluster

```
$ kubectl delete -f pravega.yaml
$ kubectl delete -f pvc.yaml
```

### Uninstall the Pravega Operator

> Note that the Pravega clusters managed by the Pravega operator will NOT be deleted even if the operator is uninstalled.

To delete all clusters, delete all cluster CR objects before uninstalling the Pravega Operator.

```
$ kubectl delete -f deploy
```

## Configuration

### Use non-default service accounts

You can optionally configure non-default service accounts for the Bookkeeper, Pravega Controller, and Pravega Segment Store pods.

For BookKeeper, set the `serviceAccountName` field under the `bookkeeper` block.

```
...
spec:
  bookkeeper:
    serviceAccountName: bk-service-account
...
```

For Pravega, set the `controllerServiceAccountName` and `segmentStoreServiceAccountName` fields under the `pravega` block.

```
...
spec:
  pravega:
    controllerServiceAccountName: ctrl-service-account
    segmentStoreServiceAccountName: ss-service-account
...
```

### Installing on a Custom Namespace with RBAC enabled

Create the namespace.

```
$ kubectl create namespace pravega-io
```

Update the namespace configured in the `deploy/role_binding.yaml` file.

```
$ sed -i -e 's/namespace: default/namespace: pravega-io/g' deploy/role_binding.yaml
```

Apply the changes.

```
$ kubectl -n pravega-io apply -f deploy
```

Note that the Pravega Operator only monitors the `PravegaCluster` resources which are created in the same namespace, `pravega-io` in this example. Therefore, before creating a `PravegaCluster` resource, make sure an Operator exists in that namespace.

```
$ kubectl -n pravega-io create -f example/cr.yaml
```

```
$ kubectl -n pravega-io get pravegaclusters
NAME      AGE
pravega   28m
```

```
$ kubectl -n pravega-io get pods -l pravega_cluster=pravega
NAME                                          READY     STATUS    RESTARTS   AGE
pravega-bookie-0                              1/1       Running   0          29m
pravega-bookie-1                              1/1       Running   0          29m
pravega-bookie-2                              1/1       Running   0          29m
pravega-pravega-controller-6c54fdcdf5-947nw   1/1       Running   0          29m
pravega-pravega-segmentstore-0                1/1       Running   0          29m
pravega-pravega-segmentstore-1                1/1       Running   0          29m
pravega-pravega-segmentstore-2                1/1       Running   0          29m
```

### Use Google Filestore Storage as Tier 2

1. [Create a Google Filestore](https://console.cloud.google.com/filestore/instances).

> Refer to https://cloud.google.com/filestore/docs/accessing-fileshares for more information


2. Create a `pv.yaml` file with the `PersistentVolume` specification to provide Tier 2 storage.

```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: pravega-volume
spec:
  capacity:
    storage: 1T
  accessModes:
  - ReadWriteMany
  nfs:
    path: /[FILESHARE]
    server: [IP_ADDRESS]
```

where:

- `[FILESHARE]` is the name of the fileshare on the Cloud Filestore instance (e.g. `vol1`)
- `[IP_ADDRESS]` is the IP address for the Cloud Filestore instance (e.g. `10.123.189.202`)


3. Deploy the `PersistentVolume` specification.

```
$ kubectl create -f pv.yaml
```

4. Create and deploy a `PersistentVolumeClaim` to consume the volume created.

```yaml
kind: PersistentVolumeClaim
apiVersion: v1
metadata:
  name: pravega-tier2
spec:
  storageClassName: ""
  accessModes:
    - ReadWriteMany
  resources:
    requests:
      storage: 50Gi
```

```
$ kubectl create -f pvc.yaml
```

Use the same `pravega.yaml` above to deploy the Pravega cluster.


### Tune Pravega configuration

Pravega has many configuration options for setting up metrics, tuning, etc. The available options can be found
[here](https://github.com/pravega/pravega/blob/master/config/config.properties) and are
expressed through the `pravega/options` part of the resource specification. All values must be expressed as Strings.

```yaml
...
spec:
  pravega:
    options:
      metrics.enableStatistics: "true"
      metrics.statsdHost: "telegraph.default"
      metrics.statsdPort: "8125"
...
```

## Releases  

The latest Pravega releases can be found on the [Github Release](https://github.com/pravega/pravega-operator/releases) project page.
