<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->

# Deploying in Kubernetes

## Table of Contents

 * [Requirements](#requirements)
    * [Pravega Operator](#pravega-operator)
 * [Usage](#usage)    
    * [Installation of the Pravega Operator](#install-the-pravega-operator)
    * [Deploy a sample Pravega Cluster](#deploy-a-sample-pravega-cluster)
    * [Scale a Pravega Cluster](#scale-a-pravega-cluster)
    * [Upgrade a Pravega Cluster](#upgrade-a-pravega-cluster)
    * [Uninstall the Pravega Cluster](#uninstall-the-pravega-cluster)
    * [Uninstall the Pravega Operator](#uninstall-the-pravega-operator)
 * [Configuration](#configuration)
    * [Use non-default service accounts](#use-non-default-service-accounts)
    * [Installing on a Custom Namespace with RBAC enabled](#installing-on-a-custom-namespace-with-rbac-enabled)
    * [Tier 2: Google Filestore Storage](#use-google-filestore-storage-as-tier-2)
    * [Tune Pravega Configurations](#tune-pravega-configuration)
    * [Enable external access](#enable-external-access)
* [Releases](#releases)

## Requirements

- Kubernetes 1.8+
- An existing Apache Zookeeper 3.5 cluster. This can be easily deployed using our [Zookeeper Operator](https://github.com/pravega/zookeeper-operator).
- [Pravega Operator](https://github.com/pravega/pravega-operator/edit/master/README.md) manages Pravega clusters deployed to Kubernetes and automates tasks related to operating a Pravega cluster.

## Usage

### Install the Pravega Operator

> Note: If you are running on Google Kubernetes Engine (GKE), please [check this first](https://github.com/pravega/pravega-operator#installation-on-google-kubernetes-engine).

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
  name: "example"
spec:
  version: 0.4.0
  zookeeperUri: [ZOOKEEPER_HOST]:2181

  bookkeeper:
    replicas: 3
    image:
      repository: pravega/bookkeeper
    autoRecovery: true

  pravega:
    controllerReplicas: 1
    segmentStoreReplicas: 3
    image:
      repository: pravega/pravega
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

Verify that the cluster instances and its components are being created.

```
$ kubectl get PravegaCluster
NAME      VERSION   DESIRED MEMBERS   READY MEMBERS   AGE
example   0.4.0     7                 0               25s
```

After a couple of minutes, all cluster members should become ready.

```
$ kubectl get PravegaCluster
NAME      VERSION   DESIRED MEMBERS   READY MEMBERS   AGE
example   0.4.0     7                 7               2m
```

```
$ kubectl get all -l pravega_cluster=example
NAME                                              READY   STATUS    RESTARTS   AGE
pod/example-bookie-0                              1/1     Running   0          2m
pod/example-bookie-1                              1/1     Running   0          2m
pod/example-bookie-2                              1/1     Running   0          2m
pod/example-pravega-controller-64ff87fc49-kqp9k   1/1     Running   0          2m
pod/example-pravega-segmentstore-0                1/1     Running   0          2m
pod/example-pravega-segmentstore-1                1/1     Running   0          1m
pod/example-pravega-segmentstore-2                1/1     Running   0          30s

NAME                                            TYPE        CLUSTER-IP    EXTERNAL-IP   PORT(S)              AGE
service/example-bookie-headless                 ClusterIP   None          <none>        3181/TCP             2m
service/example-pravega-controller              ClusterIP   10.23.244.3   <none>        10080/TCP,9090/TCP   2m
service/example-pravega-segmentstore-headless   ClusterIP   None          <none>        12345/TCP            2m

NAME                                                    DESIRED   CURRENT   READY   AGE
replicaset.apps/example-pravega-controller-64ff87fc49   1         1         1       2m

NAME                                            DESIRED   CURRENT   AGE
statefulset.apps/example-bookie                 3         3         2m
statefulset.apps/example-pravega-segmentstore   3         3         2m
```

By default, a `PravegaCluster` instance is only accessible within the cluster through the Controller `ClusterIP` service. From within the Kubernetes cluster, a client can connect to Pravega at:

```
tcp://<pravega-name>-pravega-controller.<namespace>:9090
```

And the `REST` management interface is available at:

```
http://<pravega-name>-pravega-controller.<namespace>:10080/
```

[Check this](#enable-external-access) to enable external access to a Pravega cluster.


### Scale a Pravega Cluster

You can scale Pravega components independently by modifying their corresponding field in the Pravega resource spec. You can either `kubectl edit` the cluster or `kubectl patch` it. If you edit it, update the number of replicas for BookKeeper, Controller, and/or Segment Store and save the updated spec.

Example of patching the Pravega resource to scale the Segment Store instances to 4.

```
kubectl patch PravegaCluster example --type='json' -p='[{"op": "replace", "path": "/spec/pravega/segmentStoreReplicas", "value": 4}]'
```

### Upgrade a Pravega Cluster

Check out the [Upgrade Guide](https://github.com/pravega/pravega-operator/blob/master/doc/upgrade-cluster.md).

### Uninstall the Pravega cluster

```
$ kubectl delete -f pravega.yaml
$ kubectl delete -f pvc.yaml
```


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

If external access is enabled in your Pravega cluster, Segment Store pods will require access to some Kubernetes API endpoints to obtain the external IP and port. Make sure that the service account you are using for the Segment Store has, at least, the following permissions.

```
kind: Role
apiVersion: rbac.authorization.k8s.io/v1
metadata:
  name: pravega-components
  namespace: "pravega-namespace"
rules:
- apiGroups: ["pravega.pravega.io"]
  resources: ["*"]
  verbs: ["get"]
- apiGroups: [""]
  resources: ["pods", "services"]
  verbs: ["get"]
---
kind: ClusterRole
apiVersion: rbac.authorization.k8s.io/v1
metadata:
  name: pravega-components
rules:
- apiGroups: [""]
  resources: ["nodes"]
  verbs: ["get"]
```

Replace the `namespace` with your own namespace.


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
### Enable external access

By default, a Pravega cluster uses `ClusterIP` services which are only accessible from within Kubernetes. However, when creating the Pravega cluster resource, you can opt to enable external access.

In Pravega, clients initiate the communication with the Pravega Controller, which is a stateless component frontended by a Kubernetes service that load-balances the requests to the backend pods. Then, clients discover the individual Segment Store instances to which they directly read and write data to. Clients need to be able to reach each and every Segment Store pod in the Pravega cluster.

If your Pravega cluster needs to be consumed by clients from outside Kubernetes (or from another Kubernetes deployment), you can enable external access in two ways, depending on your environment constraints and requirements. Both ways will create one service for all Controllers, and one service for each Segment Store pod.

1. Via [`LoadBalancer`](https://kubernetes.io/docs/concepts/services-networking/service/#loadbalancer) service type.
2. Via [`NodePort`](https://kubernetes.io/docs/concepts/services-networking/service/#nodeport) service type.

For more information, Please check [Kubernetes documentation](https://kubernetes.io/docs/concepts/services-networking/service/#publishing-services-service-types).

Example of configuration for using `LoadBalancer` service types:

```yaml
...
spec:
  externalAccess:
    enabled: true
    type: LoadBalancer
...
```

Clients will need to connect to the external Controller address and will automatically discover the external address of all Segment Store pods.

## Releases  

The latest Pravega releases can be found on the [GitHub Release](https://github.com/pravega/pravega-operator/releases) project page.
