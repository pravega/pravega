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
# Deploying Pravega on Kubernetes 101

We show you how to deploy your "first Pravega cluster in Kubernetes". We provide a step-by-step guide 
to deploy Pravega in both Google Kubernetes Engine (GKE) and Amazon Elastic Kubernetes Service (EKS). 
Our goal is to keep things as simple as possible, and, at the same time, provide you with valuable 
insights on the services that form a Pravega cluster and the operators we developed to deploy them. 

## Creating and Setting Up the Kubernetes Cluster
First, we need to create the Kubernetes cluster to deploy Pravega. We assume as a pre-requisite that 
you have an account with at least one of the cloud providers mentioned above. If you already have an account 
for Google Cloud and/or AWS, then it is time to create a Kubernetes cluster for Pravega.

## GKE
Creating a Kubernetes cluster in GKE is straightforward. The defaults in general are enough for running a 
demo Pravega cluster, but we suggest just a couple of setting changes to deploy Pravega:

1. Go to `Kubernetes Engine` drop-down menu and select `Clusters > Create Cluster` option.
2. Pick a name for your Kubernetes cluster (i.e., `pravega-gke`).
3. As an important point, in `Master version` section you should select a Kubernetes version 1.15. 
The reason is that we are going to exercise the latest Pravega and Bookkeeper Operators, which requires 
Kubernetes version 1.15+.
4. Also, as the Pravega cluster consists of several services, we need to select a slightly larger node 
flavor compared to the default one. Thus, go to `default-pool > Nodes > Machine type` and select `n1-standard-4` 
nodes (4vCPUs, 15GB of RAM) and select 4 nodes instead of 3 (default). Note that this deployment is still 
accessible with the trial account.
5. Press the `Create` button, and that’s it.

Note that we use the Cloud Shell provided by GKE to deploy Pravega from the browser itself without installing 
locally any CLI (but feel free to use the Google Cloud CLI instead).

Pravega and Bookkeeper Operators also [require elevated privileges](https://github.com/pravega/bookkeeper-operator/blob/master/doc/development.md#installation-on-google-kubernetes-engine)
in order to watch for custom resources. For this reason, in GKE you need as a pre-requisite to grant those 
permissions first by executing:

```
kubectl create clusterrolebinding cluster-admin-binding --clusterrole=cluster-admin --user=$(gcloud config get-value core/account)
```

## EKS

In the case of AWS, we are going to use the EKS CLI, which automates and simplifies different aspects of the 
cluster creation and configuration (e.g., VPC, subnets, etc.). You will need to [install and configure the 
EKS CLI](https://docs.aws.amazon.com/eks/latest/userguide/getting-started-eksctl.html) before proceeding with 
the cluster creation.

Once the EKS CLI is installed, we just require one command to create an EKS cluster:
 
```
eksctl create cluster \
--name pravega-eks \
--region us-west-2 \
--nodegroup-name standard-workers \
--node-type t3.xlarge \
--nodes 3 \
--nodes-min 1 \
--nodes-max 4 \
--ssh-access \
--ssh-public-key ~/.ssh/pravega_aws.pub \
--managed
```

Similar to the GKE case, the previous command uses a larger node type compared to the default one 
(`--node-type t3.xlarge`). Note that the `--ssh-public-key` parameter expects a public key that has 
been generated when installing the AWS CLI to securely connect with your cluster (for more info, 
please [read this document](https://docs.aws.amazon.com/cli/latest/userguide/cli-services-ec2-keypairs.html#creating-a-key-pair)). 
Also, take into account that the region for the EKS cluster should match the configured region in your AWS CLI.

Now, we are ready to prepare our Kubernetes cluster for the installation of Pravega.

## Install Helm

To simplify the deployment of Pravega, we use [Helm charts](https://helm.sh/). You will need to [install a 
Helm 3](https://helm.sh/docs/intro/install/) client to proceed with the installation instructions in this blog post.

Once you install the Helm client, you just need to get the public charts we provide to deploy a Pravega cluster:

```
helm repo add pravega https://charts.pravega.io
helm repo update
```

## Webhook conversion and Cert-Manager

The most recent versions of Pravega Operator resort to the
[Webhook Conversion feature](https://kubernetes.io/docs/tasks/extend-kubernetes/custom-resources/custom-resource-definition-versioning/#webhook-conversion). For this reason, Cert-Manager or some other certificate management solution must be
deployed for managing webhook service certificates. To install Cert-Manager, just execute this command:
```
kubectl apply -f https://github.com/jetstack/cert-manager/releases/latest/download/cert-manager.yaml
```

##Deploying Pravega

Next, we show you step by step how to deploy Pravega, which involves the deployment of Apache Zookeeper, 
Bookkeeper (journal), and Pravega (as well as their respective Operators). Also, given that Pravega moves 
"cold" data to what we call long-term storage (a.k.a Tier 2), we need to instantiate a storage backend 
for such purpose.

## Apache Zookeeper

[Apache Zookeeper](https://zookeeper.apache.org/) is a distributed system that provides reliable coordination 
services, such as consensus and group management. Pravega uses Zookeeper to store specific pieces of metadata as 
well as to offer a consistent view of data structures used by multiple service instances.

As part of the Pravega project, we have developed a [Zookeeper Operator](https://github.com/pravega/zookeeper-operator) 
to manage the deployment of Zookeeper clusters in Kubernetes. Thus, deploying the Zookeeper Operator is the first step 
to deploy Zookeeper:

```
helm install zookeeper-operator pravega/zookeeper-operator
```

With the Zookeeper Operator up and running, the next step is to deploy Zookeeper. We can do so with the helm chart we 
published for Zookeeper: 

```
helm install zookeeper pravega/zookeeper
```

This chart instantiates a Zookeeper cluster made of 3 instances and their respective Persistent Volume Claims (PVC) 
of 20GB of storage each, which is enough for a demo Pravega cluster.

Once the previous command has been executed, you can see both Zookeeper Operator and Zookeeper running in the 
cluster:

```console
$ kubectl get pods
NAME                                  READY   STATUS    RESTARTS   AGE
zookeeper-0                           1/1     Running   0          3m46s
zookeeper-1                           1/1     Running   0          3m6s
zookeeper-2                           1/1     Running   0          2m25s
zookeeper-operator-6b9759bbcb-9j25s   1/1     Running   0          4m
```

## Apache Bookkeeper
[Apache Bookkeeper](https://bookkeeper.apache.org/) is a distributed and reliable storage system that provides 
a distributed log abstraction. Bookkeeper excels on achieving low latency, append-only writes. This 
is the reason why Pravega uses Bookkeeper for journaling: Pravega writes data to Bookkeeper, which provides low latency, 
persistent, and replicated storage for stream appends. Pravega uses the data in BookKeeper to recover from failures, 
and that data is truncated once it is flushed to tiered long-term storage.

As in the case of Zookeeper, we have also developed a [Bookkeeper Operator](https://github.com/pravega/bookkeeper-operator) 
to manage the lifecycle of Bookkeeper clusters deployed in Kubernetes. Thus, the next step is to deploy the Bookkeeper Operator:

```
kubectl apply -f https://github.com/pravega/bookkeeper-operator/raw/master/config/certmanager/certificate.yaml
helm install bookkeeper-operator pravega/bookkeeper-operator --set webhookCert.certName=selfsigned-cert-bk --set webhookCert.secretName=selfsigned-cert-tls-bk
```

Once running, we can proceed to deploy Bookkeeper. In this case, we will use the Helm chart publicly available to quickly 
spin up a Bookkeeper cluster:

```
helm install bookkeeper pravega/bookkeeper
```

As a result, you can see below both Zookeeper and Bookkeeper up and running:

```console
$ kubectl get pods
NAME                                   READY   STATUS    RESTARTS   AGE
bookkeeper-bookie-0                    1/1     Running   0          2m10s
bookkeeper-bookie-1                    1/1     Running   0          2m10s
bookkeeper-bookie-2                    1/1     Running   0          2m10s
bookkeeper-operator-85568f8949-d652z   1/1     Running   0          4m10s
zookeeper-0                            1/1     Running   0          8m59s
zookeeper-1                            1/1     Running   0          8m19s
zookeeper-2                            1/1     Running   0          7m38s
zookeeper-operator-6b9759bbcb-9j25s    1/1     Running   0          9m13s
```

## Long-Term Storage
We mentioned before that Pravega automatically [moves data to Long-Term Storage](http://pravega.io/docs/latest/segment-store-service/#synchronization-with-tier-2-storage-writer) 
(or Tier 2). This feature is very interesting, because it positions Pravega in a "sweet spot" in the latency vs 
throughput trade-off: Pravega achieves low latency writes by using Bookkeeper for appends. At the same time, 
it also provides high throughput reads when accessing historical data.

As our goal is to keep things as simple as possible, we deploy a simple storage option: the NFS Server provisioner. 
With such a provisioner, we have a pod that acts as an NFS Server for Pravega. To deploy it, you need to execute 
the next command:

```
helm repo add stable https://charts.helm.sh/stable
helm install stable/nfs-server-provisioner --generate-name
```

Once the NFS Server provisioner is up and running, Pravega will require a PVC for long-term storage pointing to the 
NFS Server provisioner that we have just deployed. To create the PVC, you can just copy the following manifest 
(namely `tier2_pvc.yaml`):

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
      storage: 5Gi 
```

And create the PVC for long-term storage as follows:

```
kubectl apply -f tier2_pvc.yaml 
```

As you may notice, the long-term storage option suggested in this post is just for demo purposes, just to keep 
things simple. But, if you really want to have a real Pravega cluster running in the cloud, then we suggest you 
to use actual storage services like FileStore in GKE and EFS in AWS. There are instructions on [how to deploy 
production long-term storage options](https://github.com/pravega/pravega-operator/blob/master/doc/longtermstorage.md)
in the documentation of Pravega Operator.

## Pravega
We are almost there! The last step is to deploy Pravega Operator and Pravega, pretty much as what we have 
already done for Zookeeper and Bookkeeper. As usual, we first need to deploy the Pravega Operator 
(and its required certificate) as follows:

```
kubectl apply -f https://github.com/pravega/pravega-operator/raw/master/config/certmanager/certificate.yaml
helm install pravega-operator pravega/pravega-operator --set webhookCert.certName=selfsigned-cert --set webhookCert.secretName=selfsigned-cert-tls
```

Once deployed, we can deploy Pravega with the default Helm chart publicly available as follows:

```
helm install pravega pravega/pravega
```

That's it! Once this command gets executed, you will have your first Pravega cluster up and running:

```console
$ kubectl get pods
NAME                                         READY   STATUS    RESTARTS  AGE
bookkeeper-bookie-0                          1/1     Running   0         9m6s
bookkeeper-bookie-1                          1/1     Running   0         9m6s
bookkeeper-bookie-2                          1/1     Running   0         9m6s
bookkeeper-operator-85568f8949-d652z         1/1     Running   0         11m
nfs-server-provisioner-1592297085-0          1/1     Running   0         5m26s
pravega-operator-6c6d9db459-mpjr4            1/1     Running   0         4m19s
pravega-pravega-controller-5b447c85b-t8jsx   1/1     Running   0         2m56s
pravega-pravega-segment-store-0              1/1     Running   0         2m56s
zookeeper-0                                  1/1     Running   0         15m
zookeeper-1                                  1/1     Running   0         15m
zookeeper-2                                  1/1     Running   0         14m
zookeeper-operator-6b9759bbcb-9j25s          1/1     Running   0         16m
```

##Executing a Sample Application

Finally, we would like to help you to exercise the Pravega cluster you just deployed. Let’s deploy a pod in 
our Kubernetes cluster to run samples and applications, like the one we propose in the manifest below 
(`test-pod.yaml`):

```yaml
kind: Pod 
apiVersion: v1 
metadata: 
  name: test-pod 
spec: 
  containers: 
    - name: test-pod 
      image: ubuntu:18.04 
      args: [bash, -c, 'for ((i = 0; ; i++)); do echo "$i: $(date)"; sleep 100; done'] 
```

You can directly use this manifest and create your Ubuntu 18.04 pod as follows:

```
kubectl create -f test-pod.yaml 
```

Once the pod is up and running, we suggest you to login into the pod and build the [Pravega samples](https://github.com/pravega/pravega-samples) 
to interact with the Pravega cluster by executing the following commands:

```
kubectl exec -it test-pod -- /bin/bash 
apt-get update 
apt-get -y install git-core openjdk-8-jdk 
git clone -b r0.8 https://github.com/pravega/pravega-samples 
cd pravega-samples 
./gradlew installDist 
```

With this, we can go to the location where the Pravega samples executable files have been generated and execute one of them, 
making sure that we point to the Pravega Controller service:

```

cd pravega-client-examples/build/install/pravega-client-examples/ 
bin/consoleWriter -u tcp://pravega-pravega-controller:9090
```

That’s it, you have executed your first sample against the Pravega cluster! With the `consoleWriter`, 
you will be able to write to Pravega regular events or transactions. We also encourage you to execute 
on another terminal the `consoleReader`, so you will see how events are both written and read at the same 
time (for more info, see the [Pravega samples documentation](https://github.com/pravega/pravega-samples/tree/master/pravega-client-examples#consolerw)). 
There are many other interesting samples for Pravega in the repository, so please be curious and try them out.

##What is next?
This guide (also available in this [blog post](https://blog.pravega.io/2020/06/20/deploying-pravega-in-kubernetes/))
provides a high-level overview on how to deploy Pravega on Kubernetes. But there is much more to learn! We 
suggest you to continue exploring Pravega with the following documents:

- [Deploying Pravega](../deployment/deployment.md): A more advanced guide on how to deploy Pravega on Kubernetes
and other environments.
- [Developer Guide](../beginner_dev_guide.md): Start creating your own applications using Pravega.
- [Understanding Pravega](../pravega-concepts.md): Be curious to understand the concepts and design choices behind
Pravega.