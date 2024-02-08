# Deploy Pravega on AWS EKS with local drives

Deploying Pravega on AWS EKS with local drives is an interesting approach, specially for benchmarking purposes.
This document provides the instructions to instantiate such a deployment:

1. Install the AWS `eksclt` tool and create an SSH key pair (namely, `~/.ssh/pravega_aws.pub`).
2. We provide the scripts to automate the deployment (`deploy_pravega.py` and `undeploy_pravega.py`).
In particular, the `pravega.yaml` assumes AWS S3 as long-term storage. To complete the configuration,
you will need to create a bucket (in the same AWS region as the EKS cluster) named `pravega-tier2`, or change
the `s3.bucket` parameter to the bucket you prefer. Moreover, to allow Pravega accessing the S3 bucket, 
you will need to replace the `YOUR_AWS_ACCESS_KEY_ID` and `YOUR_AWS_SECRET_ACCESS_KEY` strings with you
access and secret keys in `pravega.yaml`, respectively.
3. Create the EKS cluster using `eksctl`:
```
eksctl create cluster --name pravega --region us-east-1 --node-type i3en.2xlarge --nodes 3 --ssh-access --ssh-public-key ~/.ssh/pravega_aws.pub
```
The instruction above creates a 3-node EKS cluster named `pravega` on region `us-east-1` with worker
instances of type `i3en.2xlarge`. The output you should see after executing the above command may look like:
```
2-01 15:13:12 [ℹ]  eksctl version 0.166.0
2024-02-01 15:13:12 [ℹ]  using region us-east-1
...
2024-02-01 15:28:55 [ℹ]  kubectl command should work with "/home/raul/.kube/config", try 'kubectl get nodes'
2024-02-01 15:28:55 [✔]  EKS cluster "pravega" in "us-east-1" region is ready
```
4. We need to add permissions to the EKS cluster for instantiating EBS storage volumes. To this end, one approach is to
go to the IAM service in your AWS console. There, you will see the roles created for the EKS cluster (e.g.,
`eksctl-pravega-cluster-ServiceRole-xxx`, `eksctl-pravega-nodegroup-ng-xxx`). Go to these roles and add to them
the `AmazonEBSCSIDriverPolicy` policy. If that step alone does not allow automatic volume provisioning in your cluster,
you could also execute the following steps for granting `AmazonEBSCSIDriverPolicy` permissions to your EKS cluster:
```
eksctl utils associate-iam-oidc-provider --cluster pravega --approve
eksctl create iamserviceaccount \
  --region us-east-1 \
  --name ebs-csi-controller-sa \
  --namespace kube-system \
  --cluster pravega \
  --attach-policy-arn arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy \
  --approve \
  --role-only \
  --role-name AmazonEKS_EBS_CSI_DriverRole
eksctl create addon --name aws-ebs-csi-driver --cluster pravega --service-account-role-arn arn:aws:iam::$(aws sts get-caller-identity --query Account --output text):role/AmazonEKS_EBS_CSI_DriverRole --force
```
5. Run the Pravega deployment script:
```
python deploy_pravega.py
```
The above command will perform all the steps for deploying Pravega on your EKS cluster. The logs should look like:
```
storageclass.storage.k8s.io/standard created
storageclass.storage.k8s.io/bookie-journal created
storageclass.storage.k8s.io/bookie-ledger created
...
pravegacluster.pravega.pravega.io/pravega created
pod/influxdb created
service/influxdb-service exposed
deployment.apps/grafana created
```

After that, you should be able to check your Pravega cluster pods:
```
NAME                                          READY   STATUS    RESTARTS   AGE     IP               NODE                             NOMINATED NODE   READINESS GATES
bookkeeper-bookie-0                           1/1     Running   0          97s     192.168.40.239   ip-192-168-55-115.ec2.internal   <none>           <none>
bookkeeper-bookie-1                           1/1     Running   0          97s     192.168.19.18    ip-192-168-26-6.ec2.internal     <none>           <none>
bookkeeper-bookie-2                           1/1     Running   0          97s     192.168.48.254   ip-192-168-62-99.ec2.internal    <none>           <none>
bookkeeper-operator-c668678b7-28md7           1/1     Running   0          2m21s   192.168.52.193   ip-192-168-55-115.ec2.internal   <none>           <none>
grafana-69fd74886c-4cg2d                      1/1     Running   0          32s     192.168.50.92    ip-192-168-55-115.ec2.internal   <none>           <none>
influxdb                                      1/1     Running   0          35s     192.168.60.111   ip-192-168-55-115.ec2.internal   <none>           <none>
pravega-operator-649749f968-vbgw6             1/1     Running   0          89s     192.168.49.162   ip-192-168-55-115.ec2.internal   <none>           <none>
pravega-pravega-controller-67844c689d-pfxm4   0/1     Running   0          37s     192.168.41.204   ip-192-168-62-99.ec2.internal    <none>           <none>
pravega-pravega-segment-store-0               1/1     Running   0          36s     192.168.5.174    ip-192-168-26-6.ec2.internal     <none>           <none>
zookeeper-0                                   1/1     Running   0          3m42s   192.168.25.34    ip-192-168-26-6.ec2.internal     <none>           <none>
zookeeper-operator-77fdb75969-jfp8c           1/1     Running   0          8m19s   192.168.20.212   ip-192-168-26-6.ec2.internal     <none>           <none>
```

More interestingly, we see that Bookkeeper pods are using the local drives of `ì3` type instances 
(via Rancher local volume provisioner):
```
AME                          STATUS   VOLUME                                     CAPACITY   ACCESS MODES   STORAGECLASS     AGE
data-zookeeper-0              Bound    pvc-ef7e9979-648b-4378-836c-7827aab597a6   20Gi       RWO            standard         9m32s
index-bookkeeper-bookie-0     Bound    pvc-bb2617cd-6dd0-4a62-9789-a924c0d4ef3e   10Gi       RWO            standard         3m37s
index-bookkeeper-bookie-1     Bound    pvc-cd86e02d-9aeb-47a8-bb46-8016e2ad2e2c   10Gi       RWO            standard         3m37s
index-bookkeeper-bookie-2     Bound    pvc-b9dfab27-890d-41e3-9c00-9958a14587c9   10Gi       RWO            standard         3m37s
journal-bookkeeper-bookie-0   Bound    pvc-c7c11a2c-ac0b-4ad0-9899-874adc92ca65   1500Gi     RWO            bookie-journal   3m37s
journal-bookkeeper-bookie-1   Bound    pvc-caa919d7-7c9b-41ed-be8a-efc2bff7aa32   1500Gi     RWO            bookie-journal   3m37s
journal-bookkeeper-bookie-2   Bound    pvc-851e2ed2-6372-4ed8-b9fb-c8f53165dbe6   1500Gi     RWO            bookie-journal   3m37s
ledger-bookkeeper-bookie-0    Bound    pvc-749f6945-b251-4289-b550-c6b0a1709d01   1500Gi     RWO            bookie-ledger    3m37s
ledger-bookkeeper-bookie-1    Bound    pvc-f2e1baf5-28de-43a9-b6a7-8a0435b0e8f2   1500Gi     RWO            bookie-ledger    3m37s
ledger-bookkeeper-bookie-2    Bound    pvc-f194f012-8ce4-45d2-b862-727ea9d2dfdd   1500Gi     RWO            bookie-ledger    3m37s
```
These are the drives what we can observe inside a Bookie for journal and ledger:

```
[root@bookkeeper-bookie-0 bookkeeper]# df -h
Filesystem      Size  Used Avail Use% Mounted on
/dev/nvme2n1    2.3T  300K  2.2T   1% /home/ledger
/dev/nvme1n1    2.3T  608K  2.2T   1% /home/journal
```

To verify that the Pravega cluster works, you can run some workloads using 
the [Pravega Benchmark](https://github.com/pravega/pravega-benchmark):
```
root@ubuntu2:/home/pravega-benchmark/build/install/pravega-benchmark# bin/pravega-benchmark -controller tcp://pravega-pravega-controller:9090 -scope test -stream stream4 -segments 10 -producers 4 -size 100000 -throughput 500 -time 100
...
2024-02-01 15:03:53:511 +0000 [ForkJoinPool-1-worker-3] INFO io.pravega.perf.PerfStats -    30674 records Writing,    6132.3 records/sec, 584.83 MiB/sec,   504.1 ms avg latency,  2351.0 ms max latency
2024-02-01 15:03:58:424 +0000 [ForkJoinPool-1-worker-3] INFO io.pravega.perf.PerfStats - 518203 records Writing, 5184.882 records/sec, 100000 bytes record size, 494.47 MiB/sec, 229.7 ms avg latency, 2840.0 ms max latency, 34 ms 50th, 246 ms 75th, 1071 ms 95th, 1691 ms 99th, 2068 ms 99.9th, 2111 ms 99.99th.
```