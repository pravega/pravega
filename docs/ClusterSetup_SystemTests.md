# System Tests Cluster Setup, Tear down & Debugging

## Cluster Setup

### Create a new cluster 
1. Join the 'clusters' slack channel.

	a. To create a new test cluster:
	
	`!cc --pks --pks-env=nightshift2`

	b. To destroy a cluster 
	
	`!cd <cluster-name> --force`

	c. To list all clusters held by a user
	
	`!cl`

### Install necessary tools
2. On your local VM install the following:

a. Install Jarvis:



b. Install PKS

Download and install PKS CLI (Linux) as documented [here](https://network.pivotal.io/products/pivotal-container-service/)

c. Install kubectl

Setup kubectl as documented [here](https://kubernetes.io/docs/tasks/tools/install-kubectl/)

d. Install helm
```
    $> wget https://storage.googleapis.com/kubernetes-helm/helm-v2.10.0-linux-amd64.tar.gz
   
    $> tar -zxvf helm-v2.10.0-linux-amd64.tar.gz
    
    $> cp linux-amd64/helm /usr/sbin
```
e. Clone the charts repo in some folder:
```
$> git clone https://github.com/OlegPS/charts.git
```

### Setup Cluster for system tests

  ```$> jarvis save <cluster-name>```

  After adding all necessary entries into /etc/hosts, again: 
  
  ```
  $> jarvis save <cluster-name>

  $> kubectl config use-context <cluster-name>

  $> kubectl get pod --all-namespaces
  
  $ $> helm init --service-account tiller --wait --upgrade (first time )
  
  OR just
  
  $> helm init 
  
  $> kubectl create serviceaccount --namespace kube-system tiller
  
  $> kubectl create clusterrolebinding tiller-cluster-rule --clusterrole=cluster-admin --serviceaccount=kube-system:tiller

  $> kubectl patch deploy --namespace kube-system tiller-deploy -p '{\"spec\":{\"template\":{\"spec\":{\"serviceAccount\":\"tiller\"}}}}'
  
  $> cd  <location of charts git clone>/charts/stable/nfs-server-provisioner
  
  $> helm install --set nfs.server=10.249.249.220  --set nfs.path=/ifs --set storageClass.name=nfs --set nfs.mountOptions='{nolock,sec=sys,vers=4.0}' ./charts/stable/nfs-client-provisioner
  
  $> helm list
  
  $> kubectl create -f ./stable/nfs-client-provisioner/pvc.yaml
  
  $> kubectl get pvc
  $> kubectl get storageclass
  ```
  Now the cluster is setup and you can run system tests using :
  
 ```./gradlew --info startK8SystemTests -DimageVersion=<image version #> -DimagePrefix=nautilus -DdockerRegistryUrl=devops-repo.isus.emc.com:8116```
   
   
## Cluster Tear Down:

Create a bash script with the following entries:

```
	kubectl delete PravegaCluster pravega
	
	kubectl delete zk zookeeper
	
	kubectl delete pvc data-zookeeper-0 ledger-pravega-bookie-0 ledger-pravega-bookie-1 ledger-pravega-bookie-2 cache-pravega-		segmentstore-0 cache-pravega-segmentstore-1 cache-pravega-segmentstore-2 journal-pravega-bookie-0 journal-pravega-bookie-1 		journal-pravega-bookie-2 pravega-tier2
	
	kubectl delete pods --field-selector=status.phase!=Running
	
	kubectl delete deployment pravega-operator
	
	kubectl delete deployment zookeeper-operator
	
	kubectl delete -f /path/to/chartsrepo/charts/stable/nfs-client-provisioner/pvc.yaml
	
	tier2name=`helm list | awk 'NR==2{print $1}'`
	
	echo "Deleting tier2 $tier2name"
	helm delete $tier2name
	sleep 5
	kubectl get pvc
	kubectl get storageclass
	kubectl get po
```

## Check test logs on jenkins cluster (husk/miek)
  
 1. Add this DNS record to your hosts file
 
`10.249.250.202 api-nightshift.ecs.lab.emc.com`

 2. Login to Nightshift
 
`pks login -a api-nightshift.ecs.lab.emc.com -u labadmin -p ChangeMe --skip-ssl-validation`

 3. Check pks clusters has cluster you want to check:
 
    `pks clusters`
 
 4. Point to your cluster of interest:
 
    `pks get-credentials <cluster-name>`
    
    `kubectl config use-context <cluster-name>`
    
    
 5. Confirm it works:
    `kubectl get po`
    
    `kubectl get pravegaclusters`
    
    `kubectl get pravegaclusters pravega -o yaml`
 
 6. To check logs:
 
 To write log for component/test to a file:
 
    `kubectl logs -f <podname> &> filename`
    
 To check directly
 
    `kubectl logs <podname> | less`
