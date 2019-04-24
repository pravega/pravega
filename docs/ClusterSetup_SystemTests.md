# System Tests Cluster Setup

## Create a new cluster 
1. Join the 'clusters' slack channel.

	a. To create a new test cluster:
	
	`!cc --pks --pks-env=nightshift2`

	b. To destroy a cluster 
	
	`!cd <cluster-name> --force`

	c. To list all clusters held by a user
	
	`!cl`

## Install necessary tools
2. On your local VM install the following:

a. Install Jarvis:

`$> curl -k https://asd-nautilus-jenkins.isus.emc.com/jenkins/job/nautilus-platform-master/lastStableBuild/artifact/go/go-cli/build/linux-jarvis > /usr/local/bin/jarvis`

`$> chmod +x /usr/local/bin/jarvis`

b. Install PKS
Download PKS CLI (Linux) from [here](https://network.pivotal.io/products/pivotal-container-service/)

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


## Setup Cluster for system tests

```$>jarvis save <cluster-name>```

 After adding all necessary entries into /etc/hosts, again: 
  ```
  $>jarvis save <cluster-name>

  $>kubectl config use-context <cluster-name>

  $> kubectl get pod --all-namespaces
  
  $ $> helm init --service-account tiller --wait --upgrade (first time )
  OR
  $> helm init –upgrade
  $> kubectl create serviceaccount --namespace kube-system tiller
  $> kubectl create clusterrolebinding tiller-cluster-rule --clusterrole=cluster-admin --serviceaccount=kube-system:tiller

  $> kubectl patch deploy --namespace kube-system tiller-deploy -p '{\"spec\":{\"template\":{\"spec\":{\"serviceAccount\":\"tiller\"}}}}'
  
  $> cd charts/stable/nfs-server-provisioner
  
  $> helm install --set nfs.server=10.249.249.220  --set nfs.path=/ifs --set storageClass.name=nfs --set nfs.mountOptions='{nolock,sec=sys,vers=4.0}' <path to charts/stable/nfs-server-provisioner>
  
  $> helm list
  $> kubectl create -f ./stable/nfs-client-provisioner/pvc.yaml
  $> kubectl get pvc
  $kubectl get storageclass
  ```
  
  
