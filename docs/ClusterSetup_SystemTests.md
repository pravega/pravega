# System Tests Cluster Setup

## Create a new cluster 
1. Join the 'clusters' slack channel.

	a. To create a new test cluster:
	
        `!cc --pks --pks-env=nightshift2`

	b. To destroy a cluster 
	
	`!cd <cluster-name> --force`

	c. To list all clusters held by a user
	
	`!cl`

## Install necessary  tools
2. On your local VM install jarvis, pks and kubectl:
a. Install Jarvis:
`
$> curl -k https://asd-nautilus-jenkins.isus.emc.com/jenkins/job/nautilus-platform-master/lastStableBuild/artifact/go/go-cli/build/linux-jarvis > /usr/local/bin/jarvis
$> chmod +x /usr/local/bin/jarvis
`

b. Install PKS
Download PKS CLI (Linux) from [here] (https://network.pivotal.io/products/pivotal-container-service/)

c. Install kubectl
https://kubernetes.io/docs/tasks/tools/install-kubectl/

d. Install helm

   `$> wget https://storage.googleapis.com/kubernetes-helm/helm-v2.10.0-linux-amd64.tar.gz`
   
    `$> tar -zxvf helm-v2.10.0-linux-amd64.tar.gz`
    
    `$> cp linux-amd64/helm /usr/sbin`
    
## Setup Cluster for system tests

`$>jarvis save <cluster-name>`

  After adding all necessary entries into /etc/hosts, again: 
  
`$>jarvis save <cluster-name>`

`$>kubectl config use-context <cluster-name>`

`$> kubectl get pod --all-namespaces`

