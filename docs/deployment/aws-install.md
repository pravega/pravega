<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Running on AWS

Pre-reqs: Have an AWS account and have Terraform installed. To install and download Terraform, follow the instructions here:
https://www.terraform.io/downloads.html

### Deploy Steps
- Define the nodes layout in installer/hosts-template.

There are three sections of hosts-template:
- common-services is the section for zookeeper and bookkeeper
- pravega-controller is the section for pravega controller node
- pravega-hosts is the section for the pravega segment store node.

Here is an example of hosts-template:

[common-services]
N0 myid=0
N1 myid=1
N2 myid=2

[pravega-controller]
N0

[pravega-hosts]
N0
N1
N2

- Put you AWS keypair file under the installer directory
- Run "terraform apply" under the deployment directory, and then following the prompt instructions, enter the AWS account credentials.

Currently, we only support two regions: us-east-1 and us-west-1. We list below the instance types we recommend for them.

### Region us-east-1
- Three m3.xlarge for EMR
- Three m3.2xlarge for Pravega
- One m3.medium for bootstrap, also as client

### Region us-west-1:
- Three m3.xlarge for EMR
- Three i3.4xlarge for Pravega
- One i3.xlarge for bootstrap, also as client

Other instance type mights present conflicts with the Linux Images used.
