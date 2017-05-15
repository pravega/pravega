<!--
Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Running on AWS

Pre-reqs: terraform installed, and AWS account created

Terraform can be downloaded from https://www.terraform.io/downloads.html

### Deploy Steps
- Define the nodes layout in installer/hosts-template (Optional).

There are three sections of hosts-template, common-services is for zookeeper and bookkeeper, pravega-controller is for pravega controller nodes, and pravega-hosts is for pravega segmentstore nodes.
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

- Run "sudo terraform apply" under deployment/aws directory, then follow prompts, enter AWS account credentials.

There are four variables would be needed:

1. AWS access key and AWS secret key, which can be obtained from AWS account
2. cred_path, which is the definite path of key pair file. It would be downloaded when key pair is created
3. AWS region: For now, only two region are supported: us-east-1 and us-west-1. Below is recommended default instance types for them.

### Region us-east-1
- Three m3.xlarge for EMR
- Three m3.2xlarge for Pravega
- One m3.medium for bootstrap, also as client

### Region us-west-1:
- Three m3.xlarge for EMR
- Three i3.4xlarge for Pravega
- One i3.xlarge for bootstrap, also as client

Other instance types might have compatibility issue.

### How to destroy the pravega cluster

Run "sudo terraform destroy", then enter "yes"
