<!--
Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
-->
# Running on AWS

Pre-reqs: Have an AWS account and have Terraform installed. To install and download Terraform, follow the instructions here: https://www.terraform.io/downloads.html

### Deploy Steps
- Run "sudo terraform apply" under the deployment/aws directory, and then follow prompt instruction, enter the AWS account credentials.

There are four variables would be needed:

1. AWS access key and AWS secret key, which can be obtained from AWS account
2. cred_path, which is the absolute path of key pair file. It would be downloaded when key pair is created
3. AWS region: Currently, we only support two regions: us-east-1 and us-west-1. We list below the instance types we recommend for them.
  - Region us-east-1:
    - Three m3.xlarge for EMR
    - Three m3.2xlarge for Pravega
    - One m3.medium for bootstrap, also as client
  - Region us-west-1:
    - Three m3.xlarge for EMR
    - Three i3.4xlarge for Pravega
    - One i3.xlarge for bootstrap, also as client

Other instance types might present conflicts with the Linux Images used.

### How to customize the pravega cluster
- Change default value of "pravega_num" in variable.tf
- Define the your own nodes layout in installer/hosts-template, default hosts-template is under installer directory.

There are three sections of hosts-template:
1. common-services is the section for zookeeper and bookkeeper
2. pravega-controller is the section for pravega controller node
3. pravega-hosts is the section for the pravega segment store node.


### How to destroy the pravega cluster

Run "sudo terraform destroy", then enter "yes"
