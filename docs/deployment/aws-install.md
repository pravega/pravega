# Running on AWS

Pre-reqs: terraform installed, and create a AWS account

###Deploy Steps
- Define the nodes layout in installer/hosts-template.

There are three sections of hosts-template, common-services is for zookeeper and bookkeeper, pravega-controller is for pravega controller node, and pravega-hosts is for pravega segmentstore node.
Here is a example of hosts-template:
#####
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

- Put you AWS keypair file under installer directory
- Run "terraform apply" under deployment directory, then follow prompts, enter AWS account credentials.

For now, only two region is supported: us-east-1 and us-west-1. Below is recommended instance type for them.

###Region us-east-1
- Three m3.xlarge for EMR
- Three m3.2xlarge for Pravega
- One m3.medium for bootstrap, also as client

###Region us-west-1:
- Three m3.xlarge for EMR
- Three i3.4xlarge for Pravega
- One i3.xlarge for bootstrap, also as client

Other instance type might have conflict with the Linux Images used.