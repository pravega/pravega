# Running on AWS

Pre-reqs: terraform installed, and create a AWS account

###Deploy Steps
- Define the nodes layout in installer/hosts-template
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