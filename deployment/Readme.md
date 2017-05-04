How to quick deploy a pravega cluster?
Pre-reqs: Install terraform, and create a AWS account
1. Define the nodes layout in installer/hosts-template
2. Put you AWS keypair file under installer directory
2. Run "terraform apply", then follow prompts, enter AWS account credentials.

For now, only two region is supported: us-east-1 and us-west-1.
us-east-1 uses normal hardware resources, and its recommended config is:
- Three m3.xlarge for EMR
- Three m3.2xlarge for Pravega
- One m3.medium for bootstrap, also as client

us-west-1 uses extremely high performance hardware, and its recommended config is:
- Three m3.xlarge for EMR
- Three i3.4xlarge for Pravega
- One i3.xlarge for bootstrap, also as client

Other instance type might have conflict with the Linux Images used.
