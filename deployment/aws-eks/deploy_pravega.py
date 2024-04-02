#!/usr/bin/python -u
#
# Copyright Pravega Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import paramiko
import boto3
import time
import subprocess
from kubernetes import client, config

# Script to deploy Pravega using local NVMe drives on AWS EKS.
# To run this script, you need to perform the next steps:
#
# 1) Install the eksclt tool and create an SSH key pair (namely, ~/.ssh/pravega_aws.pub).
#
# 2) Create the EKS cluster using eksctl:
# > eksctl create cluster --name pravega --region us-east-1 --node-type i3en.2xlarge --nodes 3 --ssh-access --ssh-public-key ~/.ssh/pravega_aws.pub
# Note that the script assumes a certain type of instance containing local drives to locate and mount them. Please, if
# you are using another type of i3 instance, it may happen that the name of the drives to mount change across instances.
# Take that into account when setting the journal_nvme_device and ledger_nvme_device variables in the main() method.
#
# 3) Attach the AmazonEBSCSIDriverPolicy to the EKS Cluster role, so we can create EBS volumes (replace
# YourClusterNameHere and YourRegion by the appropriate values):
# > eksctl utils associate-iam-oidc-provider --region=YourRegion --cluster=YourClusterNameHere --approve
# > eksctl create iamserviceaccount \
#   --region eu-central-1 \
#   --name ebs-csi-controller-sa \
#   --namespace kube-system \
#   --cluster YourClusterNameHere \
#   --attach-policy-arn arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy \
#   --approve \
#   --role-only \
#   --role-name AmazonEKS_EBS_CSI_DriverRole
# > eksctl create addon --name aws-ebs-csi-driver --cluster YourClusterNameHere \
#   --service-account-role-arn arn:aws:iam::$(aws sts get-caller-identity --query Account --output text):role/AmazonEKS_EBS_CSI_DriverRole --force
#
# 4) Run the deployment script for Pravega.
# > python deploy_pravega.py


def get_ec2_instances(region, filters):
    ec2 = boto3.resource('ec2', region_name=region)
    instances = ec2.instances.filter(Filters=filters)
    return instances


def format_and_mount_nvme_drive(ip_address, username, private_key_path, nvme_device, mount_directory):
    try:
        # SSH into the EC2 instance
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        client.connect(ip_address, username=username, key_filename=private_key_path)

        # Format the NVMe drive using parted and mkfs
        format_commands = [
            f"sudo yum install -y parted",
            f"sudo parted -s /dev/{nvme_device} mklabel gpt mkpart primary ext4 0% 100%",
            f"sudo mkfs.ext4 /dev/{nvme_device}",
        ]

        for command in format_commands:
            stdin, stdout, stderr = client.exec_command(command)
            exit_status = stdout.channel.recv_exit_status()

            if exit_status != 0:
                raise Exception(f"Command execution failed with exit code {exit_status}: {stderr.read().decode()}")

        print(f"NVMe drive /dev/{nvme_device} formatted successfully on {ip_address}")

        # Mount the NVMe drive to the specified directory
        mount_command = f"sudo mkdir -p {mount_directory} && sudo mount /dev/{nvme_device} {mount_directory}"
        stdin, stdout, stderr = client.exec_command(mount_command)
        exit_status = stdout.channel.recv_exit_status()

        if exit_status != 0:
            raise Exception(f"Mounting failed with exit code {exit_status}: {stderr.read().decode()}")

        print(f"NVMe drive /dev/{nvme_device} mounted to {mount_directory} on {ip_address}")

    except Exception as e:
        print(f"Error: {str(e)}")

    finally:
        # Close the SSH connection
        client.close()


def run_command(tool, command):
    try:
        # Run command and capture the output
        result = subprocess.run([tool] + command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        # Print the command output
        print(result.stdout)

    except subprocess.CalledProcessError as e:
        # Print error output if the command fails
        print(f"Error: {e.stderr}")


def set_local_drives_rancher():
    configmap_name = "local-path-config"
    namespace = "local-path-storage"

    # Load the kubeconfig file or use the in-cluster configuration
    config.load_kube_config()
    # Create a Kubernetes client
    v1 = client.CoreV1Api()
    try:
        # Retrieve the existing ConfigMap
        configmap = v1.read_namespaced_config_map(name=configmap_name, namespace=namespace)
        # Update the data field with the new value
        print(configmap.data['config.json'])
        configmap.data['config.json'] = configmap.data['config.json'].replace('\"/opt/local-path-provisioner\"', '\"/home/ledger\", \"/home/journal\"')
        print(configmap.data['config.json'])
        # Patch the ConfigMap with the updated data
        v1.patch_namespaced_config_map(name=configmap_name, namespace=namespace, body=configmap)
        print(f"ConfigMap '{configmap_name}' in namespace '{namespace}' updated successfully.")
    except Exception as e:
        print(f"Error updating ConfigMap: {str(e)}")

def main():
    # AWS credentials and region
    aws_access_key = 'AWS_ACCESS_KEY'
    aws_secret_key = 'AWS_SECRET_KEY'
    aws_region = 'us-east-1'

    # EC2 filters (modify as needed)
    filters = [
        {'Name': 'instance-state-name', 'Values': ['running']},
        # Add more filters if needed
    ]

    # Replace these values with your EC2 instance details
    username = 'ec2-user'
    private_key_path = '/home/raul/.ssh/pravega_aws.pub'
    journal_nvme_device = 'nvme1n1'
    journal_mount_directory = '/home/journal'
    ledger_nvme_device = 'nvme2n1'
    ledger_mount_directory = '/home/ledger'

    try:
        # Create storage classes.
        run_command("kubectl", ["create", "-f", "./storage-classes.yaml"])

        # Format node drives, mount them, and create the PVs for Bookies.
        ec2_instances = get_ec2_instances(aws_region, filters)
        for instance in ec2_instances:
            ip_address = instance.public_ip_address
            format_and_mount_nvme_drive(ip_address, username, private_key_path, journal_nvme_device, journal_mount_directory)
            format_and_mount_nvme_drive(ip_address, username, private_key_path, ledger_nvme_device, ledger_mount_directory)
            # Add a delay between instances (optional)
            time.sleep(1)

        # Install Rancher local volume provisioner.
        run_command("kubectl", ["apply", "-f", "https://raw.githubusercontent.com/rancher/local-path-provisioner/v0.0.26/deploy/local-path-storage.yaml"])
        time.sleep(5)
        set_local_drives_rancher()

        # Install the EBS volume provisioner.
        run_command("kubectl", ["apply", "-k", "github.com/kubernetes-sigs/aws-ebs-csi-driver/deploy/kubernetes/overlays/stable/?ref=master"])

        # Install helm repos.
        run_command("helm", ["repo", "add", "pravega", "https://charts.pravega.io"])
        run_command("helm", ["repo", "update"])

        # Install cert manager and certs.
        run_command("kubectl", ["apply", "-f", "https://github.com/cert-manager/cert-manager/releases/download/v1.11.0/cert-manager.yaml"])
        # Wait for the cert-manager to start working
        time.sleep(10)
        run_command("kubectl", ["create", "-f", "./bk-cert.yaml"])
        run_command("kubectl", ["create", "-f", "./pravega-cert.yaml"])

        # Install Zookeeper Operator and Zookeeper.
        run_command("helm", ["install", "zookeeper-operator", "pravega/zookeeper-operator"])
        run_command("helm", ["install", "zookeeper", "pravega/zookeeper", "--set", "replicas=1"])

        # Install Bookkeeper Operator and Bookkeeper.
        run_command("helm", ["install", "bookkeeper-operator", "pravega/bookkeeper-operator"])
        run_command("kubectl", ["create", "-f", "./bookkeeper.yaml"])

        # Install Pravega Operator and Pravega.
        run_command("helm", ["install", "pravega-operator", "pravega/pravega-operator"])
        run_command("kubectl", ["create", "-f", "./pravega.yaml"])

        # Install InfluxDB.
        run_command("kubectl", ["create", "-f", "./influxdb.yaml"])
        run_command("kubectl", ["expose", "pod", "influxdb", "--port=8086", "--name=influxdb-service"])

        # Install Grafana.
        run_command("kubectl", ["create", "deployment", "grafana", "--image=docker.io/grafana/grafana:7.5.17"])

    except Exception as e:
        print(f"Error: {str(e)}")


if __name__ == "__main__":
    main()

