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

# Script to undeploy Pravega using local NVMe drives on AWS EKS.


def run_command(tool, command):
    try:
        # Run command and capture the output
        result = subprocess.run([tool] + command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        # Print the command output
        print(result.stdout)

    except subprocess.CalledProcessError as e:
        # Print error output if the command fails
        print(f"Error: {e.stderr}")


def main():

    try:
        # Uninstall Grafana.
        run_command("kubectl", ["delete", "deployment", "grafana"])

        # Uninstall InfluxDB.
        run_command("kubectl", ["delete", "service", "influxdb-service"])
        run_command("kubectl", ["delete", "-f", "./influxdb.yaml"])

        # Uninstall Pravega Operator and Pravega.
        run_command("kubectl", ["delete", "-f", "./pravega.yaml"])
        time.sleep(5)
        run_command("helm", ["delete", "pravega-operator"])

        # Uninstall Bookkeeper Operator and Bookkeeper.
        run_command("kubectl", ["delete", "-f", "./bookkeeper.yaml"])
        time.sleep(5)
        run_command("helm", ["delete", "bookkeeper-operator"])

        # Uninstall Zookeeper Operator and Zookeeper.
        run_command("helm", ["delete", "zookeeper"])
        time.sleep(5)
        run_command("helm", ["delete", "zookeeper-operator"])

        # Uninstall cert manager and certs.
        run_command("kubectl", ["delete", "-f", "./bk-cert.yaml"])
        run_command("kubectl", ["delete", "-f", "./pravega-cert.yaml"])
        time.sleep(5)
        run_command("kubectl", ["delete", "-f", "https://github.com/cert-manager/cert-manager/releases/download/v1.11.0/cert-manager.yaml"])

        # Uninstall the EBS volume provisioner.
        run_command("kubectl", ["delete", "-k", "github.com/kubernetes-sigs/aws-ebs-csi-driver/deploy/kubernetes/overlays/stable/?ref=master"])

        # Uninstall Rancher local volume provisioner.
        run_command("kubectl", ["delete", "-f", "https://raw.githubusercontent.com/rancher/local-path-provisioner/v0.0.26/deploy/local-path-storage.yaml"])
        time.sleep(5)

        # Delete storage classes.
        run_command("kubectl", ["delete", "-f", "./storage-classes.yaml"])

    except Exception as e:
        print(f"Error: {str(e)}")


if __name__ == "__main__":
    main()

