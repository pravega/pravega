/*
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

provider "aws" {
 access_key = "${var.aws_access_key}"
 secret_key = "${var.aws_secret_key}"
 region = "${var.aws_region}"
}

resource "aws_security_group" "pravega_default" {
 name = "pravega_default"
 ingress {
  from_port = 0
  to_port = 0
  protocol = "-1"
  cidr_blocks = ["0.0.0.0/0"]
 }
 egress {
  from_port = 0
  to_port = 0
  protocol = "-1"
  cidr_blocks = ["0.0.0.0/0"]
 }
}

# IAM Role setups

###

# IAM role for EMR Service
resource "aws_iam_role" "iam_emr_service_role" {
  name = "iam_emr_service_role"

  assume_role_policy = <<EOF
{
  "Version": "2008-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Principal": {
        "Service": "elasticmapreduce.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}

resource "aws_iam_role_policy" "iam_emr_service_policy" {
  name = "iam_emr_service_policy"
  role = "${aws_iam_role.iam_emr_service_role.id}"

  policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Resource": "*",
        "Action": [
            "ec2:AuthorizeSecurityGroupEgress",
            "ec2:AuthorizeSecurityGroupIngress",
            "ec2:CancelSpotInstanceRequests",
            "ec2:CreateNetworkInterface",
            "ec2:CreateSecurityGroup",
            "ec2:CreateTags",
            "ec2:DeleteNetworkInterface",
            "ec2:DeleteSecurityGroup",
            "ec2:DeleteTags",
            "ec2:DescribeAvailabilityZones",
            "ec2:DescribeAccountAttributes",
            "ec2:DescribeDhcpOptions",
            "ec2:DescribeInstanceStatus",
            "ec2:DescribeInstances",
            "ec2:DescribeKeyPairs",
            "ec2:DescribeNetworkAcls",
            "ec2:DescribeNetworkInterfaces",
            "ec2:DescribePrefixLists",
            "ec2:DescribeRouteTables",
            "ec2:DescribeSecurityGroups",
            "ec2:DescribeSpotInstanceRequests",
            "ec2:DescribeSpotPriceHistory",
            "ec2:DescribeSubnets",
            "ec2:DescribeVpcAttribute",
            "ec2:DescribeVpcEndpoints",
            "ec2:DescribeVpcEndpointServices",
            "ec2:DescribeVpcs",
            "ec2:DetachNetworkInterface",
            "ec2:ModifyImageAttribute",
            "ec2:ModifyInstanceAttribute",
            "ec2:RequestSpotInstances",
            "ec2:RevokeSecurityGroupEgress",
            "ec2:RunInstances",
            "ec2:TerminateInstances",
            "ec2:DeleteVolume",
            "ec2:DescribeVolumeStatus",
            "ec2:DescribeVolumes",
            "ec2:DetachVolume",
            "iam:GetRole",
            "iam:GetRolePolicy",
            "iam:ListInstanceProfiles",
            "iam:ListRolePolicies",
            "iam:PassRole",
            "s3:CreateBucket",
            "s3:Get*",
            "s3:List*",
            "sdb:BatchPutAttributes",
            "sdb:Select",
            "sqs:CreateQueue",
            "sqs:Delete*",
            "sqs:GetQueue*",
            "sqs:PurgeQueue",
            "sqs:ReceiveMessage"
        ]
    }]
}
EOF
}

# IAM Role for EC2 Instance Profile
resource "aws_iam_role" "iam_emr_profile_role" {
  name = "iam_emr_profile_role"

  assume_role_policy = <<EOF
{
  "Version": "2008-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Principal": {
        "Service": "ec2.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
}

resource "aws_iam_instance_profile" "emr_profile" {
  name  = "emr_profile_pravega"
  role = "${aws_iam_role.iam_emr_profile_role.name}"
}

resource "aws_iam_role_policy" "iam_emr_profile_policy" {
  name = "iam_emr_profile_policy"
  role = "${aws_iam_role.iam_emr_profile_role.id}"

  policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Resource": "*",
        "Action": [
            "cloudwatch:*",
            "dynamodb:*",
            "ec2:Describe*",
            "elasticmapreduce:Describe*",
            "elasticmapreduce:ListBootstrapActions",
            "elasticmapreduce:ListClusters",
            "elasticmapreduce:ListInstanceGroups",
            "elasticmapreduce:ListInstances",
            "elasticmapreduce:ListSteps",
            "kinesis:CreateStream",
            "kinesis:DeleteStream",
            "kinesis:DescribeStream",
            "kinesis:GetRecords",
            "kinesis:GetShardIterator",
            "kinesis:MergeShards",
            "kinesis:PutRecord",
            "kinesis:SplitShard",
            "rds:Describe*",
            "s3:*",
            "sdb:*",
            "sns:*",
            "sqs:*"
        ]
    }]
}
EOF
}
 
resource "aws_emr_cluster" "pravega-emr-cluster" {
  name = "pravega-emr-cluster"
  release_label = "emr-5.5.0"
  applications = ["Hadoop"]

  ec2_attributes {
    key_name = "${var.aws_key_name}"
    emr_managed_master_security_group = "${aws_security_group.pravega_default.id}"
    emr_managed_slave_security_group = "${aws_security_group.pravega_default.id}"
    instance_profile = "${aws_iam_instance_profile.emr_profile.arn}"
  }

  master_instance_type = "m3.xlarge"
  core_instance_type = "m3.xlarge"
  core_instance_count = "${var.hadoop_instance_count}"
  service_role = "${aws_iam_role.iam_emr_service_role.arn}"
}

resource "aws_instance" "pravega" {
 depends_on = ["aws_security_group.pravega_default"]
 count = "${var.pravega_num}"
 ami = "${lookup(var.pravega_aws_amis, var.aws_region)}"
 instance_type = "${lookup(var.pravega_instance_type, var.aws_region)}"
 key_name = "${var.aws_key_name}"
 security_groups = ["pravega_default"]
 provisioner "local-exec" {
   command = "chmod 400 ${var.cred_path}"
 }
 provisioner "remote-exec" {
   connection = {
      type = "ssh"
      user = "ubuntu"
      private_key = "${file("${var.cred_path}")}"
   }
   inline = [
      "wget -c http://www.apache.org/dist/zookeeper/zookeeper-3.5.1-alpha/zookeeper-3.5.1-alpha.tar.gz && sudo cp zookeeper-3.5.1-alpha.tar.gz /zookeeper-3.5.1-alpha.tar.gz", 
      "wget -c http://www.apache.org/dist/hadoop/core/hadoop-2.7.3/hadoop-2.7.3.tar.gz && sudo cp hadoop-2.7.3.tar.gz /hadoop-2.7.3.tar.gz",
      "wget -c http://www.apache.org/dist/bookkeeper/bookkeeper-4.4.0/bookkeeper-server-4.4.0-bin.tar.gz && sudo cp bookkeeper-server-4.4.0-bin.tar.gz /bookkeeper-server-4.4.0-bin.tar.gz",
      "sudo apt-get install -y python",
      "sudo cp /home/ubuntu/.ssh/authorized_keys /root/.ssh/",
   ]
 } 
}

resource "aws_instance" "boot" {
 ami = "${lookup(var.pravega_aws_amis, var.aws_region)}"
 instance_type = "m3.medium"
 key_name = "${var.aws_key_name}"
 security_groups = ["pravega_default"]
 depends_on = ["aws_instance.pravega"]
 provisioner "local-exec" {
   command = "cp ${var.cred_path} installer && chmod +x bootstrap.sh && ./bootstrap.sh '${join(",", aws_instance.pravega.*.public_ip)}' ${aws_emr_cluster.pravega-emr-cluster.master_public_dns} ${var.aws_region} "
 }
 provisioner "file" {
   connection = {
      type = "ssh"
      user = "ubuntu"
      private_key = "${file("${var.cred_path}")}"
   }
   source = "installer/"
   destination = "/home/ubuntu"
 }
 provisioner "remote-exec" {
   connection = {
      type = "ssh"
      user = "ubuntu"
      private_key = "${file("${var.cred_path}")}"
   }
   inline = [
      "sudo apt-add-repository ppa:ansible/ansible -y",
      "sudo apt-get -y update",
      "sudo apt-get install -y software-properties-common",
      "sudo apt-get install -y ansible",
      "sudo apt-get install -y git",
      "cd /tmp && git clone https://github.com/pravega/pravega.git && cd pravega/",
      "sudo add-apt-repository ppa:openjdk-r/ppa -y && sudo apt-get -y update && sudo apt-get install -y openjdk-8-jdk",
      "export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 && ./gradlew distTar && mv build/distributions/pravega*.tgz /home/ubuntu/data/pravega-0.1.0-SNAPSHOT.tgz",
      "cd /home/ubuntu",
      "chmod 400 $(basename ${var.cred_path})",
      "ansible-playbook -i hosts entry_point.yml --private-key=$(basename ${var.cred_path})",
   ]
  }
}
