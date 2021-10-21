/**
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
# aws credentials and region
provider "aws" {
 access_key = "${var.aws_access_key}"
 secret_key = "${var.aws_secret_key}"
 region = "${var.aws_region}"
}

#security group, allow all incoming and outgoing traffic
resource "aws_security_group" "pravega_default" {
 name_prefix = "pravega_default"
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


#swarm master instance resources
resource "aws_instance" "swarm_master" {
 depends_on = ["aws_security_group.pravega_default"]
 count = "${var.master_num}"
 ami = "${lookup(var.pravega_aws_amis, var.aws_region)}"
 instance_type = "${lookup(var.pravega_instance_type, var.aws_region)}"
 key_name = "${var.aws_key_name}"
 vpc_security_group_ids = ["${aws_security_group.pravega_default.id}"]
 provisioner "local-exec" {
   command = "chmod 400 ${var.cred_path}"
 }
  provisioner "file" {
    source = "${var.config_path}/master_script.sh"
    destination = "/tmp/master_script.sh"
   connection = {
      type = "ssh"
      user = "ubuntu"
      private_key = "${file("${var.cred_path}")}"
   }
  }

 provisioner "remote-exec" {
   connection = {
      type = "ssh"
      user = "ubuntu"
      private_key = "${file("${var.cred_path}")}"
   }
   inline = [
      "chmod +x /tmp/master_script.sh",
      "/tmp/master_script.sh ${var.pravega_org} ${var.pravega_branch}",
    ]
   }
  provisioner "local-exec" {
   command = "scp -o StrictHostKeyChecking=no -i ${var.cred_path} ubuntu@${aws_instance.swarm_master.public_ip}:/home/ubuntu/token.sh  ${var.config_path}"
 }

}

#swarm slave instance resources
resource "aws_instance" "swarm_slaves" {
 depends_on = ["aws_security_group.pravega_default", "aws_instance.swarm_master"]
 count = "${var.slave_num}"
 ami = "${lookup(var.pravega_aws_amis, var.aws_region)}"
 instance_type = "${lookup(var.pravega_instance_type, var.aws_region)}"
 key_name = "${var.aws_key_name}"
 vpc_security_group_ids = ["${aws_security_group.pravega_default.id}"]
 provisioner "local-exec" {
   command = "chmod 400 ${var.cred_path}"
 }

 provisioner "file" {
    source = "${var.config_path}/slave_script.sh"
    destination = "/tmp/slave_script.sh"
    connection = {
      type = "ssh"
      user = "ubuntu"
      private_key = "${file("${var.cred_path}")}"
   }
  }


 provisioner "remote-exec" {
   connection = {
      type = "ssh"
      user = "ubuntu"
      private_key = "${file("${var.cred_path}")}"
   }
   inline = [
      "chmod +x /tmp/slave_script.sh",
      "/tmp/slave_script.sh ${var.pravega_org} ${var.pravega_branch}",
    ]
}
 provisioner "remote-exec" {
   connection = {
      type = "ssh"
      user = "root"
      private_key = "${file("${var.cred_path}")}"
   }
  script = "${var.config_path}/token.sh"
 }
}
