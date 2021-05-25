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
variable "aws_access_key" {
  description = "AWS Access Key"
}

variable "aws_secret_key" {
  description = "AWS Secret Key"
}

variable "aws_region" {
  description = "AWS Region to launch configuration in"
}

variable "aws_key_name" {
  description = "AWS key pair name"
}

variable "master_num" {
  type = "string"
  default = "1"
}

variable "slave_num" {
  type = "string"
  default = "3"
}

variable "pravega_aws_amis" {
  default = {
    us-east-2 = "ami-2581aa40"
  }
}

variable "pravega_instance_type" {
  default = {
    us-east-2 = "t2.xlarge"
  }
}

variable "cred_path" {
  description = "aws key file absolute path"
  type = "string"
}

variable "config_path" {
  description = "aws instance config file absolute path"
  type = "string"
}

variable "pravega_org" {
  description = "pravega org type mention the fork name in case of fork"
  type = "string"
  default = "pravega"
}

variable "pravega_branch" {
  description = "pravega branch to be used to execute system tests"
  type = "string"
  default = "master"
}
