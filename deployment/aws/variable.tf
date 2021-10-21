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

variable "pravega_num" {
  type = "string"
  default = "3"
}

variable "pravega_aws_amis" {
  default = {
    us-east-1 = "ami-7747d01e"
    us-west-1 = "ami-73f7da13"
  }
}

variable "pravega_instance_type" {
  default = {
    us-east-1 = "m3.xlarge"
    us-west-1 = "i3.4xlarge"
  }
}

variable "cred_path" {
  description = "aws key file absolute path"
  type = "string"
}

variable "hadoop_instance_count" {
  type = "string"
  default = "3"
}

variable "pravega_release" {
  type = "string"
  default = "https://github.com/pravega/pravega/releases/download/v0.1.0-prerelease2/pravega-0.1.0-1404.04e62df.tgz"
}
