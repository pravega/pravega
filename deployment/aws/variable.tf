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
    us-west-1 = "ami-2afbde4a"
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
