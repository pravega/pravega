/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 */
output "ip" {
  value = "${aws_instance.swarm_master.*.public_ip}"
}

output "master_public_dns" {
  value = "${aws_instance.swarm_master.*.public_dns}"
}

output "slave_public_dns" {
  value = "${aws_instance.swarm_slaves.*.public_dns}"
}
