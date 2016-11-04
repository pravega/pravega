/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.controller.fault;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Container Balancers are used to fetch the new owners of a segment containers.
 * It is used to compute the owners of the new segment containers.
 */
public interface ContainerBalancer<H, C> {
    /**
     * Compute the new owners of the segment containers with the list of new hosts added and removed.
     *
     * @param previousMapping Existing host to container mapping.
     * @param currentHosts The updated list of hosts in the cluster.
     * @return The new host to containers mapping after performaing a rebalance operation
     */
    public Optional<Map<H, C>> rebalance(Map<H, C> previousMapping, Set<H> currentHosts);

}
