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

import com.emc.pravega.common.cluster.Host;

import java.util.Map;
import java.util.Set;

/**
 * Container Balancers are used to fetch the new owners for the segment containers.
 */
public interface ContainerBalancer {
    /**
     * Compute the new owners of the segment containers based on hosts alive in the cluster.
     *
     * @param previousMapping       Existing host to container mapping. If non-empty its assumed to be balanced for the
     *                              older host set.
     * @param currentHosts          The updated list of hosts in the cluster.
     * @return                      The new host to containers mapping after performing a rebalance operation.
     * @throws NullPointerException If previousMapping is null.
     * @throws NullPointerException If currentHosts is null.
     */
    Map<Host, Set<Integer>> rebalance(Map<Host, Set<Integer>> previousMapping, Set<Host> currentHosts);

}
