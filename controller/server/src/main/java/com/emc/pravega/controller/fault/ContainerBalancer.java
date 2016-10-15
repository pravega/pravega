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

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Container Balancers are used to fetch the new owners of a segment containers.
 * It is used to compute the owners of the new segment containers.
 */
public interface ContainerBalancer<C, H> extends SegContainerHostMapping<C, H> {
    /**
     * Compute the new owners of the segment containers owned by the removed host.
     *
     * @param hostRemoved
     * @param availableHosts
     * @return
     */
    public CompletableFuture<Map<C, H>> hostRemoved(H hostRemoved, List<H> availableHosts);

    /**
     * Compute the new owners of the segment containers when a new host has been added.
     *
     * @param host
     * @param availableHosts
     * @return
     */
    public CompletableFuture<Map<C, H>> hostAdded(H host, List<H> availableHosts);

    //TODO: Check if this kind of function is required
    public CompletableFuture<Map<C, H>> recompute(Map<C, H> segmentToErrorHost);

}
