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
package com.emc.pravega.common.cluster;


import java.util.List;

/**
 * Cluster interface enables you to register / de-register a Host to a cluster
 */
public interface Cluster extends AutoCloseable {

    /**
     * Register a Host to a cluster
     *
     * @param host
     * @throws Exception
     */
    public void registerHost(final Host host) throws Exception;

    /**
     * De-register a Host from a cluster
     *
     * @param host
     * @throws Exception
     */
    public void deregisterHost(final Host host) throws Exception;

    /**
     * Add Listeners
     *
     * @param hostAdded   - Host added Cluster Listener
     * @param hostRemoved - Host removed Cluster Listener
     */
    public void addListener(final ClusterListener hostAdded, final ClusterListener hostRemoved) throws Exception;

    /**
     * Get the current cluster members.
     *
     * @return List<Host>
     */
    public List<Host> getClusterMembers();

}
