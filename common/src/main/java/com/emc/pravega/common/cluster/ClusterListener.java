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

import java.util.concurrent.Executor;

/**
 * Receive notifications for node added or node removed.
 */
public interface ClusterListener {

    /**
     * Method invoked when node has been added
     */
    public void nodeAdded();

    /**
     * Method invoked when node has been removed
     */
    public void nodeRemoved();

    /**
     * Start listener for a given cluster
     */
    public void start(final String clusterName);

    /**
     * Start listener on a custom executor.
     * @param clusterName name of the cluster on which the listener should run
     * @param executor custom executor on which the listener should run.
     */
    public void start(final String clusterName, final Executor executor);
}
