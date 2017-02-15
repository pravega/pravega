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

package com.emc.pravega.framework.services;

import java.net.URI;
import java.util.List;

/**
 * Service abstraction for the test framework. T
 * System tests use Service apis to configure, fetch deployment details and get the current status of the service.
 * .
 */
public interface Service {

    /**
     * Start a given service.
     *
     * @param wait true indicates that it is a blocking call.
     */
    public void start(final boolean wait);

    /**
     * Stop a service.
     */
    public void stop();

    /**
     * Clean the service.
     */
    public void clean();

    /**
     * Return the ID of the service.
     *
     * @return ID of the service.
     */
    public String getID();

    /**
     * Check if the service is up and running.
     *
     * @return true if the service is running.
     */
    public boolean isRunning();

    /**
     * Get the list of Host:port URIs where the service is running.
     *
     * @return List<URI> list of Host:port where the service is running.
     */
    public List<URI> getServiceDetails();
}
