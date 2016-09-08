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

package com.emc.pravega.cluster.zkutils.common;

import com.google.common.base.Preconditions;

/**
 * Created by kandha on 8/4/16.
 * This class represents an access point for node and controllers
 */
public final class Endpoint {
    private final String hostName;
    private final int port;
    private final int hashCode;

    /**
     * Constructor
     * @param hostname hostname used to connect to the node
     * @param port port associated with the node
     */
    Endpoint(String hostname, int port) {
        Preconditions.checkNotNull(hostname);
        this.hostName = hostname;
        this.port = port;
        hashCode = (hostName + ":" + port).hashCode();
    }

    /**
     * Construct from a string which contains <hostname>:<part> form of string
     * @param identifierString
     */
    Endpoint(String identifierString) {
        String[] parts = identifierString.split(":");
        Preconditions.checkArgument(parts.length == 2);
        this.hostName = parts[0];
        this.port = Integer.parseInt(parts[1]);
        hashCode = identifierString.hashCode();
    }

    public static Endpoint constructFrom(String fullPath) {
        Preconditions.checkNotNull(fullPath);

        String[] parts = fullPath.split("/");
        String lastPart = parts[parts.length - 1];
        return new Endpoint(lastPart);
    }

    /**
     * Override hash code so that it can be used as a parameter of a hashmap etc
     * @return the hashcode
     */
    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public boolean equals(Object ep) {
        return toString().equals(ep.toString());
    }

    /**
     * Get a custom string representation
     * @return a string representation
     */
    @Override
    public String toString() {
        return hostName + ":" + port;
    }
}
