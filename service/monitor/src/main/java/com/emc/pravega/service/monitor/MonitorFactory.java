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
package com.emc.pravega.service.monitor;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.stream.impl.ClientFactoryImpl;
import com.google.common.annotations.VisibleForTesting;

import java.net.URI;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Factory for creating segment monitors.
 */
public class MonitorFactory {

    // TODO: read from config!!
    // We need to clean the controller URI scheme for readers and writers - maybe read from zk
    static final String SCOPE = "pravega";
    static final String CONTROLLER_ADDR = "localhost";
    static final int CONTROLLER_PORT = 9090;
    private static AtomicReference<ClientFactory> clientFactory = new AtomicReference<>();

    @VisibleForTesting
    public static void setClientFactory(ClientFactory cf) {
        clientFactory.set(cf);
    }

    public enum MonitorType {
        ThresholdMonitor
    }

    public static SegmentTrafficMonitor createMonitor(MonitorType monitorType) {
        if (monitorType.equals(MonitorType.ThresholdMonitor)) {

            if (clientFactory.get() == null) {
                clientFactory.compareAndSet(null, new ClientFactoryImpl(SCOPE, URI.create(String.format("tcp://%s:%d", CONTROLLER_ADDR, CONTROLLER_PORT))));
            }

            return ThresholdMonitor.getMonitorSingleton(clientFactory.get());
        }
        return null;
    }
}
