/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
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
