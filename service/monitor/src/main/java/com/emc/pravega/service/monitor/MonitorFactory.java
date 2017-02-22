/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.monitor;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.stream.impl.ClientFactoryImpl;
import com.google.common.annotations.VisibleForTesting;

import java.net.URI;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.emc.pravega.service.monitor.AutoScalerConfig.DEFAULT;

/**
 * Factory for creating segment monitors.
 */
public class MonitorFactory {

    // TODO: read from config!!
    // We need to clean the controller URI scheme for readers and writers - maybe read from zk
    private static final String SCOPE = "pravega";
    private static final String CONTROLLER_ADDR = "localhost";
    private static final int CONTROLLER_PORT = 9090;

    private static final ExecutorService EXECUTOR = new ThreadPoolExecutor(1, // core size
            10, // max size
            10 * 60L, // idle timeout
            TimeUnit.SECONDS,
            new ArrayBlockingQueue<Runnable>(2000)); // queue with a size

    private static AtomicReference<ClientFactory> clientFactory = new AtomicReference<>();
    private static AtomicReference<AutoScalerConfig> configuration = new AtomicReference<>(DEFAULT);

    @VisibleForTesting
    public static void setClientFactory(ClientFactory cf) {
        clientFactory.set(cf);
    }

    @VisibleForTesting
    public static void setConfiguration(AutoScalerConfig config) {
        configuration.set(config);
    }

    public enum MonitorType {
        AutoScaleMonitor
    }

    public static SegmentTrafficMonitor createMonitor(MonitorType monitorType, ExecutorService executor, ScheduledExecutorService cacheMaintenanceExecutor) {
        if (monitorType.equals(MonitorType.AutoScaleMonitor)) {

            if (clientFactory.get() == null) {
                clientFactory.compareAndSet(null, new ClientFactoryImpl(SCOPE, URI.create(String.format("tcp://%s:%d", CONTROLLER_ADDR, CONTROLLER_PORT))));
            }

            return AutoScaleMonitor.getMonitor(clientFactory.get(), configuration.get(), executor, cacheMaintenanceExecutor);
        }
        return null;
    }
}
