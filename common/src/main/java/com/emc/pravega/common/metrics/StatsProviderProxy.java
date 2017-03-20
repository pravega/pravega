/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */

package com.emc.pravega.common.metrics;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class StatsProviderProxy implements StatsProvider {
    private final AtomicReference<StatsProvider> instance = new AtomicReference<>(new NullStatsProvider());
    private final ConcurrentHashMap<StatsLoggerProxy, String> statsLoggerProxies = new ConcurrentHashMap<>();

    StatsProviderProxy() { }

    void setProvider(MetricsConfig config) {
        if (config.isEnableStatistics()) {
            log.info("Stats enabled");
            instance.set(new YammerStatsProvider(config));
        } else {
            log.info("Stats disabled");
            instance.set(new NullStatsProvider());
        }

        statsLoggerProxies.forEach( (proxy, scope) -> {
            proxy.setLogger(instance.get().createStatsLogger(scope));
        });
    }

    @Override
    public void start() {
        instance.get().start();
    }

    @Override
    public void close() {
        instance.get().close();
    }

    @Override
    public StatsLogger createStatsLogger(String scope) {
        StatsLogger logger = instance.get().createStatsLogger(scope);
        StatsLoggerProxy proxy = new StatsLoggerProxy(logger);
        statsLoggerProxies.put(proxy, scope);

        return proxy;
    }

    @Override
    public DynamicLogger createDynamicLogger() {
        return instance.get().createDynamicLogger();
    }
}
