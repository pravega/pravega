/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.stream;

import com.emc.pravega.common.metrics.MetricsConfig;
import com.emc.pravega.controller.util.ZKUtils;
import com.google.common.annotations.VisibleForTesting;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.ScheduledExecutorService;

/**
 * ZK stream metadata store.
 */
@Slf4j
public class ZKStreamMetadataStore extends AbstractStreamMetadataStore {

    public ZKStreamMetadataStore(ScheduledExecutorService executor) {
        this(ZKUtils.getCuratorClient(), executor);
        initialize(ZKUtils.getCuratorClient(), ZKUtils.getMetricsConfig());
    }

    @VisibleForTesting
    public ZKStreamMetadataStore(CuratorFramework client, ScheduledExecutorService executor) {
        ZKStream.initialize(client, executor);
        initialize(client, ZKUtils.getMetricsConfig());
    }

    private void initialize(CuratorFramework client, MetricsConfig metricsConfig) {
        if (metricsConfig != null) {
            METRICS_PROVIDER.start(metricsConfig);
        }
    }

    @Override
    ZKStream newStream(final String scope, final String name) {
        return new ZKStream(scope, name);
    }
}
