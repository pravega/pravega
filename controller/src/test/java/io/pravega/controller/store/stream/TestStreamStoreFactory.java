package io.pravega.controller.store.stream;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.controller.store.PravegaTablesStoreHelper;
import io.pravega.controller.util.Config;
import org.apache.curator.framework.CuratorFramework;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;

public class TestStreamStoreFactory {
    @VisibleForTesting
    public static StreamMetadataStore createPravegaTablesStore(final CuratorFramework client,
                                                               final ScheduledExecutorService executor, PravegaTablesStoreHelper helper) {
        return new PravegaTablesStreamMetadataStore(client, executor, Duration.ofHours(Config.COMPLETED_TRANSACTION_TTL_IN_HOURS), helper);
    }
}
