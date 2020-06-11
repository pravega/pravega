package io.pravega.controller.store.stream;

import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.rpc.auth.GrpcAuthHelper;
import org.apache.curator.framework.CuratorFramework;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;

public class StreamStoreHelper {

    public static StreamMetadataStore getPravegaTablesStreamStore(SegmentHelper segHelper,
                                                                  CuratorFramework cli,
                                                                  ScheduledExecutorService executor,
                                                                  Duration gcPeriod, GrpcAuthHelper authHelper) {
        return new PravegaTablesStreamMetadataStore(segHelper, cli, executor, gcPeriod, authHelper);
    }
}
