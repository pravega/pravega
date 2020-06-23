package io.pravega.segmentstore.server.containers.dataRecovery;

import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.storage.AsyncStorageWrapper;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.SegmentRollingPolicy;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.rolling.RollingStorage;
import io.pravega.shared.segment.SegmentToContainerMapper;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Iterator;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import io.pravega.segmentstore.server.store.ServiceConfig;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ListAllSegments {
    protected final String APPEND_FORMAT = "Segment_%s_Append_%d";
    protected final long DEFAULT_ROLLING_SIZE = (int) (APPEND_FORMAT.length() * 1.5);
    private SegmentToContainerMapper segToConMapper;
    protected static final Duration TIMEOUT = Duration.ofMillis(60000 * 1000);

    public ListAllSegments() {
        segToConMapper = new SegmentToContainerMapper(ServiceConfig.CONTAINER_COUNT.getDefaultValue());
    }

    public void execute(Storage tier2) throws IOException {
        int containerCount = segToConMapper.getTotalContainerCount();
        FileWriter[] writers = new FileWriter[containerCount];
        for (int containerId=0; containerId < containerCount; containerId++) {
            File f = new File(String.valueOf(containerId));
            if(f.exists() && !f.delete()){
                log.error("Failed to delete "+ f.getAbsolutePath());
                return;
            }
            if(!f.createNewFile()){
                log.error("Failed to create "+ f.getAbsolutePath());
                return;
            }
            writers[containerId] = new FileWriter(f.getName());
        }

        log.info("Generating container files with the segments they own...");
        Iterator<SegmentProperties> it = tier2.listSegments();
        while(it.hasNext()) {
            SegmentProperties curr = it.next();
            int containerId = segToConMapper.getContainerId(curr.getName());
            writers[containerId].write(curr.getLength()+"\t"+ curr.isSealed()+"\t"+curr.getName()+"\n");
        }
        for (int containerId=0; containerId < containerCount; containerId++) {
            writers[containerId].close();
        }
    }

    public static ScheduledExecutorService createExecutorService(int threadPoolSize) {
        ScheduledThreadPoolExecutor es = new ScheduledThreadPoolExecutor(threadPoolSize);
        es.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        es.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        es.setRemoveOnCancelPolicy(true);
        return es;
    }
}
