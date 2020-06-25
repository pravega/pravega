package io.pravega.segmentstore.server;

import com.google.common.base.Charsets;
import io.pravega.common.Exceptions;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.tables.IteratorItem;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.server.containers.DebugStreamSegmentContainer;
import io.pravega.segmentstore.server.tables.ContainerTableExtension;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.shared.segment.SegmentToContainerMapper;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static io.pravega.shared.NameUtils.getMetadataSegmentName;

@Slf4j
public class DataRecoveryTestUtils {
    private static final Duration timeout = Duration.ofSeconds(10);
    private static final ScheduledExecutorService executorService = createExecutorService(100);

    public static List<List<SegmentProperties>> listAllSegments(Storage tier2, int containerCount) throws IOException {
        SegmentToContainerMapper segToConMapper = new SegmentToContainerMapper(containerCount);
        List<List<SegmentProperties>> segmentToContainers = new ArrayList<>();
        for (int containerId = 0; containerId<containerCount; containerId++) {
            segmentToContainers.add(new ArrayList<>());
        }
        log.info("Generating container files with the segments they own...");

        Iterator<SegmentProperties> it = tier2.listSegments();
        if (it == null) {
            return segmentToContainers;
        }
        while(it.hasNext()) {
            SegmentProperties curr = it.next();
            int containerId = segToConMapper.getContainerId(curr.getName());
            segmentToContainers.get(containerId).add(curr);
        }
        return segmentToContainers;
    }

    public static ScheduledExecutorService createExecutorService(int threadPoolSize) {
        ScheduledThreadPoolExecutor es = new ScheduledThreadPoolExecutor(threadPoolSize);
        es.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        es.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        es.setRemoveOnCancelPolicy(true);
        return es;
    }

    public static void createAllSegments(DebugStreamSegmentContainer container, int containerId, List<SegmentProperties> segments) {
        System.out.format("Recovery started for container# %s\n", containerId);

        ContainerTableExtension ext = container.getExtension(ContainerTableExtension.class);
        AsyncIterator<IteratorItem<TableKey>> it = ext.keyIterator(getMetadataSegmentName(containerId), null, Duration.ofSeconds(10)).join();
        Set<TableKey> segmentsInMD = new HashSet<>();
        it.forEachRemaining(k -> segmentsInMD.addAll(k.getEntries()), executorService).join();
        for (SegmentProperties segment : segments) {
            long len = segment.getLength();
            boolean isSealed = segment.isSealed();
            String segmentName = segment.getName();
            segmentsInMD.remove(TableKey.unversioned(getTableKey(segmentName)));
                /*
                    1. segment exists in both metadata and storage, update SegmentMetadata
                    2. segment only in metadata, delete
                    3. segment only in storage, re-create it
                 */

            container.getStreamSegmentInfo(segment.getName(), timeout)
                    .thenAccept(e -> {
                        container.createStreamSegment(segmentName, len, isSealed)
                                .exceptionally(ex1 -> {
                                    log.error("Got an error while creating segment", ex1);
                                    return null;
                                }).join();
                    })
                    .exceptionally(e -> {
                        log.error("Got an exception on getStreamSegmentInfo", e);
                        if (Exceptions.unwrap(e) instanceof StreamSegmentNotExistsException) {
                            container.createStreamSegment(segmentName, len, isSealed)
                                    .exceptionally(ex2 -> {
                                        log.error("Got an error while creating segment", ex2);
                                        return null;
                                    }).join();
                        }
                        return null;
                    }).join();
        }
        for(TableKey k : segmentsInMD){
            String segmentName = new String(k.getKey().array(), Charsets.UTF_8);
            log.info("Deleting segment : {} as it is not in storage", segmentName);
            container.deleteStreamSegment(segmentName, timeout).join();
        }
        System.out.format("Recovery done for container# %s\n", containerId);
    }

    private static ArrayView getTableKey(String segmentName) {
        return new ByteArraySegment(segmentName.getBytes(Charsets.UTF_8));
    }
}
