package io.pravega.segmentstore.server.containers.dataRecovery;

import com.google.common.base.Charsets;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.tables.IteratorItem;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.server.containers.DebugStreamSegmentContainer;
import io.pravega.segmentstore.server.tables.ContainerTableExtension;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.FileNotFoundException;
import java.time.Duration;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import static io.pravega.shared.NameUtils.getMetadataSegmentName;

@Slf4j
public class CreateSegments implements Runnable {
    private final int containerId;
    private final DebugStreamSegmentContainer container;
    private static final Duration timeout = Duration.ofSeconds(10);
    ScheduledExecutorService executorService = ListAllSegments.createExecutorService(100);

    public CreateSegments(DebugStreamSegmentContainer container, int containerId){
        this.container = container;
        this.containerId = containerId;
    }

    @Override
    public void run() {
        System.out.format("Recovery started for container# %s\n", containerId);
        Scanner s = null;
        try {
            s = new Scanner(new File(String.valueOf(containerId)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return;
        }
        ContainerTableExtension ext = container.getExtension(ContainerTableExtension.class);
        AsyncIterator<IteratorItem<TableKey>> it = ext.keyIterator(getMetadataSegmentName(containerId), null, Duration.ofSeconds(10)).join();
        Set<TableKey> segmentsInMD = new HashSet<>();
        it.forEachRemaining(k -> segmentsInMD.addAll(k.getEntries()), executorService).join();
        while (s.hasNextLine()) {
            String[] fields = s.nextLine().split("\t");
            int len = Integer.parseInt(fields[0]);
            boolean isSealed = Boolean.parseBoolean(fields[1]);
            String segmentName = fields[2];
            segmentsInMD.remove(TableKey.unversioned(getTableKey(segmentName)));
                /*
                    1. segment exists in both metadata and storage, update SegmentMetadata
                    2. segment only in metadata, delete
                    3. segment only in storage, re-create it
                 */

            container.getStreamSegmentInfo(segmentName, timeout)
                    .thenAccept(e -> {
                        container.createStreamSegment(segmentName, len, isSealed)
                                .exceptionally(ex1 -> {
                                    log.error("Got an error while creating segment", ex1);
                                    return null;
                                }).join();
                    })
                    .exceptionally(e -> {
                        log.error("Got an exception on getStreamSegmentInfo", e);
                        Throwable cause = e.getCause();
                        if (cause instanceof StreamSegmentNotExistsException) {
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
