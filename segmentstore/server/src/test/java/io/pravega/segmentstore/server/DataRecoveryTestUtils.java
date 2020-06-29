/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static io.pravega.shared.NameUtils.getMetadataSegmentName;

@Slf4j
public class DataRecoveryTestUtils {
    private static final Duration TIMEOUT = Duration.ofSeconds(10);
    private static final ScheduledExecutorService EXECUTOR_SERVICE = createExecutorService(10);

    /**
     * Lists all segments from a given long term storage.
     * @param tier2 Long term storage.
     * @param containerCount Total number of segment containers.
     * @return A 2D list containing segments by container Ids.
     * @throws IOException in case of exception during the execution.
     */
    public static Map<Integer, List<SegmentProperties>> listAllSegments(Storage tier2, int containerCount) throws IOException {
        SegmentToContainerMapper segToConMapper = new SegmentToContainerMapper(containerCount);
        Map<Integer, List<SegmentProperties>> segmentToContainers = new HashMap<Integer, List<SegmentProperties>>();
        log.info("Generating container files with the segments they own...");
        Iterator<SegmentProperties> it = tier2.listSegments();
        if (it == null) {
            return segmentToContainers;
        }
        while (it.hasNext()) {
            SegmentProperties curr = it.next();
            int containerId = segToConMapper.getContainerId(curr.getName());
            List<SegmentProperties> segmentsList = segmentToContainers.get(containerId);
            if (segmentsList == null) {
                segmentsList = new ArrayList<>();
                segmentsList.add(curr);
                segmentToContainers.put(containerId, segmentsList);
            } else {
                segmentToContainers.get(containerId).add(curr);
            }
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

    /**
     * Creates all segments given in the list with the given DebugStreamSegmentContainer.
     * @param container The DebugStreamSegmentContainer instance which will used to create segments.
     * @param segments List of segments to be created.
     */
    public static void createAllSegments(DebugStreamSegmentContainer container, List<SegmentProperties> segments) {
        if (segments == null) {
            return;
        }
        int containerId = container.getId();
        System.out.format("Recovery started for container# %s\n", containerId);
        ContainerTableExtension ext = container.getExtension(ContainerTableExtension.class);
        AsyncIterator<IteratorItem<TableKey>> it = ext.keyIterator(getMetadataSegmentName(containerId), null,
                Duration.ofSeconds(10)).join();

        // Add all segments present in the container metadata in a set.
        Set<TableKey> segmentsInMD = new HashSet<>();
        it.forEachRemaining(k -> segmentsInMD.addAll(k.getEntries()), EXECUTOR_SERVICE).join();

        for (SegmentProperties segment : segments) {
            long len = segment.getLength();
            boolean isSealed = segment.isSealed();
            String segmentName = segment.getName();

            /*
                1. segment exists in both metadata and storage, update SegmentMetadata
                2. segment only in metadata, delete
                3. segment only in storage, re-create it
             */
            segmentsInMD.remove(TableKey.unversioned(getTableKey(segmentName)));
            container.getStreamSegmentInfo(segment.getName(), TIMEOUT)
                    .thenAccept(e -> {
                        container.createStreamSegment(segmentName, len, isSealed)
                                .exceptionally(ex -> {
                                    log.error("Exception occurred while creating segment", ex);
                                    return null;
                                }).join();
                    })
                    .exceptionally(e -> {
                        log.error("Got an exception on getStreamSegmentInfo", e);
                        if (Exceptions.unwrap(e) instanceof StreamSegmentNotExistsException) {
                            container.createStreamSegment(segmentName, len, isSealed)
                                    .exceptionally(ex -> {
                                        log.error("Exception occurred while creating segment", ex);
                                        return null;
                                    }).join();
                        }
                        return null;
                    }).join();
        }
        for (TableKey k : segmentsInMD) {
            String segmentName = new String(k.getKey().array(), Charsets.UTF_8);
            log.info("Deleting segment : {} as it is not in storage", segmentName);
            container.deleteStreamSegment(segmentName, TIMEOUT).join();
        }
        System.out.format("Recovery done for container# %s\n", containerId);
    }

    private static ArrayView getTableKey(String segmentName) {
        return new ByteArraySegment(segmentName.getBytes(Charsets.UTF_8));
    }
}
