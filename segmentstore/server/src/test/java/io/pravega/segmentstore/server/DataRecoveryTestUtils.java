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
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.ArrayView;
import io.pravega.common.util.AsyncIterator;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.segmentstore.contracts.SegmentProperties;
import io.pravega.segmentstore.contracts.StreamSegmentNotExistsException;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.IteratorArgs;
import io.pravega.segmentstore.contracts.tables.IteratorItem;
import io.pravega.segmentstore.contracts.tables.TableKey;
import io.pravega.segmentstore.server.containers.DebugStreamSegmentContainer;
import io.pravega.segmentstore.server.tables.ContainerTableExtension;
import io.pravega.segmentstore.storage.SegmentHandle;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.shared.NameUtils;
import io.pravega.shared.segment.SegmentToContainerMapper;
import io.pravega.test.common.TestUtils;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.pravega.shared.NameUtils.getMetadataSegmentName;

/**
 * Utility methods for data recovery tests.
 */
@Slf4j
public class DataRecoveryTestUtils {
    private static final Duration TIMEOUT = Duration.ofSeconds(30);

    /**
     * Lists all segments from a given long term storage.
     * @param storage           Long term storage.
     * @return                  A map of lists containing segments by container Ids.
     * @throws                  IOException in case of exception during the execution.
     */
    public static void recoverAllSegments(Storage storage, Map<Integer, DebugStreamSegmentContainer> debugStreamSegmentContainers,
                                          ExecutorService executorService)
            throws IOException, InterruptedException {

        log.info("Recovery started for all containers...");
        Map<DebugStreamSegmentContainer, Set<String>> metadataSegmentsByContainer = new HashMap<>();
        for (DebugStreamSegmentContainer debugStreamSegmentContainer : debugStreamSegmentContainers.values()) {
            ContainerTableExtension ext = debugStreamSegmentContainer.getExtension(ContainerTableExtension.class);
            AsyncIterator<IteratorItem<TableKey>> it = ext.keyIterator(getMetadataSegmentName(debugStreamSegmentContainer.getId()),
                    IteratorArgs.builder().fetchTimeout(TIMEOUT).build()).join();

            // Add all segments present in the container metadata in a set.
            Set<String> metadataSegments = new HashSet<>();
            it.forEachRemaining(k -> metadataSegments.addAll(k.getEntries().stream().map(entry -> entry.getKey().toString())
                    .collect(Collectors.toSet())), executorService).join();
            metadataSegmentsByContainer.put(debugStreamSegmentContainer, metadataSegments);
        }

        SegmentToContainerMapper segToConMapper = new SegmentToContainerMapper(debugStreamSegmentContainers.size());
        Iterator<SegmentProperties> it = storage.listSegments();
        if (it == null) {
            log.info("No segments found in the long term storage.");
            return;
        }

        // Iterate through all segments. Create each one of their using their respective debugSegmentContainer instance.
        while (it.hasNext()) {
            SegmentProperties curr = it.next();
            int containerId = segToConMapper.getContainerId(curr.getName());
            metadataSegmentsByContainer.get(debugStreamSegmentContainers.get(containerId)).remove(curr.getName());
            executorService.execute(new SegmentRecovery(debugStreamSegmentContainers.get(containerId), curr));
        }

        for (DebugStreamSegmentContainer debugStreamSegmentContainer : metadataSegmentsByContainer.keySet()) {
            for (String segmentName : metadataSegmentsByContainer.get(debugStreamSegmentContainer)) {
                log.info("Deleting segment '{}' as it is not in storage", segmentName);
                debugStreamSegmentContainer.deleteStreamSegment(segmentName, TIMEOUT).join();
            }
        }
        executorService.shutdown();
        executorService.awaitTermination(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
        return;
    }

    /**
     * Creates all segments given in the list with the given DebugStreamSegmentContainer.
     */
    public static class SegmentRecovery implements Runnable {
        private final DebugStreamSegmentContainer container;
        private final SegmentProperties storageSegment;

        public SegmentRecovery(DebugStreamSegmentContainer container, SegmentProperties segment) {
            this.container = container;
            this.storageSegment = segment;
        }

        @Override
        public void run() {
            if (storageSegment == null) {
                return;
            }
            long len = storageSegment.getLength();
            boolean isSealed = storageSegment.isSealed();
            String segmentName = storageSegment.getName();

            /*
                1. segment exists in both metadata and storage, re-create it
                2. segment only in metadata, delete
                3. segment only in storage, re-create it
             */
            val registerSegment = container.registerExistingSegment(segmentName, len, isSealed);

            val streamSegmentInfo = container.getStreamSegmentInfo(storageSegment.getName(), TIMEOUT)
                    .thenAccept(e -> {
                        if (len != e.getLength() || isSealed != e.isSealed()) {
                            container.deleteStreamSegment(segmentName, TIMEOUT).join();
                            registerSegment.join();
                        }
                    });

            Futures.exceptionallyComposeExpecting(streamSegmentInfo, ex -> ex instanceof StreamSegmentNotExistsException, () -> registerSegment);
        }
    }

    /**
     * Deletes container-metadata segment and attribute index segment for the given container Id.
     * @param storage       Long term storage to delete the segments from.
     * @param containerId   Id of the container for which the segments has to be deleted.
     */
    public static void deleteContainerMetadataSegments(Storage storage, int containerId) {
        String segmentName = NameUtils.getMetadataSegmentName(containerId);
        deleteSegment(storage, segmentName);
        deleteSegment(storage, NameUtils.getAttributeSegmentName(segmentName));
    }

    /**
     * Deletes the segment with given segment name from the given long term storage.
     * @param tier2         Long term storage to delete the segment from.
     * @param segmentName   Name of the segment to be deleted.
     */
    static void deleteSegment(Storage tier2, String segmentName) {
        SegmentHandle segmentHandle = tier2.openWrite(segmentName).join();
        tier2.delete(segmentHandle, TIMEOUT).join();
    }
}
