/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.pravega.service.server.host.stats;

import com.emc.pravega.common.netty.WireCommands;
import com.emc.pravega.service.contracts.SegmentInfo;
import com.emc.pravega.service.monitor.SegmentTrafficMonitor;
import lombok.Synchronized;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class SegmentStats {
    private static final long TWO_MINUTES = Duration.ofMinutes(2).toMillis();

    private static Map<String, SegmentAggregates> aggregatesMap = new HashMap<>();
    private static List<SegmentTrafficMonitor> monitors = new ArrayList<>();

    @Synchronized
    public static void addMonitor(SegmentTrafficMonitor monitor) {
        monitors.add(monitor);
    }

    public static void createSegment(SegmentInfo segment) {
        aggregatesMap.put(segment.getStreamSegmentName(), new SegmentAggregates(segment));
        monitors.forEach(x -> x.notify(segment.getStreamSegmentName(), SegmentTrafficMonitor.NotificationType.SegmentSealed));
    }

    public static void sealSegment(String streamSegmentName) {
        if (aggregatesMap.containsKey(streamSegmentName)) {
            aggregatesMap.remove(streamSegmentName);
            monitors.forEach(x -> x.notify(streamSegmentName, SegmentTrafficMonitor.NotificationType.SegmentSealed));
        }
    }

    public static void policyUpdate(SegmentInfo segment) {
        SegmentAggregates aggregates = aggregatesMap.get(segment.getStreamSegmentName());
        aggregates.targetRate = segment.getTargetRate();
        aggregates.scaleType = segment.getType();
    }

    /**
     * Updates segment specific aggregates.
     * Then if two minutes have elapsed between last report
     * of aggregates for this segment, send a new update to the monitor.
     * This update to the monitor is processed by monitor asynchronously.
     *
     * @param streamSegmentName stream segment name
     * @param dataLength        length of data that was written
     * @param numOfEvents       number of events that were written
     */
    public static void record(String streamSegmentName, long dataLength, int numOfEvents) {
        SegmentAggregates aggregates = aggregatesMap.get(streamSegmentName);
        if (aggregates.scaleType != WireCommands.CreateSegment.NO_SCALE) {
            aggregates.update(dataLength, numOfEvents);

            if (System.currentTimeMillis() - aggregates.lastReportedTime > TWO_MINUTES) {
                CompletableFuture.runAsync(() -> monitors.forEach(monitor -> monitor.process(streamSegmentName, aggregates.targetRate, aggregates.scaleType, aggregates.startTime,
                        aggregates.twoMinuteRate, aggregates.fiveMinuteRate, aggregates.tenMinuteRate, aggregates.twentyMinuteRate)));
                aggregates.lastReportedTime = System.currentTimeMillis();
            }
        }
    }

    /**
     * Method called with txn stats whenever a txn is committed.
     *
     * @param streamSegmentName parent segment name
     * @param dataLength        length of data written in txn
     * @param numOfEvents       number of events written in txn
     * @param txnCreationTime   time when txn was created
     */
    public static void merge(String streamSegmentName, long dataLength, int numOfEvents, long txnCreationTime) {
        SegmentAggregates aggregates = aggregatesMap.get(streamSegmentName);
        aggregates.updateTx(dataLength, numOfEvents, txnCreationTime);
    }
}
