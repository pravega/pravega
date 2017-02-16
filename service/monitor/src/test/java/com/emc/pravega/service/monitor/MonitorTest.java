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
package com.emc.pravega.service.monitor;

import com.emc.pravega.common.netty.WireCommands;
import com.emc.pravega.controller.requests.ScaleRequest;
import com.emc.pravega.stream.EventStreamWriter;
import com.emc.pravega.stream.EventWriterConfig;
import com.emc.pravega.stream.Segment;
import com.emc.pravega.stream.Transaction;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.Future;

public class MonitorTest {

    private static final String SCOPE = "scope";
    private static final String STREAM = "stream";

    @Test
    public void monitorTest() {
        EventStreamWriter<ScaleRequest> writer = new EventStreamWriter<ScaleRequest>() {
            @Override
            public Future<Void> writeEvent(String routingKey, ScaleRequest event) {
                assert event != null;

                assert routingKey.equals(String.format("%s/%s", SCOPE, STREAM)) &&
                        event.getScope().equals(SCOPE) &&
                        event.getStream().equals(STREAM) &&
                        event.getDirection() == ScaleRequest.UP;
                return null;
            }

            @Override
            public Transaction<ScaleRequest> beginTxn(long transactionTimeout) {
                return null;
            }

            @Override
            public Transaction<ScaleRequest> getTxn(UUID transactionId) {
                return null;
            }

            @Override
            public EventWriterConfig getConfig() {
                return null;
            }

            @Override
            public void flush() {

            }

            @Override
            public void close() {

            }
        };

        ThresholdMonitor monitor = new ThresholdMonitor(writer);

        String streamSegmentName = Segment.getScopedName(SCOPE, STREAM, 0);
        monitor.notify(streamSegmentName, SegmentTrafficMonitor.NotificationType.SegmentCreated);
        long twentyminutesback = System.currentTimeMillis() - Duration.ofMinutes(20).toMillis();
        monitor.put(streamSegmentName, new ImmutablePair<>(twentyminutesback, twentyminutesback));

        monitor.process(streamSegmentName, 10, WireCommands.CreateSegment.IN_EVENTS_PER_SEC,
                twentyminutesback,
                1001, 500, 200, 200);
    }
}
