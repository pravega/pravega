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

package com.emc.pravega.service.server.containers;

import com.emc.pravega.common.ObjectClosedException;
import com.emc.pravega.service.server.logs.operations.StreamSegmentAppendOperation;
import com.emc.pravega.testcommon.AssertExtensions;
import com.emc.pravega.testcommon.IntentionalException;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

/**
 * Unit tests for the PendingAppendsCollection class.
 */
public class PendingAppendsCollectionTests {
    private static final int SEGMENT_COUNT = 10;
    private static final int CLIENTS_PER_SEGMENT = 10;
    private static final int APPENDS_PER_CLIENT = 10;
    private static final byte[] APPEND_DATA = "hello".getBytes();

    /**
     * Tests the ability to register an append and then get the correct AppendContext for it.
     */
    @Test
    @SuppressWarnings("checkstyle:CyclomaticComplexity")
    public void testRegisterGet() {
        HashMap<Long, HashMap<UUID, AppendContext>> lastContexts = new HashMap<>();
        CompletableFuture<Long> doneFirstThird = new CompletableFuture<>();
        CompletableFuture<Long> doneSecondThird = new CompletableFuture<>();
        CompletableFuture<Long> doneThirdThird = new CompletableFuture<>();
        try (PendingAppendsCollection c = new PendingAppendsCollection()) {
            for (long segmentId = 0; segmentId < SEGMENT_COUNT; segmentId++) {
                HashMap<UUID, AppendContext> lastSegmentContexts = new HashMap<>();
                lastContexts.put(segmentId, lastSegmentContexts);
                for (int clientId = 0; clientId < CLIENTS_PER_SEGMENT; clientId++) {
                    for (int appendId = 0; appendId < APPENDS_PER_CLIENT; appendId++) {
                        AppendContext ac = new AppendContext(UUID.randomUUID(), appendId);
                        lastSegmentContexts.put(ac.getClientId(), ac);
                        StreamSegmentAppendOperation op = new StreamSegmentAppendOperation(segmentId, APPEND_DATA, ac);
                        if (segmentId < SEGMENT_COUNT / 3) {
                            c.register(op, doneFirstThird);
                        } else if (segmentId < SEGMENT_COUNT * 2 / 3) {
                            c.register(op, doneSecondThird);
                        } else {
                            c.register(op, doneThirdThird);
                        }

                        CompletableFuture<AppendContext> contextFuture = c.get(op.getStreamSegmentId(), ac.getClientId());
                        Assert.assertFalse("get() returned a completed future even though the underlying append did not complete.", contextFuture.isDone());
                    }
                }
            }

            // Complete all the "appends" in the first third.
            doneFirstThird.complete(1L);

            // Make sure that there is nothing in the collection for the first half, and that the second half still returns
            // unfinished futures.
            HashMap<AppendContext, CompletableFuture<AppendContext>> secondThirdContexts = new HashMap<>();
            HashMap<AppendContext, CompletableFuture<AppendContext>> thirdThirdContexts = new HashMap<>();
            for (long segmentId : lastContexts.keySet()) {
                for (UUID clientId : lastContexts.get(segmentId).keySet()) {
                    CompletableFuture<AppendContext> contextFuture = c.get(segmentId, clientId);
                    if (segmentId < SEGMENT_COUNT / 3) {
                        Assert.assertNull("get() returned a non-null result for a Segment-Client combination that had completed all appends.", contextFuture);
                    } else {
                        Assert.assertNotNull("get() returned a null result for a Segment-Client combination that had not completed all appends.", contextFuture);
                        Assert.assertFalse("get() returned a completed future even though the underlying append did not complete.", contextFuture.isDone());
                        if (segmentId < SEGMENT_COUNT * 2 / 3) {
                            secondThirdContexts.put(lastContexts.get(segmentId).get(clientId), contextFuture);
                        } else {
                            thirdThirdContexts.put(lastContexts.get(segmentId).get(clientId), contextFuture);
                        }
                    }
                }
            }

            // Complete all the "appends" in the second third.
            doneSecondThird.complete(2L);
            for (Map.Entry<AppendContext, CompletableFuture<AppendContext>> e : secondThirdContexts.entrySet()) {
                Assert.assertTrue("Future returned from get() did not complete even though the underlying append did complete.", e.getValue().isDone());
                Assert.assertEquals("Unexpected result contained in returned future.", e.getKey(), e.getValue().join());
            }

            // Fail all the "appends" in the third third.
            doneThirdThird.completeExceptionally(new IntentionalException());
            for (Map.Entry<AppendContext, CompletableFuture<AppendContext>> e : thirdThirdContexts.entrySet()) {
                Assert.assertTrue("Future returned from get() did not complete even though the underlying append did complete.", e.getValue().isDone());
                AssertExtensions.assertThrows(
                        "Future returned from get() did not complete exceptionally even though the underlying append failed.",
                        e.getValue()::join,
                        ex -> ex instanceof IntentionalException);
            }
        }
    }

    /**
     * Verifies that close() fails all pending appends.
     */
    @Test
    public void testClose() {
        PendingAppendsCollection c = new PendingAppendsCollection();
        long segmentId = 1;
        UUID clientId = UUID.randomUUID();
        c.register(new StreamSegmentAppendOperation(segmentId, APPEND_DATA, new AppendContext(clientId, 0)), new CompletableFuture<>());
        CompletableFuture<AppendContext> contextFuture = c.get(segmentId, clientId);
        c.close();

        Assert.assertTrue("Future returned from get() did not complete even though the PendingAppendCollection closed.", contextFuture.isDone());
        AssertExtensions.assertThrows(
                "Future returned from get() did not complete exceptionally even though the PendingAppendCollection closed.",
                contextFuture::join,
                ex -> ex instanceof ObjectClosedException);
    }
}
