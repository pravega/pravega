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

package com.emc.pravega.service.server.host.selftest;

import com.emc.pravega.service.contracts.ReadResult;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

/**
 * Created by andrei on 10/12/16.
 */
class ConsoleStoreAdapter implements StoreAdapter {
    @Override
    public CompletableFuture<Void> append(String streamSegmentName, byte[] data, Duration timeout) {
        TestLogger.log("CSA", "Append Segment=%s, Length=%d.", streamSegmentName, data.length);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<ReadResult> readFromStore(String streamSegmentName, long offset, int maxLength, Duration timeout) {
        TestLogger.log("CSA", "Read Segment=%s, Offset=%d, Length=%d.", streamSegmentName, offset, maxLength);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> createStreamSegment(String streamSegmentName, Duration timeout) {
        TestLogger.log("CSA", "Create Segment=%s.", streamSegmentName);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<String> createTransaction(String parentStreamSegmentName, Duration timeout) {
        TestLogger.log("CSA", "Create Transaction Parent=%s.", parentStreamSegmentName);
        return CompletableFuture.completedFuture(parentStreamSegmentName + System.nanoTime());
    }

    @Override
    public CompletableFuture<Void> mergeTransaction(String transactionName, Duration timeout) {
        TestLogger.log("CSA", "Merge Transaction=%s.", transactionName);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> sealStreamSegment(String streamSegmentName, Duration timeout) {
        TestLogger.log("CSA", "Seal Segment=%s.", streamSegmentName);
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> deleteStreamSegment(String streamSegmentName, Duration timeout) {
        TestLogger.log("CSA", "Delete Segment=%s.", streamSegmentName);
        return CompletableFuture.completedFuture(null);
    }
}
