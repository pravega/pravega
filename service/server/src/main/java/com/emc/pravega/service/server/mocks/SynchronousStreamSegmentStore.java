/**
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.emc.pravega.service.server.mocks;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.service.contracts.AppendContext;
import com.emc.pravega.service.contracts.ReadResult;
import com.emc.pravega.service.contracts.SegmentProperties;
import com.emc.pravega.service.contracts.StreamSegmentStore;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import lombok.RequiredArgsConstructor;

/**
 * Wraps a {@link StreamSegmentStore} and turns the async calls into synchronous ones by blocking on
 * the futures for test purposes. 
 */
@RequiredArgsConstructor
public class SynchronousStreamSegmentStore implements StreamSegmentStore {

    private final StreamSegmentStore impl;

    @Override
    public CompletableFuture<Void> append(String streamSegmentName, byte[] data, AppendContext appendContext,
            Duration timeout) {
        CompletableFuture<Void> result = impl.append(streamSegmentName, data, appendContext, timeout);
        FutureHelpers.await(result);
        return result;
    }

    @Override
    public CompletableFuture<Void> append(String streamSegmentName, long offset, byte[] data,
            AppendContext appendContext, Duration timeout) {
        CompletableFuture<Void> result = impl.append(streamSegmentName, offset, data, appendContext, timeout);
        FutureHelpers.await(result);
        return result;
    }

    @Override
    public CompletableFuture<ReadResult> read(String streamSegmentName, long offset, int maxLength, Duration timeout) {
        CompletableFuture<ReadResult> result = impl.read(streamSegmentName, offset, maxLength, timeout);
        FutureHelpers.await(result);
        return result;
    }

    @Override
    public CompletableFuture<SegmentProperties> getStreamSegmentInfo(String streamSegmentName, Duration timeout) {
        CompletableFuture<SegmentProperties> result = impl.getStreamSegmentInfo(streamSegmentName, timeout);
        FutureHelpers.await(result);
        return result;
    }

    @Override
    public CompletableFuture<Void> createStreamSegment(String streamSegmentName, Duration timeout) {
        CompletableFuture<Void> result = impl.createStreamSegment(streamSegmentName, timeout);
        FutureHelpers.await(result);
        return result;
    }

    @Override
    public CompletableFuture<String> createTransaction(String parentStreamSegmentName, UUID transactionId,
            Duration timeout) {
        CompletableFuture<String> result = impl.createTransaction(parentStreamSegmentName, transactionId, timeout);
        FutureHelpers.await(result);
        return result;
    }

    @Override
    public CompletableFuture<Long> mergeTransaction(String transactionName, Duration timeout) {
        CompletableFuture<Long> result = impl.mergeTransaction(transactionName, timeout);
        FutureHelpers.await(result);
        return result;
    }

    @Override
    public CompletableFuture<Long> sealStreamSegment(String streamSegmentName, Duration timeout) {
        CompletableFuture<Long> result = impl.sealStreamSegment(streamSegmentName, timeout);
        FutureHelpers.await(result);
        return result;
    }

    @Override
    public CompletableFuture<Void> deleteStreamSegment(String streamSegmentName, Duration timeout) {
        CompletableFuture<Void> result = impl.deleteStreamSegment(streamSegmentName, timeout);
        FutureHelpers.await(result);
        return result;
    }

    @Override
    public CompletableFuture<AppendContext> getLastAppendContext(String streamSegmentName, UUID clientId,
            Duration timeout) {
        CompletableFuture<AppendContext> result = impl.getLastAppendContext(streamSegmentName, clientId, timeout);
        FutureHelpers.await(result);
        return result;
    }

}
