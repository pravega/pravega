/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.client.stream.impl;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.TxnFailedException;
import io.pravega.client.stream.impl.segment.SegmentSealedException;
import io.pravega.client.stream.impl.segment.SegmentOutputStream;

final class SegmentTransactionImpl<Type> implements SegmentTransaction<Type> {
    private final Serializer<Type> serializer;
    private final SegmentOutputStream out;
    private final UUID txId;

    SegmentTransactionImpl(UUID txId, SegmentOutputStream out, Serializer<Type> serializer) {
        this.txId = txId;
        this.out = out;
        this.serializer = serializer;
    }

    @Override
    public void writeEvent(Type event) throws TxnFailedException {
        try {
            ByteBuffer buffer = serializer.serialize(event);
            out.write(new PendingEvent(null, buffer,  CompletableFuture.completedFuture(null)));
        } catch (SegmentSealedException e) {
            throw new TxnFailedException(e);
        }
    }

    @Override
    public UUID getId() {
        return txId;
    }

    @Override
    public void flush() throws TxnFailedException {
        try {
            out.flush();
        } catch (SegmentSealedException e) {
            throw new TxnFailedException(e);
        }
    }

    @Override
    public void close() throws TxnFailedException {
        try {
            out.close();
        } catch (SegmentSealedException e) {
            throw new TxnFailedException(e);
        }
    }

}