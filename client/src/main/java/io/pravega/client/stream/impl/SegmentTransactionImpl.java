/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.client.stream.impl;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.TxnFailedException;
import io.pravega.client.stream.impl.segment.SegmentOutputStream;
import io.pravega.client.stream.impl.segment.SegmentSealedException;

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
