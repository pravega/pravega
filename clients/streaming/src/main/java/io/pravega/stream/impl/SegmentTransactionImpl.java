/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.stream.impl;

import io.pravega.stream.Sequence;
import io.pravega.stream.Serializer;
import io.pravega.stream.TxnFailedException;
import io.pravega.stream.impl.segment.SegmentOutputStream;
import io.pravega.stream.impl.segment.SegmentSealedException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;

final class SegmentTransactionImpl<Type> implements SegmentTransaction<Type> {
    private final Serializer<Type> serializer;
    private final SegmentOutputStream out;
    private final UUID txId;
    private final Object lock = new Object();
    private final AtomicLong sequenceNumber = new AtomicLong(0);

    SegmentTransactionImpl(UUID txId, SegmentOutputStream out, Serializer<Type> serializer) {
        this.txId = txId;
        this.out = out;
        this.serializer = serializer;
    }

    @Override
    public void writeEvent(Type event) throws TxnFailedException {
        try {
            ByteBuffer buffer = serializer.serialize(event);
            synchronized (lock) {
                Sequence sequence = Sequence.create(0, sequenceNumber.incrementAndGet());
                out.write(new PendingEvent(null, sequence, buffer,  CompletableFuture.completedFuture(null)));
            }
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