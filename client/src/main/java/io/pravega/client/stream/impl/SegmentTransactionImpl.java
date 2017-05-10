/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import io.pravega.client.segment.impl.SegmentOutputStream;
import io.pravega.client.segment.impl.SegmentSealedException;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.TxnFailedException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import javax.annotation.concurrent.GuardedBy;

final class SegmentTransactionImpl<Type> implements SegmentTransaction<Type> {
    private final Serializer<Type> serializer;
    private final SegmentOutputStream out;
    private final UUID txId;
    private final Object lock = new Object();
    @GuardedBy("lock")
    private long sequenceNumber = 0;

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
                sequenceNumber++;
                out.write(new PendingEvent(null, sequenceNumber, buffer,  CompletableFuture.completedFuture(null)));
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