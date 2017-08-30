/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import com.google.common.base.Preconditions;
import io.pravega.client.segment.impl.SegmentOutputStream;
import io.pravega.client.segment.impl.SegmentSealedException;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.TxnFailedException;
import io.pravega.common.concurrent.FutureHelpers;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.concurrent.GuardedBy;
import lombok.val;

final class SegmentTransactionImpl<Type> implements SegmentTransaction<Type> {
    private final Serializer<Type> serializer;
    private final UUID txId;
    private final Object lock = new Object();
    @GuardedBy("lock")
    private final SegmentOutputStream out;
    @GuardedBy("lock")
    private final LinkedList<CompletableFuture<Boolean>> outstanding = new LinkedList<>();
    private final AtomicReference<Throwable> txnFailedCause = new AtomicReference<>();

    SegmentTransactionImpl(UUID txId, SegmentOutputStream out, Serializer<Type> serializer) {
        this.txId = txId;
        this.out = out;
        this.serializer = serializer;
    }

    @Override
    public void writeEvent(Type event) throws TxnFailedException {
        checkFailed();
        ByteBuffer buffer = serializer.serialize(event);
        CompletableFuture<Boolean> ack = new CompletableFuture<Boolean>();
        PendingEvent pendingEvent = new PendingEvent(null, buffer, ack);
        synchronized (lock) {
            out.write(pendingEvent);
            outstanding.addLast(ack);
            removeCompleted();
        }
        checkFailed();
    }

    private void checkFailed() throws TxnFailedException {
        Throwable cause = txnFailedCause.get();
        if (cause != null) {
            throw new TxnFailedException(cause);
        }
    }

    @GuardedBy("lock")
    private void removeCompleted() {
        for (Iterator<CompletableFuture<Boolean>> iter = outstanding.iterator(); iter.hasNext();) {
            val ack = iter.next();
            if (ack.isDone()) {
                Throwable exception = FutureHelpers.getException(ack);
                if (exception != null) {
                    txnFailedCause.compareAndSet(null, exception);
                }
                iter.remove();
            } else {
                break;
            }
        }
    }

    @Override
    public UUID getId() {
        return txId;
    }

    @Override
    public void flush() throws TxnFailedException {
        checkFailed();
        try {
            out.flush();
            synchronized (lock) {
                removeCompleted();
                checkFailed();
                Preconditions.checkState(outstanding.isEmpty());
            }
        } catch (SegmentSealedException e) {
            throw new TxnFailedException(e);
        }
    }

    @Override
    public void close() throws TxnFailedException {
        flush();
        try {
            out.close();
        } catch (SegmentSealedException e) {
            throw new TxnFailedException(e);
        }
    }

}