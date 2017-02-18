/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.stream.impl;

import com.emc.pravega.stream.Serializer;
import com.emc.pravega.stream.impl.segment.SegmentOutputStream;
import com.emc.pravega.stream.impl.segment.SegmentSealedException;
import com.google.common.base.Preconditions;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Sends events to the SegmentOutputStream and tracks the ones that are outstanding.
 */
public class SegmentEventWriterImpl<Type> implements SegmentEventWriter<Type> {

    private final Serializer<Type> serializer;

    private final SegmentOutputStream out;
    private final Vector<PendingEvent<Type>> outstanding = new Vector<>();
    private final AtomicBoolean sealed = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    /**
     * Creates a new instance of the SegmentEventWriter class.
     *
     * @param out        The segment output stream for writing.
     * @param serializer The serializer to be applied to events.
     */
    public SegmentEventWriterImpl(SegmentOutputStream out, Serializer<Type> serializer) {
        Preconditions.checkNotNull(out);
        Preconditions.checkNotNull(serializer);
        this.serializer = serializer;
        this.out = out;
    }

    @Override
    public void write(PendingEvent<Type> m) throws SegmentSealedException {
        checkSealedAndClosed();
        ByteBuffer buffer = serializer.serialize(m.getValue());
        out.write(buffer, m.getAckFuture());
        outstanding.add(m);
    }

    @Override
    public void flush() throws SegmentSealedException {
        checkSealedAndClosed();
        try {
            out.flush();
        } catch (SegmentSealedException e) {
            sealed.set(true);
            throw e;
        }
    }

    @Override
    public void close() throws SegmentSealedException {
        Preconditions.checkState(!sealed.get(), "Already Sealed");
        if (closed.get()) {
            return;
        }
        try {
            out.close();
        } catch (SegmentSealedException e) {
            sealed.set(true);
            throw e;
        }
    }

    private void checkSealedAndClosed() {
        Preconditions.checkState(!sealed.get(), "Already Sealed");
        Preconditions.checkState(!closed.get(), "Already Closed");
    }

    @Override
    public List<PendingEvent<Type>> getUnackedEvents() {
        return new ArrayList<>(outstanding);
    }

    @Override
    public boolean isAlreadySealed() {
        return sealed.get();
    }

}
