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
     * Constructor to initiate an instance of SegmentEventWriter Impl.
     *
     * @param out The segment output stream for writing.
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
