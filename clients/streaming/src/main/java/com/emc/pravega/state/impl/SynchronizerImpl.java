/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.state.impl;

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;

import javax.annotation.concurrent.GuardedBy;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.state.InitialUpdate;
import com.emc.pravega.state.Revisioned;
import com.emc.pravega.state.Synchronizer;
import com.emc.pravega.state.Update;
import com.emc.pravega.stream.Serializer;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.impl.segment.EndOfSegmentException;
import com.emc.pravega.stream.impl.segment.SegmentInputStream;
import com.emc.pravega.stream.impl.segment.SegmentOutputStream;
import com.emc.pravega.stream.impl.segment.SegmentSealedException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class SynchronizerImpl<StateT extends Revisioned, UpdateT extends Update<StateT>, InitT extends InitialUpdate<StateT>>
        implements Synchronizer<StateT, UpdateT, InitT> {

    private static final int INITIALIZATION = 1;
    private static final int UPDATE = 2;
    @Getter
    private final Stream stream;
    @GuardedBy("lock")
    private final SegmentInputStream in;
    @GuardedBy("lock")
    private final SegmentOutputStream out;
    private final Serializer<UpdateT> updateSerializer;
    private final Serializer<InitT> initSerializer;
    private final Object lock = new Object();

    @Override
    public StateT getLatestState() {
        try {
            Entry<Long, ByteBuffer> initial = getInit(0);
            InitialUpdate<StateT> init = decodeInit(initial.getValue());
            Map<Long, ByteBuffer> updates = getUpdates(initial.getKey(), false);
            StateT state = init.create(new RevisionImpl(initial.getKey(), 0));
            return applyUpdates(updates, state);
        } catch (EndOfSegmentException e) {
            throw new CorruptedStateException("Unexpected end of segment ", e);
        }
    }

    private StateT applyUpdates(Map<Long, ByteBuffer> updates, StateT state) {
        for (Entry<Long, ByteBuffer> entry : updates.entrySet()) {
            long offset = entry.getKey();
            state = applyUpdate(state, offset, entry.getValue());
        }
        return state;
    }

    private StateT applyUpdate(StateT state, long offset, ByteBuffer read) {
        int i = 0;
        for (UpdateT update : decodeUpdate(read)) {
            state = update.applyTo(state, new RevisionImpl(offset, i++));
        }
        return state;
    }

    private Entry<Long, ByteBuffer> getInit(long offset) throws EndOfSegmentException {
        synchronized(lock) {
            ByteBuffer read;
            in.setOffset(offset);
            do {
                read = in.read();
            } while (isUpdate(read));
            return new AbstractMap.SimpleImmutableEntry<>(in.getOffset(), read);
        }
    }
    
    private Map<Long, ByteBuffer> getUpdates(long offset, boolean atLeastOne) throws EndOfSegmentException {
        synchronized(lock) {
            in.setOffset(offset);
            Map<Long, ByteBuffer> updates = new LinkedHashMap<>();
            long currentStreamLength = in.fetchCurrentStreamLength();
            while (in.getOffset() < currentStreamLength || (atLeastOne && updates.isEmpty())) {
                ByteBuffer read = in.read();
                if (isUpdate(read)) {
                    updates.put(in.getOffset(), read);
                }
            }
            return updates;
        }
    }

    @Override
    public StateT getLatestState(StateT localState) {
        if (localState == null) {
            return getLatestState();
        }
        try {
            long offset = localState.getRevision().asImpl().getOffsetInSegment();
            Map<Long, ByteBuffer> updates = getUpdates(offset, true);
            return applyUpdates(updates, localState);
        } catch (EndOfSegmentException e) {
            throw new CorruptedStateException("Unexpected end of segment ", e);
        }
    }

    @Override
    public StateT conditionallyUpdateState(StateT localState, UpdateT update) {
        return conditionallyUpdateState(localState, Collections.singletonList(update));
    }
    
    @Override
    public StateT conditionallyUpdateState(StateT localState, List<? extends UpdateT> update) {
        CompletableFuture<Boolean> wasWritten = new CompletableFuture<>();
        long offset = localState.getRevision().asImpl().getOffsetInSegment();
        try {
            synchronized(lock) {
                out.conditionalWrite(offset, encodeUpdate(update), wasWritten);
            }
        } catch (SegmentSealedException e) {
            throw new CorruptedStateException("Unexpected end of segment ", e);
        }
        if (!FutureHelpers.getAndHandleExceptions(wasWritten, RuntimeException::new)) {
            return null;
        } else {
            return getLatestState(localState);
        }
    }
    
    @Override
    public StateT unconditionallyUpdateState(StateT localState, UpdateT update) {
        return unconditionallyUpdateState(localState, Collections.singletonList(update));
    }
    
    @Override
    public StateT unconditionallyUpdateState(StateT localState, List<? extends UpdateT> update) {
        CompletableFuture<Boolean> wasWritten = new CompletableFuture<>();
        try {
            synchronized(lock) {
                out.write(encodeUpdate(update), wasWritten);
            }
        } catch (SegmentSealedException e) {
            throw new CorruptedStateException("Unexpected end of segment ", e);
        }
        FutureHelpers.getAndHandleExceptions(wasWritten, RuntimeException::new);
        return getLatestState(localState);
    }

    @Override
    public void compact(StateT localState, InitT compaction) {
        CompletableFuture<Boolean> wasWritten = new CompletableFuture<>();
        try {
            synchronized(lock) {
                out.write(encodeInit(compaction), wasWritten);
            }
        } catch (SegmentSealedException e) {
            throw new CorruptedStateException("Unexpected end of segment ", e);
        }
        FutureHelpers.getAndHandleExceptions(wasWritten, RuntimeException::new);
    }

    private boolean isUpdate(ByteBuffer read) {
        int type = read.getInt(read.position());
        if (type == UPDATE) {
            return true;
        } else if (type == INITIALIZATION) {
            return false;
        } else {
            throw new CorruptedStateException("Update of unknown type");
        }
    }

    @VisibleForTesting
    ByteBuffer encodeInit(InitT init) {
        ByteBuffer buffer = initSerializer.serialize(init);
        ByteBuffer result = ByteBuffer.allocate(buffer.capacity() + 4);
        result.putInt(INITIALIZATION);
        result.put(buffer);
        result.rewind();
        return result;
    }

    private InitialUpdate<StateT> decodeInit(ByteBuffer read) {
        int type = read.getInt();
        Preconditions.checkState(type == INITIALIZATION);
        return initSerializer.deserialize(read);
    }

    @VisibleForTesting
    ByteBuffer encodeUpdate(List<? extends UpdateT> updates) {
        List<ByteBuffer> serializedUpdates = new ArrayList<>();
        int size = 0;
        for (UpdateT u : updates) {
            ByteBuffer serialized = updateSerializer.serialize(u);
            size += serialized.remaining();
            serializedUpdates.add(serialized);
        }
        ByteBuffer result = ByteBuffer.allocate(size + 4 + serializedUpdates.size() * 4);
        result.putInt(UPDATE);
        for (ByteBuffer update : serializedUpdates) {
            result.putInt(update.remaining());
            result.put(update);
        }
        result.rewind();
        return result;
    }

    private List<UpdateT> decodeUpdate(ByteBuffer read) {
        ArrayList<UpdateT> result = new ArrayList<>();
        int origionalLimit = read.limit();
        int type = read.getInt();
        Preconditions.checkState(type == UPDATE);
        while (read.hasRemaining()) {
            int updateLength = read.getInt();
            int position = read.position();
            read.limit(read.position() + updateLength);
            result.add(updateSerializer.deserialize(read));
            read.limit(origionalLimit);
            read.position(position + updateLength);
        }
        return result;
    }

    @Override
    public StateT initialize(InitT initializer) {
        CompletableFuture<Boolean> wasWritten = new CompletableFuture<>();
        try {
            synchronized(lock) {
                out.conditionalWrite(0, encodeInit(initializer), wasWritten);
            }
        } catch (SegmentSealedException e) {
            throw new CorruptedStateException("Unexpected end of segment ", e);
        }
        FutureHelpers.getAndHandleExceptions(wasWritten, RuntimeException::new);
        return getLatestState();        
    }

}
