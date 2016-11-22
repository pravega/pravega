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

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.emc.pravega.common.concurrent.FutureHelpers;
import com.emc.pravega.state.Revisioned;
import com.emc.pravega.state.Synchronizer;
import com.emc.pravega.state.Update;
import com.emc.pravega.stream.Serializer;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.impl.segment.EndOfSegmentException;
import com.emc.pravega.stream.impl.segment.SegmentInputStream;
import com.emc.pravega.stream.impl.segment.SegmentOutputStream;
import com.emc.pravega.stream.impl.segment.SegmentSealedException;
import com.google.common.base.Preconditions;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;

@RequiredArgsConstructor
public class SynchronizerImpl<StateT extends Revisioned, UpdateT extends Update<StateT>>
        implements Synchronizer<StateT, UpdateT> {

    private static final int UPDATE = 1;
    private static final int STATE = 2;
    @Getter
    private final Stream stream;
    private final SegmentInputStream in;
    private final SegmentOutputStream out;
    private final Serializer<UpdateT> updateSerializer;
    private final Serializer<StateT> stateSerializer;
   

    @Override
    public StateT getLatestState() {
        in.setOffset(0);
        ByteBuffer read;
        do {
            try {
                read = in.read();
            } catch (EndOfSegmentException e) {
                throw new CorruptedStateException("Unexpected end of segment ", e);
            }
        } while(isUpdate(read));
        StateT state = decodeState(read);
        in.setOffset(state.getRevision().asImpl().getOffsetInSegment());
        while (!in.wasReadAtTail()) {
            state = applyNextUpdateIfAvailable(state, true);
        }
        return state;
    }

    @SneakyThrows(EndOfSegmentException.class)
    StateT applyNextUpdateIfAvailable(StateT state, boolean breakAtTail) {
        ByteBuffer read;
        do {
            if (breakAtTail && in.wasReadAtTail()) {
                return state;
            }
            read = in.read();
        } while (!isUpdate(read));
        StateT result = state;
        for (UpdateT update : decodeUpdate(read)) {
            result = update.applyTo(state, new RevisionImpl(in.getOffset()));
        }
        return result;
    }
    
    @Override
    public StateT getLatestState(StateT localState) {
        if (localState == null) {
            return getLatestState();
        }
        StateT state = localState;
        in.setOffset(state.getRevision().asImpl().getOffsetInSegment());
        state = applyNextUpdateIfAvailable(state, false);
        while (!in.wasReadAtTail()) {
            state = applyNextUpdateIfAvailable(state, true);
        }
        return state;
    }

    @Override
    public StateT updateState(StateT localState, UpdateT update, boolean conditionalOnLatest) {
        RevisionImpl revision = localState.getRevision().asImpl();
        CompletableFuture<Boolean> wasWritten = new CompletableFuture<>();
        long offset = revision.getOffsetInSegment();
        try {
            out.conditionalWrite(offset, encodeUpdate(Collections.singletonList(update)), wasWritten);
        } catch (SegmentSealedException e) {
            throw new CorruptedStateException("Unexpected end of segment ", e);
        }
        if (!FutureHelpers.getAndHandleExceptions(wasWritten, RuntimeException::new)) { 
            return null;
        } 
        return getLatestState(localState);
    }

    @Override
    public StateT updateState(StateT localState, List<? extends UpdateT> update, boolean conditionalOnLatest) {
        RevisionImpl revision = localState.getRevision().asImpl();
        CompletableFuture<Boolean> wasWritten = new CompletableFuture<>();
        long offset = revision.getOffsetInSegment();
        try {
            out.conditionalWrite(offset, encodeUpdate(update), wasWritten);
        } catch (SegmentSealedException e) {
            throw new CorruptedStateException("Unexpected end of segment ", e);
        }
        if (!FutureHelpers.getAndHandleExceptions(wasWritten, RuntimeException::new)) { 
            return null;
        }
        return getLatestState(localState);
    }

    @Override
    public void compact(StateT compactState) {
        CompletableFuture<Boolean> wasWritten = new CompletableFuture<>();
        try {
            out.write(encodeState(compactState), wasWritten);
        } catch (SegmentSealedException e) {
            throw new CorruptedStateException("Unexpected end of segment ", e);
        }
        FutureHelpers.getAndHandleExceptions(wasWritten, RuntimeException::new);
    }
    
    private boolean isUpdate(ByteBuffer read) {
        int type = read.getInt(read.position());
        if (type == UPDATE) {
            return true;
        } else if (type == STATE) {
            return false;
        } else {
            throw new CorruptedStateException("Update of unknown type");
        }
    }
    
    private ByteBuffer encodeState(StateT state) {
        ByteBuffer buffer = stateSerializer.serialize(state);
        ByteBuffer result = ByteBuffer.allocate(buffer.capacity() + 4);
        result.putInt(STATE);
        result.put(buffer);
        result.rewind();
        return result;
    }
    
    private StateT decodeState(ByteBuffer read) {
        int type = read.getInt();
        Preconditions.checkState(type == STATE);
        return stateSerializer.deserialize(read);
    }
    
    private ByteBuffer encodeUpdate(List<? extends UpdateT> updates) {
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

}
