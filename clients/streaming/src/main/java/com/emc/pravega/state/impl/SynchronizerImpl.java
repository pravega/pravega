package com.emc.pravega.state.impl;

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
import com.emc.pravega.stream.impl.segment.EndOfSegmentException;
import com.emc.pravega.stream.impl.segment.SegmentInputStream;
import com.emc.pravega.stream.impl.segment.SegmentOutputStream;
import com.google.common.base.Preconditions;

import lombok.SneakyThrows;

public class SynchronizerImpl<StateT extends Revisioned, UpdateT extends Update<StateT>>
        implements Synchronizer<StateT, UpdateT> {

    private static final int UPDATE = 1;
    private static final int STATE = 2;
    private final SegmentInputStream in;
    private final SegmentOutputStream out;
    private final Serializer<UpdateT> updateSerializer;
    private final Serializer<StateT> stateSerializer;
   

    @Override
    public StateT getLatestState() {
        in.setOffset(0);
        ByteBuffer read;
        do {
            read = in.read();
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
        UpdateT update = decodeUpdate(read);
        return update.applyTo(state, new RevisionImpl(in.getOffset()));
    }
    
    @Override
    public StateT getLatestState(StateT localState) {
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
        out.conditionalWrite(offset, encodeUpdate(Collections.singletonList(update)), wasWritten);
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
        out.conditionalWrite(offset, encodeUpdate(update), wasWritten);
        if (!FutureHelpers.getAndHandleExceptions(wasWritten, RuntimeException::new)) { 
            return null;
        }
        return getLatestState(localState);
    }

    @Override
    public void compact(StateT compactState) {
        // TODO
    }
    
    private boolean isUpdate(ByteBuffer read) {
        byte type = read.get(read.position());
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
        byte type = read.get();
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
    
    private UpdateT decodeUpdate(ByteBuffer read) {
        byte type = read.get();
        Preconditions.checkState(type == UPDATE);
        return updateSerializer.deserialize(read);
    }

}
