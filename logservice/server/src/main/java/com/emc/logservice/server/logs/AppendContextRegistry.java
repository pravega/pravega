package com.emc.logservice.server.logs;

import com.emc.logservice.contracts.AppendContext;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by andrei on 6/1/16.
 */
public class AppendContextRegistry {
    private final ConcurrentHashMap<String, Entry> entries;

    public AppendContextRegistry() {
        this.entries = new ConcurrentHashMap<>();
    }

    public SimpleCallback add(long streamSegmentId, AppendContext context) {
        Entry e = new Entry(getKey(streamSegmentId, context.getClientId()), context, false);
        this.entries.put(e.key, e);
        return e;
    }

    public void addCompleted(long streamSegmentId, AppendContext context) {
        Entry e = new Entry(getKey(streamSegmentId, context.getClientId()), context, false);
        this.entries.put(e.key, e);
    }

    public CompletableFuture<AppendContext> get(long streamSegmentId, UUID clientId) {
        String key = getKey(streamSegmentId, clientId);
        Entry e = this.entries.getOrDefault(key, null);
        if (e == null) {
            return null;
        }
        else {
            return e.completionFuture;
        }
    }

    private String getKey(long streamSegmentId, UUID connectionId) {
        return Long.toHexString(streamSegmentId) + "|" + connectionId.toString();
    }

    private static class Entry implements SimpleCallback {
        public final String key;
        public final AppendContext context;
        public final CompletableFuture<AppendContext> completionFuture;

        public Entry(String key, AppendContext context, boolean isComplete) {
            this.key = key;
            this.context = context;
            this.completionFuture = isComplete ? CompletableFuture.completedFuture(this.context) : new CompletableFuture<>();
        }

        //region SimpleCallback Implementation

        @Override
        public void complete() {
            this.completionFuture.complete(this.context);
        }

        @Override
        public void fail(Throwable ex) {
            this.completionFuture.completeExceptionally(ex);
        }

        @Override
        public boolean isDone() {
            return this.completionFuture.isDone();
        }

        //endregion

        @Override
        public String toString() {
            return String.format("%s: %s (%s)", this.key, this.context, this.completionFuture.isDone() ? (this.completionFuture.isCompletedExceptionally() ? "Error" : "Complete") : "Pending");
        }
    }
}
