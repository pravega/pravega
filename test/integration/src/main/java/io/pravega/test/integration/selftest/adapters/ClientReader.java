/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.selftest.adapters;

import com.google.common.base.Preconditions;
import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.segment.impl.NoSuchEventException;
import io.pravega.client.stream.EventPointer;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.Sequence;
import io.pravega.common.Exceptions;
import io.pravega.common.concurrent.CancellationToken;
import io.pravega.common.concurrent.Futures;
import io.pravega.common.util.ByteArraySegment;
import io.pravega.common.util.Retry;
import io.pravega.test.integration.selftest.Event;
import io.pravega.test.integration.selftest.TestConfig;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import javax.annotation.concurrent.GuardedBy;
import lombok.Data;
import lombok.SneakyThrows;

/**
 * StoreReader that reads from a Pravega Client.
 */
class ClientReader implements StoreReader, AutoCloseable {
    //region Members
    private static final ReaderConfig READER_CONFIG = ReaderConfig.builder().build();
    private static final ReaderGroupConfig READER_GROUP_CONFIG = ReaderGroupConfig.builder().startingPosition(Sequence.MIN_VALUE).build();
    private static final Retry.RetryAndThrowBase<Exception> READ_RETRY = Retry
            .withExpBackoff(1, 10, 4)
            .retryingOn(ReinitializationRequiredException.class)
            .throwingOn(Exception.class);
    private final URI controllerUri;
    private final TestConfig testConfig;
    private final ClientFactory clientFactory;
    private final ScheduledExecutorService executor;
    @GuardedBy("readers")
    private final HashMap<String, StreamReader> readers;
    @GuardedBy("readers")
    private boolean closed;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ClientReader class.
     *
     * @param controllerUri The Controller's URI.
     * @param clientFactory A ClientFactory to use.
     * @param executor      An executor to use for background async operations.
     */
    ClientReader(URI controllerUri, TestConfig testConfig, ClientFactory clientFactory, ScheduledExecutorService executor) {
        this.controllerUri = Preconditions.checkNotNull(controllerUri, "controllerUri");
        this.testConfig = Preconditions.checkNotNull(testConfig, "testConfig");
        this.clientFactory = Preconditions.checkNotNull(clientFactory, "clientFactory");
        this.executor = Preconditions.checkNotNull(executor, "executor");
        this.readers = new HashMap<>();
    }

    //endregion

    //region AutoCloseable Implementation

    @Override
    public void close() {
        Collection<StreamReader> toClose = null;
        synchronized (this.readers) {
            if (!this.closed) {
                toClose = new ArrayList<>(this.readers.values());
                this.readers.clear();
                this.closed = true;
            }
        }

        if (toClose != null) {
            toClose.forEach(StreamReader::close);
        }
    }

    //endregion

    //region StoreReader Implementation

    @Override
    public CompletableFuture<Void> readAll(String streamName, Consumer<ReadItem> eventHandler, CancellationToken cancellationToken) {
        return getReader(streamName).resumeReading(eventHandler, cancellationToken);
    }

    @Override
    public CompletableFuture<ReadItem> readExact(String streamName, Object address) {
        Exceptions.checkNotNullOrEmpty(streamName, "streamName");
        Preconditions.checkArgument(address instanceof EventPointer, "Unexpected address type.");
        EventPointer a = (EventPointer) address;
        StreamReader reader = getReader(streamName);
        return CompletableFuture.supplyAsync(() -> reader.readExact(a), this.executor);
    }

    @Override
    public CompletableFuture<Void> readAllStorage(String streamName, Consumer<Event> eventHandler, CancellationToken cancellationToken) {
        throw new UnsupportedOperationException("readAllStorage is not supported on ClientReader.");
    }

    private StreamReader getReader(String streamName) {
        synchronized (this.readers) {
            Exceptions.checkNotClosed(this.closed, this);
            StreamReader reader = this.readers.getOrDefault(streamName, null);
            if (reader == null) {
                reader = new StreamReader(streamName);
            }

            return reader;
        }
    }

    //endregion

    //region StreamReader

    private class StreamReader implements AutoCloseable {
        private final String readerGroup;
        private final String readerId;
        @GuardedBy("this")
        private boolean closed;
        @GuardedBy("this")
        private EventStreamReader<byte[]> reader;

        StreamReader(String streamName) {
            this.readerGroup = UUID.randomUUID().toString().replace("-", "");
            this.readerId = UUID.randomUUID().toString().replace("-", "");
            try (ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(ClientAdapterBase.SCOPE, ClientReader.this.controllerUri)) {
                readerGroupManager.createReaderGroup(this.readerGroup, READER_GROUP_CONFIG, Collections.singleton(streamName));
            }

            this.closed = false;
        }

        @Override
        public void close() {
            EventStreamReader<byte[]> r = null;
            synchronized (this) {
                if (!this.closed) {
                    r = this.reader;
                    this.reader = null;
                    this.closed = true;
                }
            }
            if (r != null) {
                r.close();
            }
        }

        CompletableFuture<Void> resumeReading(Consumer<ReadItem> eventHandler, CancellationToken cancellationToken) {
            return Futures.loop(
                    () -> canRead(cancellationToken),
                    () -> CompletableFuture.runAsync(() -> readNextItem(eventHandler), ClientReader.this.executor),
                    ClientReader.this.executor);
        }

        ReadItem readExact(EventPointer a) {
            try {
                byte[] data = getReader().fetchEvent(a);
                return toReadItem(data, a);
            } catch (NoSuchEventException e) {
                throw new CompletionException(e);
            }
        }

        @SneakyThrows
        private void readNextItem(Consumer<ReadItem> eventHandler) {
            EventRead<byte[]> readResult = READ_RETRY.run(() -> getReader().readNextEvent(ClientReader.this.testConfig.getTimeout().toMillis()));
            if (readResult.getEvent() == null) {
                // We are done.
                close();
                return;
            }

            StreamReadItem readItem = toReadItem(readResult.getEvent(), readResult.getEventPointer());
            eventHandler.accept(readItem);
        }

        private StreamReadItem toReadItem(byte[] data, EventPointer address) {
            return new StreamReadItem(new Event(new ByteArraySegment(data), 0), address);
        }

        private boolean canRead(CancellationToken cancellationToken) {
            if (cancellationToken.isCancellationRequested()) {
                return false;
            }
            synchronized (this) {
                return !this.closed;
            }
        }

        private synchronized EventStreamReader<byte[]> getReader() {
            Exceptions.checkNotClosed(this.closed, this);
            if (this.reader == null) {
                this.reader = ClientReader.this.clientFactory.createReader(this.readerId, this.readerGroup, ClientAdapterBase.SERIALIZER, READER_CONFIG);
            }

            return this.reader;
        }
    }

    //endregion

    //region StreamReadItem

    @Data
    private static class StreamReadItem implements ReadItem {
        private final Event event;
        private final Object address;

        @Override
        public String toString() {
            return String.format("Event = [%s], Address = [%s]", this.event, this.address);
        }
    }

    //endregion
}
