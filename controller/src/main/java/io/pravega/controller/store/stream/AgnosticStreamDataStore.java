/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.store.stream;

import io.pravega.client.ClientConfig;
import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.state.Revision;
import io.pravega.client.state.RevisionedStreamClient;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.common.lang.AtomicInt96;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.rpc.auth.GrpcAuthHelper;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/** A stream data path store that neither knows about the type of storeclient nor the segmentstore
 * and interfaces only with the user over the controller REST endpoint **/
@Slf4j
class AgnosticStreamDataStore implements StreamDataStore {

    private final AtomicInt96 counter;

    private final Executor executor;

    @Getter(AccessLevel.PACKAGE)
    private final AgnosticStreamDataStoreHelper storeHelper;

    private final StreamMetadataStore streamMetadataStore;

    private final GrpcAuthHelper grpcAuthHelper;

    AgnosticStreamDataStore(final StreamMetadataStore streamMetadataStore,
                            final SegmentHelper segmentHelper,
                            final CuratorFramework cf,
                            Executor executor,
                            GrpcAuthHelper grpcAuthHelper) {
        this.streamMetadataStore = streamMetadataStore;
        this.executor = executor;
        this.counter = new AtomicInt96();
        this.grpcAuthHelper = grpcAuthHelper;
        this.storeHelper = new AgnosticStreamDataStoreHelper(segmentHelper, cf, executor);
    }
    
    @Override
    public void close() throws IOException {

    }

    /**
     * Appends an event to the stream.
     *
     * @param routingKey Name of routingKey to be used.
     * @param scopeName Name of scope to be used.
     * @param streamName Name of stream to be used.
     * @param message raw data to be appended to stream.
     */
    @Override
    public CompletableFuture<Void> createEvent(final String  routingKey,
                                               final String scopeName,
                                               final String streamName,
                                               final String message) {
        CompletableFuture<Void> ack = CompletableFuture.completedFuture(null);
        ClientConfig clientConfig = this.getStoreHelper().getSegmentHelper().getConnectionFactory().getClientConfig();
        SynchronizerClientFactory synchronizerClientFactory = SynchronizerClientFactory.withScope(scopeName, ClientConfig.builder().build());
        RevisionedStreamClient<String> revisedStreamClient = synchronizerClientFactory.createRevisionedStreamClient(
                streamName,
                new JavaSerializer<String>(), SynchronizerConfig.builder().build());
        Revision r = revisedStreamClient.fetchLatestRevision();
        revisedStreamClient.writeConditionally(r, message);
        log.info("createEvent wrote to revision: {} for scopeName: {}, streamName:{}", r, scopeName, streamName);
        return ack;
    }

    /**
     * Gets an event from the stream.
     *
     * @param routingKey Name of routingKey to be used.
     * @param scopeName Name of scope to be used.
     * @param streamName Name of stream to be used.
     * @param segmentNumber segment of the stream.
     */
    @Override
    public CompletableFuture<String> getEvent(final String  routingKey,
                                              final String scopeName,
                                              final String streamName,
                                              final Long segmentNumber) {
        ClientConfig clientConfig = this.getStoreHelper().getSegmentHelper().getConnectionFactory().getClientConfig();
        SynchronizerClientFactory synchronizerClientFactory = SynchronizerClientFactory.withScope(scopeName, ClientConfig.builder().build());
        RevisionedStreamClient<String> revisedStreamClient = synchronizerClientFactory.createRevisionedStreamClient(
                streamName,
                new JavaSerializer<String>(), SynchronizerConfig.builder().build());
        Revision r = revisedStreamClient.fetchOldestRevision();
        Iterator<Map.Entry<Revision, String>> iter = revisedStreamClient.readFrom(r);
        StringBuffer sb = new StringBuffer();
        while (iter.hasNext()) {
            Map.Entry<Revision, String> entry = iter.next();
            sb.append(entry.getValue());
        }
        CompletableFuture<String> ack = CompletableFuture.completedFuture(sb.toString());
        log.info("getEvent returning data for scopeName: {} streamName: {}", scopeName, streamName);
        return ack;
    }
}
