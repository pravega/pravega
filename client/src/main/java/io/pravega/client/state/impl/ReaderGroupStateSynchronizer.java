/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.state.impl;

import io.pravega.client.SynchronizerClientFactory;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.state.InitialUpdate;
import io.pravega.client.state.StateSynchronizer;
import io.pravega.client.state.SynchronizerConfig;
import io.pravega.client.state.Update;
import io.pravega.client.stream.Serializer;
import io.pravega.client.stream.impl.ReaderGroupState;
import io.pravega.client.stream.impl.SegmentWithRange;
import lombok.val;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static io.pravega.client.stream.impl.ReaderGroupImpl.getEndSegmentsForStreams;
import static io.pravega.client.stream.impl.ReaderGroupImpl.getSegmentsForStreams;
import static io.pravega.common.concurrent.Futures.getThrowingException;
import static io.pravega.shared.NameUtils.getStreamForReaderGroup;

/**
 * A new {@link StateSynchronizer} implementation specifically with the {@link ReaderGroupState} to allow for special
 * updates to the controller.
 */
public class ReaderGroupStateSynchronizer implements StateSynchronizer<ReaderGroupState> {
    private final String scope;
    private final String readerGroup;
    private final StateSynchronizer<ReaderGroupState> synchronizer;
    private final Controller controller;

    /**
     * Creates a new instance of ReaderGroupStateSynchronizer class.
     *
     * @param readerGroup Name of the {@link io.pravega.client.stream.ReaderGroup}.
     * @param synchronizer The {@link StateSynchronizer} instance with {@link ReaderGroupState}.
     * @param controller The {@link Controller} instance.
     */
    public ReaderGroupStateSynchronizer(String scope, String readerGroup, StateSynchronizer<ReaderGroupState> synchronizer, Controller controller) {
        this.scope = scope;
        this.readerGroup = readerGroup;
        this.synchronizer = synchronizer;
        this.controller = controller;
    }

    /**
     * Creates a new instance of ReaderGroupStateSynchronizer class.
     *
     * @param readerGroup Name of the {@link io.pravega.client.stream.ReaderGroup}.
     * @param updateSerializer The serializer for {@link ReaderGroupState} updates.
     * @param initSerializer The serializer for the initial {@link ReaderGroupState} update.
     * @param config The {@link SynchronizerConfig}.
     * @param clientFactory The ClientFactory instance.
     * @param controller The {@link Controller} instance.
     */
    public ReaderGroupStateSynchronizer(String scope,
                                        String readerGroup,
                                        Serializer<InitialUpdate<ReaderGroupState>> initSerializer,
                                        Serializer<Update<ReaderGroupState>> updateSerializer,
                                        SynchronizerConfig config,
                                        SynchronizerClientFactory clientFactory,
                                        Controller controller) {
        this.scope = scope;
        this.readerGroup = readerGroup;
        String streamName = getStreamForReaderGroup(readerGroup);
        this.synchronizer = clientFactory.createStateSynchronizer(streamName, updateSerializer, initSerializer, config);
        this.controller = controller;
    }

    @Override
    public ReaderGroupState getState() {
        return synchronizer.getState();
    }

    /**
     * Method to fetch and apply all updates to the {@link ReaderGroupState} while making calls to the {@link Controller}
     * to make sure the {@link ReaderGroupState} is consistent with its version on the {@link Controller).
     */
    @Override
    public void fetchUpdates() {
        synchronizer.fetchUpdates();
        val state = getState();
        if (state.isUpdatingConfig()) {
            val controllerRGConfig = getThrowingException(controller.getReaderGroup(scope, readerGroup));
            if (state.getConfig().getGeneration() < controllerRGConfig.getGeneration()) {
                Map<SegmentWithRange, Long> segments = getSegmentsForStreams(controller, controllerRGConfig);
                synchronizer.updateStateUnconditionally(new ReaderGroupState.ReaderGroupStateInit(controllerRGConfig,
                        segments, getEndSegmentsForStreams(controllerRGConfig), false));
            }
        }
    }

    @Override
    public void updateState(UpdateGenerator<ReaderGroupState> updateGenerator) {
        synchronizer.updateState(updateGenerator);
    }

    @Override
    public <ReturnT> ReturnT updateState(UpdateGeneratorFunction<ReaderGroupState, ReturnT> updateGenerator) {
        return synchronizer.updateState(updateGenerator);
    }

    @Override
    public void updateStateUnconditionally(Update<ReaderGroupState> update) {
        synchronizer.updateStateUnconditionally(update);
    }

    @Override
    public void updateStateUnconditionally(List<? extends Update<ReaderGroupState>> update) {
        synchronizer.updateStateUnconditionally(update);
    }

    @Override
    public void initialize(InitialUpdate<ReaderGroupState> initial) {
        synchronizer.initialize(initial);
    }

    @Override
    public long bytesWrittenSinceCompaction() {
        return synchronizer.bytesWrittenSinceCompaction();
    }

    @Override
    public void compact(Function<ReaderGroupState, InitialUpdate<ReaderGroupState>> compactor) {
        synchronizer.compact(compactor);
    }

    @Override
    public void close() {
        synchronizer.close();
    }
}
