/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.eventProcessor.impl;

import io.pravega.common.Exceptions;
import io.pravega.controller.util.Config;
import io.pravega.shared.NameUtils;
import io.pravega.controller.eventProcessor.CheckpointConfig;
import io.pravega.controller.server.eventProcessor.ControllerEventProcessorConfig;
import io.pravega.client.stream.ScalingPolicy;
import com.google.common.base.Preconditions;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;

import java.time.Duration;

/**
 * Configuration of controller event processors.
 */
@ToString
@Getter
public class ControllerEventProcessorConfigImpl implements ControllerEventProcessorConfig {

    private final String scopeName;
    private final String commitStreamName;
    private final ScalingPolicy commitStreamScalingPolicy;
    private final String abortStreamName;
    private final ScalingPolicy abortStreamScalingPolicy;
    private final String scaleStreamName;
    private final ScalingPolicy scaleStreamScalingPolicy;

    private final String commitReaderGroupName;
    private final int commitReaderGroupSize;
    private final String abortReaderGroupName;
    private final int abortReaderGroupSize;
    private final String scaleReaderGroupName;
    private final int scaleReaderGroupSize;

    private final CheckpointConfig commitCheckpointConfig;
    private final CheckpointConfig abortCheckpointConfig;
    private final CheckpointConfig scaleCheckpointConfig;
    
    private final long rebalanceIntervalMillis;

    @Builder
    ControllerEventProcessorConfigImpl(final String scopeName,
                                       final String commitStreamName,
                                       final ScalingPolicy commitStreamScalingPolicy,
                                       final String abortStreamName,
                                       final ScalingPolicy abortStreamScalingPolicy,
                                       final String commitReaderGroupName,
                                       final int commitReaderGroupSize,
                                       final String abortReaderGroupName,
                                       final int abortReaderGroupSize,
                                       final CheckpointConfig commitCheckpointConfig,
                                       final CheckpointConfig abortCheckpointConfig,
                                       final ScalingPolicy scaleStreamScalingPolicy,
                                       final long rebalanceIntervalMillis) {

        Exceptions.checkNotNullOrEmpty(scopeName, "scopeName");
        Exceptions.checkNotNullOrEmpty(commitStreamName, "commitStreamName");
        Exceptions.checkNotNullOrEmpty(abortStreamName, "abortStreamName");
        Exceptions.checkNotNullOrEmpty(commitReaderGroupName, "commitReaderGroupName");
        Exceptions.checkNotNullOrEmpty(abortReaderGroupName, "abortReaderGroupName");
        Preconditions.checkArgument(commitReaderGroupSize > 0, "commitReaderGroupSize should be a positive integer");
        Preconditions.checkArgument(abortReaderGroupSize > 0, "abortReaderGroupSize should be a positive integer");
        Preconditions.checkNotNull(commitStreamScalingPolicy, "commitStreamScalingPolicy");
        Preconditions.checkNotNull(abortStreamScalingPolicy, "abortStreamScalingPolicy");
        Preconditions.checkNotNull(scaleStreamScalingPolicy, "scaleStreamScalingPolicy");
        Preconditions.checkNotNull(commitCheckpointConfig, "commitCheckpointConfig");
        Preconditions.checkNotNull(abortCheckpointConfig, "abortCheckpointConfig");

        this.scopeName = scopeName;
        this.commitStreamName = commitStreamName;
        this.commitStreamScalingPolicy = commitStreamScalingPolicy;
        this.abortStreamName = abortStreamName;
        this.abortStreamScalingPolicy = abortStreamScalingPolicy;
        this.commitReaderGroupName = commitReaderGroupName;
        this.commitReaderGroupSize = commitReaderGroupSize;
        this.abortReaderGroupName = abortReaderGroupName;
        this.abortReaderGroupSize = abortReaderGroupSize;
        this.commitCheckpointConfig = commitCheckpointConfig;
        this.abortCheckpointConfig = abortCheckpointConfig;
        this.scaleStreamName = Config.SCALE_STREAM_NAME;
        this.scaleStreamScalingPolicy = scaleStreamScalingPolicy;
        this.scaleReaderGroupName = Config.SCALE_READER_GROUP;
        this.scaleReaderGroupSize = 1;
        this.scaleCheckpointConfig = CheckpointConfig.none();
        this.rebalanceIntervalMillis = rebalanceIntervalMillis;
    }

    public static ControllerEventProcessorConfig withDefault() {
        return ControllerEventProcessorConfigImpl.builder()
                .scopeName(NameUtils.INTERNAL_SCOPE_NAME)
                .commitStreamName(NameUtils.getInternalNameForStream("commitStream"))
                .abortStreamName(NameUtils.getInternalNameForStream("abortStream"))
                .commitStreamScalingPolicy(ScalingPolicy.fixed(2))
                .abortStreamScalingPolicy(ScalingPolicy.fixed(2))
                .scaleStreamScalingPolicy(ScalingPolicy.fixed(2))
                .commitReaderGroupName("commitStreamReaders")
                .commitReaderGroupSize(1)
                .abortReaderGroupName("abortStreamReaders")
                .abortReaderGroupSize(1)
                .commitCheckpointConfig(CheckpointConfig.periodic(10, 10))
                .abortCheckpointConfig(CheckpointConfig.periodic(10, 10))
                .rebalanceIntervalMillis(Duration.ofMinutes(2).toMillis())
                .build();
    }

    @Override
    public String getRequestStreamName() {
        return scaleStreamName;
    }

    @Override
    public String getRequestReaderGroupName() {
        return scaleReaderGroupName;
    }

    @Override
    public ScalingPolicy getRequestStreamScalingPolicy() {
        return scaleStreamScalingPolicy;
    }

    @Override
    public CheckpointConfig getRequestStreamCheckpointConfig() {
        return scaleCheckpointConfig;
    }
}
