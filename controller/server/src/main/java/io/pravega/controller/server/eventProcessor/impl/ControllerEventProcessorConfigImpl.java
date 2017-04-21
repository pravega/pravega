/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.controller.server.eventProcessor.impl;

import io.pravega.common.Exceptions;
import io.pravega.shared.NameUtils;
import io.pravega.controller.eventProcessor.CheckpointConfig;
import io.pravega.controller.server.eventProcessor.ControllerEventProcessorConfig;
import io.pravega.stream.ScalingPolicy;
import com.google.common.base.Preconditions;
import lombok.Builder;
import lombok.Getter;

/**
 * Configuration of controller event processors.
 */
@Getter
public class ControllerEventProcessorConfigImpl implements ControllerEventProcessorConfig {

    private final String scopeName;
    private final String commitStreamName;
    private final ScalingPolicy commitStreamScalingPolicy;
    private final String abortStreamName;
    private final ScalingPolicy abortStreamScalingPolicy;

    private final String commitReaderGroupName;
    private final int commitReaderGroupSize;
    private final String abortReaderGrouopName;
    private final int abortReaderGroupSize;

    private final CheckpointConfig commitCheckpointConfig;
    private final CheckpointConfig abortCheckpointConfig;

    @Builder
    ControllerEventProcessorConfigImpl(final String scopeName,
                                       final String commitStreamName,
                                       final ScalingPolicy commitStreamScalingPolicy,
                                       final String abortStreamName,
                                       final ScalingPolicy abortStreamScalingPolicy,
                                       final String commitReaderGroupName,
                                       final int commitReaderGroupSize,
                                       final String abortReaderGrouopName,
                                       final int abortReaderGroupSize,
                                       final CheckpointConfig commitCheckpointConfig,
                                       final CheckpointConfig abortCheckpointConfig) {

        Exceptions.checkNotNullOrEmpty(scopeName, "scopeName");
        Exceptions.checkNotNullOrEmpty(commitStreamName, "commitStreamName");
        Exceptions.checkNotNullOrEmpty(abortStreamName, "abortStreamName");
        Exceptions.checkNotNullOrEmpty(commitReaderGroupName, "commitReaderGroupName");
        Exceptions.checkNotNullOrEmpty(abortReaderGrouopName, "abortReaderGrouopName");
        Preconditions.checkArgument(commitReaderGroupSize > 0, "commitReaderGroupSize should be a positive integer");
        Preconditions.checkArgument(abortReaderGroupSize > 0, "abortReaderGroupSize should be a positive integer");
        Preconditions.checkNotNull(commitStreamScalingPolicy, "commitStreamScalingPolicy");
        Preconditions.checkNotNull(abortStreamScalingPolicy, "abortStreamScalingPolicy");
        Preconditions.checkNotNull(commitCheckpointConfig, "commitCheckpointConfig");
        Preconditions.checkNotNull(abortCheckpointConfig, "abortCheckpointConfig");

        this.scopeName = scopeName;
        this.commitStreamName = commitStreamName;
        this.commitStreamScalingPolicy = commitStreamScalingPolicy;
        this.abortStreamName = abortStreamName;
        this.abortStreamScalingPolicy = abortStreamScalingPolicy;
        this.commitReaderGroupName = commitReaderGroupName;
        this.commitReaderGroupSize = commitReaderGroupSize;
        this.abortReaderGrouopName = abortReaderGrouopName;
        this.abortReaderGroupSize = abortReaderGroupSize;
        this.commitCheckpointConfig = commitCheckpointConfig;
        this.abortCheckpointConfig = abortCheckpointConfig;
    }

    public static ControllerEventProcessorConfig withDefault() {
        return ControllerEventProcessorConfigImpl.builder()
                .scopeName(NameUtils.INTERNAL_SCOPE_NAME)
                .commitStreamName(NameUtils.getInternalNameForStream("commitStream"))
                .abortStreamName(NameUtils.getInternalNameForStream("abortStream"))
                .commitStreamScalingPolicy(ScalingPolicy.fixed(2))
                .abortStreamScalingPolicy(ScalingPolicy.fixed(2))
                .commitReaderGroupName("commitStreamReaders")
                .commitReaderGroupSize(1)
                .abortReaderGrouopName("abortStreamReaders")
                .abortReaderGroupSize(1)
                .commitCheckpointConfig(CheckpointConfig.periodic(10, 10))
                .abortCheckpointConfig(CheckpointConfig.periodic(10, 10))
                .build();
    }
}
