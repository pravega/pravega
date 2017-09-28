/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.system.framework;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.NotImplementedException;

public class TestExecutorFactory {
    @Getter(lazy = true)
    private static final TestExecutor MARATHON_SEQUENTIAL_EXECUTOR = new RemoteSequential();

    @Getter(lazy = true)
    private static final TestExecutor DOCKER_EXECUTOR = new DockerRemoteSequential();

    public enum TestExecutorType {
        LOCAL,
        DOCKER,
        REMOTE_SEQUENTIAL,
        REMOTE_DISTRIBUTED //TODO: Yet to be implemented.
    }

    public static TestExecutor getTestExecutor(TestExecutorType type) {
        switch (type) {
            case DOCKER:
                return getDOCKER_EXECUTOR();
            case REMOTE_SEQUENTIAL:
                return getMARATHON_SEQUENTIAL_EXECUTOR();
            case REMOTE_DISTRIBUTED:
                throw new NotImplementedException("Distributed execution not implemented");
            case LOCAL:
            default:
                throw new IllegalArgumentException("Invalid Executor specified: " + type);
        }
    }
}
