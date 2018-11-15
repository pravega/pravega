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
import org.apache.commons.lang3.NotImplementedException;

import static io.pravega.test.system.framework.Utils.getConfig;

public class TestExecutorFactory {
    @Getter(lazy = true)
    private final TestExecutor marathonSequentialExecutor = new RemoteSequential();

    @Getter(lazy = true)
    private final TestExecutor dockerExecutor = new DockerBasedTestExecutor();

    public enum TestExecutorType {
        LOCAL,
        DOCKER,
        KUBERNETES,
        REMOTE_SEQUENTIAL,
        REMOTE_DISTRIBUTED //TODO: https://github.com/pravega/pravega/issues/2074.
    }

    public TestExecutor getTestExecutor(TestExecutorType type) {
        switch (type) {
            case DOCKER:
                return getDockerExecutor();
            case REMOTE_SEQUENTIAL:
                return getMarathonSequentialExecutor();
            case KUBERNETES:
                //TODO: shrids.
            case REMOTE_DISTRIBUTED:
            case LOCAL:
                throw new NotImplementedException("Distributed execution not implemented");
            default:
                throw new IllegalArgumentException("Invalid Executor specified: " + type);
        }
    }

    public static TestExecutorType getTestExecutionType() {
        return TestExecutorType.valueOf(getConfig("execType", "LOCAL"));
    }
}
