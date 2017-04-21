/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.framework;

import org.apache.commons.lang.NotImplementedException;

public class TestExecutorFactory {
    private static final TestExecutor MARATHON_SEQUENTIAL_EXECUTOR = new RemoteSequential();

    public enum TestExecutorType {
        LOCAL,
        REMOTE_SEQUENTIAL,
        REMOTE_DISTRIBUTED //TODO: Yet to be implemented.
    }

    public static TestExecutor getTestExecutor(TestExecutorType type) {
        switch (type) {
            case REMOTE_SEQUENTIAL:
                return MARATHON_SEQUENTIAL_EXECUTOR;
            case REMOTE_DISTRIBUTED:
                throw new NotImplementedException("Distributed execution not implemented");
            case LOCAL:
            default:
                throw new IllegalArgumentException("Invalid Executor specified: " + type);
        }
    }
}
