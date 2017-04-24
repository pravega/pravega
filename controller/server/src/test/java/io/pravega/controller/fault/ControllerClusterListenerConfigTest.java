/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.controller.fault;

import io.pravega.controller.fault.impl.ControllerClusterListenerConfigImpl;
import io.pravega.testcommon.AssertExtensions;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * ControllerClusterListenerConfig tests.
 */
public class ControllerClusterListenerConfigTest {

    @Test(timeout = 5000)
    public void invalidConfigTests() {
        AssertExtensions.assertThrows("Negative minThreads",
                () -> ControllerClusterListenerConfigImpl.builder()
                .minThreads(-1).maxThreads(1).idleTime(10).idleTimeUnit(TimeUnit.SECONDS).maxQueueSize(10).build(),
                e -> e.getClass() == IllegalArgumentException.class);

        AssertExtensions.assertThrows("Invalid minThreads",
                () -> ControllerClusterListenerConfigImpl.builder()
                        .minThreads(1).maxThreads(0).idleTime(10).idleTimeUnit(TimeUnit.SECONDS).maxQueueSize(10).build(),
                e -> e.getClass() == IllegalArgumentException.class);

        AssertExtensions.assertThrows("Invalid idleTime",
                () -> ControllerClusterListenerConfigImpl.builder()
                        .minThreads(1).maxThreads(1).idleTime(-1).idleTimeUnit(TimeUnit.SECONDS).maxQueueSize(10).build(),
                e -> e.getClass() == IllegalArgumentException.class);

        AssertExtensions.assertThrows("Invalid idleTimeUnit",
                () -> ControllerClusterListenerConfigImpl.builder()
                        .minThreads(1).maxThreads(1).idleTime(10).idleTimeUnit(null).maxQueueSize(10).build(),
                e -> e.getClass() == NullPointerException.class);

        AssertExtensions.assertThrows("Non-positive maxQueueSize",
                () -> ControllerClusterListenerConfigImpl.builder()
                        .minThreads(1).maxThreads(1).idleTime(10).idleTimeUnit(TimeUnit.SECONDS).maxQueueSize(0).build(),
                e -> e.getClass() == IllegalArgumentException.class);

        AssertExtensions.assertThrows("Too large maxQueueSize",
                () -> ControllerClusterListenerConfigImpl.builder()
                        .minThreads(1).maxThreads(1).idleTime(10).idleTimeUnit(TimeUnit.SECONDS).maxQueueSize(2048).build(),
                e -> e.getClass() == IllegalArgumentException.class);

        ControllerClusterListenerConfigImpl.builder()
                        .minThreads(1).maxThreads(1).idleTime(10).idleTimeUnit(TimeUnit.SECONDS).maxQueueSize(8).build();
    }
}
