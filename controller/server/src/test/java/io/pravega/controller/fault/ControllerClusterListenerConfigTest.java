/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.pravega.controller.fault;

import io.pravega.controller.fault.impl.ControllerClusterListenerConfigImpl;
import io.pravega.test.common.AssertExtensions;
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
