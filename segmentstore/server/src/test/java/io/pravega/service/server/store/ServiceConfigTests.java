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
package io.pravega.service.server.store;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the ServiceConfig class
 */
public class ServiceConfigTests {
    @Test
    public void testListeningAndPublicIPAndPort() {
        // When the published IP and port are not specified, it should default to listening IP and port
        ServiceConfig cfg1 = ServiceConfig.builder()
                .with(ServiceConfig.CONTAINER_COUNT, 1)
                .with(ServiceConfig.LISTENING_IP_ADDRESS, "myhost")
                .with(ServiceConfig.LISTENING_PORT, 4000)
                .build();
        Assert.assertTrue("Published IP and port should default to listening IP and port",
                cfg1.getListeningIPAddress().equals(cfg1.getPublishedIPAddress())
                        && cfg1.getListeningPort() == cfg1.getPublishedPort());
        // Published IP not defined but port is different as compared to listening port
        ServiceConfig cfg2 = ServiceConfig.builder()
                .with(ServiceConfig.CONTAINER_COUNT, 1)
                .with(ServiceConfig.LISTENING_IP_ADDRESS, "myhost")
                .with(ServiceConfig.PUBLISHED_IP_ADDRESS, "myhost1")
                .with(ServiceConfig.LISTENING_PORT, 4000)
                .build();
        Assert.assertTrue("Published IP should default to listening IP even when ports are different",
                !cfg2.getListeningIPAddress().equals(cfg2.getPublishedIPAddress())
                        && cfg2.getListeningPort() == cfg2.getPublishedPort());
        //Both published IP and port are defined and are different than listening IP and port
        ServiceConfig cfg3 = ServiceConfig.builder()
                .with(ServiceConfig.CONTAINER_COUNT, 1)
                .with(ServiceConfig.LISTENING_IP_ADDRESS, "myhost")
                .with(ServiceConfig.PUBLISHED_IP_ADDRESS, "myhost1")
                .with(ServiceConfig.LISTENING_PORT, 4000)
                .with(ServiceConfig.PUBLISHED_PORT, 5000)
                .build();
        Assert.assertTrue("When specified publishing IP and port should differ from listening IP and port",
                !cfg3.getListeningIPAddress().equals(cfg3.getPublishedIPAddress())
                        && cfg3.getListeningPort() != cfg3.getPublishedPort());
    }

}
