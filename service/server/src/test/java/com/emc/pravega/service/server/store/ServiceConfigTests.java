/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package com.emc.pravega.service.server.store;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for the ServiceConfig class
 */
public class ServiceConfigTests {
    @Test
    public void testListeningAndPublicIPAndPort() {
        // When the published IP and port is not specified, it should default to listening IP and port
        ServiceConfig cfg1 = ServiceConfig.builder()
                .with(ServiceConfig.LISTENING_IP_ADDRESS, "myhost")
                .with(ServiceConfig.LISTENING_PORT, 4000)
                .build();
        Assert.assertTrue("Published IP and port should default to listerning IP and port",
                cfg1.getListeningIPAddress().equals(cfg1.getListeningIPAddress())
                        && cfg1.getListeningPort() == cfg1.getPublishedPort());
        // Published IP not defined but port is different as compared to listening port
        ServiceConfig cfg2 = ServiceConfig.builder()
                .with(ServiceConfig.LISTENING_IP_ADDRESS, "myhost")
                .with(ServiceConfig.PUBLISHED_IP_ADDRESS, "myhost1")
                .with(ServiceConfig.LISTENING_PORT, 4000)
                .build();
        Assert.assertTrue("Published IP and port should default to listerning IP and port",
                !cfg2.getListeningIPAddress().equals(cfg2.getListeningIPAddress())
                        && cfg2.getListeningPort() == cfg2.getPublishedPort());
        //Both published IP and port are defined and are different than listening IP and port
        ServiceConfig cfg3 = ServiceConfig.builder()
                .with(ServiceConfig.LISTENING_IP_ADDRESS, "myhost")
                .with(ServiceConfig.PUBLISHED_IP_ADDRESS, "myhost1")
                .with(ServiceConfig.LISTENING_PORT, 4000)
                .with(ServiceConfig.PUBLISHED_PORT, 5000)
                .build();
        Assert.assertTrue("Published IP and port should default to listerning IP and port",
                !cfg2.getListeningIPAddress().equals(cfg2.getListeningIPAddress())
                        && cfg2.getListeningPort() != cfg2.getPublishedPort());
    }

}
