/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client;

import io.pravega.client.stream.impl.DefaultCredentials;
import io.pravega.client.stream.impl.JavaSerializer;
import org.junit.Test;

import java.net.URI;

import static org.junit.Assert.assertEquals;

public class ClientConfigTest {

    private static final String USERNAME = "username";
    private static final String PASSWORD = "password";

    @Test
    public void isEnableTls() {
    }

    @Test
    public void serializable() {
        JavaSerializer<ClientConfig> s = new JavaSerializer<>();
        ClientConfig expected = ClientConfig.builder()
                .credentials(new DefaultCredentials(PASSWORD, USERNAME))
                .controllerURI(URI.create("tcp://localhost:9090"))
                .trustStore("truststore.jks")
                .validateHostName(false)
                .build();
        ClientConfig actual = s.deserialize(s.serialize(expected));
        assertEquals(expected, actual);
    }

    @Test
    public void testDefault() {
        ClientConfig defaultConfig = ClientConfig.builder().build();
        assertEquals(ClientConfig.DEFAULT_MAX_CONNECTIONS_PER_SEGMENT_STORE, defaultConfig.getMaxConnectionsPerSegmentStore());
        ClientConfig config1 = ClientConfig.builder().maxConnectionsPerSegmentStore(-1).build();
        assertEquals(ClientConfig.DEFAULT_MAX_CONNECTIONS_PER_SEGMENT_STORE, config1.getMaxConnectionsPerSegmentStore());
        ClientConfig config2 = ClientConfig.builder().maxConnectionsPerSegmentStore(1).build();
        assertEquals(1, config2.getMaxConnectionsPerSegmentStore());
    }
}
