/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;


import io.pravega.client.ClientFactory;
import io.pravega.client.netty.impl.ConnectionFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class ClientFactoryTest {

    @Mock
    private ConnectionFactory connectionFactory;
    @Mock
    private Controller controllerClient;

    @Test
    public void testCloseWithExternalController() {
        ClientFactory clientFactory = new ClientFactoryImpl("scope", controllerClient);
        clientFactory.close();
        verify(controllerClient, times(1)).close();
    }

    @Test
    public void testCloseWithExternalControllerConnectionFactory() {
        ClientFactory clientFactory = new ClientFactoryImpl("scope", controllerClient, connectionFactory);
        clientFactory.close();
        verify(connectionFactory, times(1)).close();
        verify(controllerClient, times(1)).close();
    }
}
