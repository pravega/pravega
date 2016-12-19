/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.emc.pravega.integrationtests;

import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.server.host.handler.PravegaConnectionListener;
import com.emc.pravega.service.server.store.ServiceBuilder;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.state.examples.SetSynchronizer;
import com.emc.pravega.stream.TxFailedException;
import com.emc.pravega.stream.mock.MockClientFactory;
import com.emc.pravega.testcommon.Async;
import com.emc.pravega.testcommon.TestUtils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetector.Level;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import lombok.Cleanup;

public class StateSynchronizerTest {

    private Level originalLevel;
    private ServiceBuilder serviceBuilder;

    @Before
    public void setup() throws Exception {
        originalLevel = ResourceLeakDetector.getLevel();
        ResourceLeakDetector.setLevel(Level.PARANOID);
        InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());
        this.serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        this.serviceBuilder.initialize().get();
    }

    @After
    public void teardown() {
        this.serviceBuilder.close();
        ResourceLeakDetector.setLevel(originalLevel);
    }

    @Test(timeout = 30000)
    public void testStateTracker() throws TxFailedException {
        String endpoint = "localhost";
        String stateName = "abc";
        int port = TestUtils.randomPort();
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store);
        server.startListening();

        MockClientFactory clientFactory = new MockClientFactory("scope", endpoint, port);
        clientFactory.createStream(stateName, null);
        SetSynchronizer<String> setA = SetSynchronizer.createNewSet(stateName, clientFactory);
        SetSynchronizer<String> setB = SetSynchronizer.createNewSet(stateName, clientFactory);

        assertTrue(setA.attemptAdd("1"));
        assertFalse(setB.attemptAdd("Fail"));
        assertTrue(setA.attemptAdd("2"));
        setB.update();
        assertEquals(2, setB.getCurrentSize());
        assertTrue(setB.getCurrentValues().contains("1"));
        assertTrue(setB.getCurrentValues().contains("2"));
        assertTrue(setB.attemptRemove("1"));
        assertFalse(setA.attemptClear());
        setA.update();
        assertEquals(1, setA.getCurrentSize());
        assertTrue(setA.getCurrentValues().contains("2"));
        assertTrue(setA.attemptClear());
        assertEquals(0, setA.getCurrentValues().size());
    }

    @Test(timeout = 30000)
    public void testReadsAllAvailable() {
        String endpoint = "localhost";
        String stateName = "abc";
        int port = TestUtils.randomPort();
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store);
        server.startListening();

        MockClientFactory clientFactory = new MockClientFactory("scope", endpoint, port);
        clientFactory.createStream(stateName, null);
        SetSynchronizer<String> setA = SetSynchronizer.createNewSet(stateName, clientFactory);

        for (int i = 0; i < 10; i++) {
            assertTrue(setA.attemptAdd("Append: " + i));
        }
        SetSynchronizer<String> setB = SetSynchronizer.createNewSet(stateName, clientFactory);
        assertEquals(10, setB.getCurrentSize());
        for (int i = 10; i < 20; i++) {
            assertTrue(setA.attemptAdd("Append: " + i));
        }
        setB.update();
        assertEquals(20, setB.getCurrentSize());
    }

    @Test
    public void testBlocking() {
        String endpoint = "localhost";
        String stateName = "abc";
        int port = TestUtils.randomPort();
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store);
        server.startListening();

        MockClientFactory clientFactory = new MockClientFactory("scope", endpoint, port);
        clientFactory.createStream(stateName, null);
        SetSynchronizer<String> setA = SetSynchronizer.createNewSet(stateName, clientFactory);
        SetSynchronizer<String> setB = SetSynchronizer.createNewSet(stateName, clientFactory);

        assertTrue(setA.attemptAdd("foo"));
        setB.update();
        assertEquals(1, setB.getCurrentSize());
        assertTrue(setB.getCurrentValues().contains("foo"));
        Async.testBlocking(() -> setB.update(), () -> setA.attemptAdd("bar"));
        assertEquals(2, setB.getCurrentSize());
        assertTrue(setB.getCurrentValues().contains("bar"));
    }
    
}
