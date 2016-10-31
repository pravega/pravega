package com.emc.pravega.integrationtests;

import static org.junit.Assert.*;

import java.time.Duration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.emc.pravega.service.contracts.StreamSegmentStore;
import com.emc.pravega.service.server.host.handler.PravegaConnectionListener;
import com.emc.pravega.service.server.store.ServiceBuilder;
import com.emc.pravega.service.server.store.ServiceBuilderConfig;
import com.emc.pravega.state.examples.SetSynchronizer;
import com.emc.pravega.stream.Stream;
import com.emc.pravega.stream.TxFailedException;
import com.emc.pravega.stream.mock.MockStreamManager;
import com.emc.pravega.testcommon.TestUtils;

import io.netty.util.ResourceLeakDetector;
import io.netty.util.ResourceLeakDetector.Level;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;
import lombok.Cleanup;
import scala.collection.immutable.HashSet;

public class StateSynchronizerTest {
    
    private Level originalLevel;
    private ServiceBuilder serviceBuilder;

    @Before
    public void setup() throws Exception {
        originalLevel = ResourceLeakDetector.getLevel();
        ResourceLeakDetector.setLevel(Level.PARANOID);
        InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());
        this.serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());
        this.serviceBuilder.initialize(Duration.ofMinutes(1)).get();
    }

    @After
    public void teardown() {
        this.serviceBuilder.close();
        ResourceLeakDetector.setLevel(originalLevel);
    }

    @Test
    public void testStateTracker() throws TxFailedException {
        String endpoint = "localhost";
        String stateName = "abc";
        int port = TestUtils.randomPort();
        StreamSegmentStore store = this.serviceBuilder.createStreamSegmentService();
        @Cleanup
        PravegaConnectionListener server = new PravegaConnectionListener(false, port, store);
        server.startListening();
        @Cleanup
        MockStreamManager streamManager = new MockStreamManager("scope", endpoint, port);
        Stream stream = streamManager.createStream(stateName, null);
        SetSynchronizer<String> setA = SetSynchronizer.createNewSet(stream);
        SetSynchronizer<String> setB = SetSynchronizer.createNewSet(stream);
        
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
    
}
