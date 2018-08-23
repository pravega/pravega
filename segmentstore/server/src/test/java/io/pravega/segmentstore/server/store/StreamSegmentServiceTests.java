/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.store;

import io.pravega.common.io.StreamHelpers;
import io.pravega.segmentstore.contracts.AttributeUpdate;
import io.pravega.segmentstore.contracts.AttributeUpdateType;
import io.pravega.segmentstore.storage.mocks.InMemoryDurableDataLogFactory;
import io.pravega.segmentstore.storage.mocks.InMemoryStorageFactory;
import io.pravega.test.common.AssertExtensions;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Cleanup;
import lombok.val;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for the StreamSegmentService class.
 */
public class StreamSegmentServiceTests extends StreamSegmentStoreTestBase {
    private InMemoryStorageFactory storageFactory;
    private InMemoryDurableDataLogFactory durableDataLogFactory;

    @Before
    public void setUp() {
        this.storageFactory = new InMemoryStorageFactory(executorService());
        this.durableDataLogFactory = new PermanentDurableDataLogFactory(executorService());
    }

    @After
    public void tearDown() {
        if (this.durableDataLogFactory != null) {
            this.durableDataLogFactory.close();
            this.durableDataLogFactory = null;
        }

        if (this.storageFactory != null) {
            this.storageFactory.close();
            this.storageFactory = null;
        }
    }

    @Override
    protected ServiceBuilder createBuilder(ServiceBuilderConfig.Builder builderConfig, int instanceId) {
        return ServiceBuilder.newInMemoryBuilder(builderConfig.build())
                             .withStorageFactory(setup -> this.storageFactory)
                             .withDataLogFactory(setup -> this.durableDataLogFactory);
    }

    /**
     * Tests the forSegment() method. We test this here vs in StreamSegmentContainerTests because we want to exercise
     * additional code in StreamSegmentService. This will invoke the StreamSegmentContainer code as well.
     * <p>
     * There is no reason to put this test in StreamSegmentStoreTestBase as we only really need to test it once, and not
     * with every possible Storage Implementation.
     */
    @Test
    public void testForSegment() throws Exception {
        UUID attributeId1 = UUID.randomUUID();
        UUID attributeId2 = UUID.randomUUID();
        @Cleanup
        val builder = createBuilder(this.configBuilder, 0);
        builder.initialize();
        val segmentStore = builder.createStreamSegmentService();

        // Create the StreamSegments.
        val segmentNames = createSegments(segmentStore);

        // Add some appends.
        for (String segmentName : segmentNames) {
            byte[] appendData = ("Append_" + segmentName).getBytes();

            val dsa = segmentStore.forSegment(segmentName, TIMEOUT).join();
            dsa.append(appendData, Collections.singleton(new AttributeUpdate(attributeId1, AttributeUpdateType.None, 1L)), TIMEOUT).join();
            dsa.updateAttributes(Collections.singleton(new AttributeUpdate(attributeId2, AttributeUpdateType.None, 2L)), TIMEOUT).join();
            dsa.seal(TIMEOUT).join();
            dsa.truncate(1, TIMEOUT).join();

            // Check metadata.
            val info = dsa.getInfo();
            Assert.assertEquals("Unexpected name.", segmentName, info.getName());
            Assert.assertEquals("Unexpected length.", appendData.length, info.getLength());
            Assert.assertEquals("Unexpected startOffset.", 1, info.getStartOffset());
            Assert.assertEquals("Unexpected attribute count.", 2, info.getAttributes().size());
            Assert.assertEquals("Unexpected attribute 1.", 1L, (long) info.getAttributes().get(attributeId1));
            Assert.assertEquals("Unexpected attribute 2.", 2L, (long) info.getAttributes().get(attributeId2));
            Assert.assertTrue("Unexpected isSealed.", info.isSealed());

            // Check written data.
            byte[] readBuffer = new byte[appendData.length - 1];
            @Cleanup
            val readResult = dsa.read(1, readBuffer.length, TIMEOUT);
            val firstEntry = readResult.next();
            firstEntry.requestContent(TIMEOUT);
            val entryContents = firstEntry.getContent().join();
            Assert.assertEquals("Unexpected number of bytes read.", readBuffer.length, entryContents.getLength());
            StreamHelpers.readAll(entryContents.getData(), readBuffer, 0, readBuffer.length);
            AssertExtensions.assertArrayEquals("Unexpected data read back.", appendData, 1, readBuffer, 0, readBuffer.length);
        }
    }

    private static class PermanentDurableDataLogFactory extends InMemoryDurableDataLogFactory {
        PermanentDurableDataLogFactory(ScheduledExecutorService executorService) {
            super(executorService);
        }

        @Override
        public void close() {
            // This method intentionally left blank; we want this factory to live between multiple recovery attempts.
        }
    }
}
