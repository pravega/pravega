/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration;

import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.test.integration.utils.SetupUtils;
import lombok.Cleanup;
import org.junit.Assert;
import org.junit.Test;

/**
 * This runs a basic end to end test with a single thread in the thread pool to make sure we don't
 * block anything on it.
 */
public class SingleThreadEndToEndTest {

    @Test(timeout = 30000)
    public void testReadWrite() throws Exception {
        @Cleanup("stopAllServices")
        SetupUtils setupUtils = new SetupUtils();
        setupUtils.startAllServices(1);
        setupUtils.createTestStream("stream", 1);
        @Cleanup
        EventStreamWriter<Integer> writer = setupUtils.getIntegerWriter("stream");
        writer.writeEvent(1);
        writer.flush();
        @Cleanup
        EventStreamReader<Integer> reader = setupUtils.getIntegerReader("stream");

        EventRead<Integer> event = reader.readNextEvent(100);
        Assert.assertEquals(1, (int) event.getEvent());
    }

}
