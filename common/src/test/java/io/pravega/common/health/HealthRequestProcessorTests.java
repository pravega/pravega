/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.health;

import io.pravega.common.health.processor.impl.HealthReporterImpl;
import io.pravega.common.health.processor.impl.HealthRequestProcessorImpl;
import io.pravega.test.common.AssertExtensions;
import java.io.DataOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;

public class HealthRequestProcessorTests {
    @Test
    public void testHealthReporter() throws IOException, NoSuchHealthProcessor, NoSuchHealthCommand {
        HealthReporterImpl root = new TestHealthReporterImpl("root", new String[]{"hi"});
        HealthRequestProcessorImpl processor = new HealthRequestProcessorImpl(root);
        ByteArrayOutputStream writer = new ByteArrayOutputStream();
        processor.processHealthRequest(writer, "root", "hi");
        Assert.assertEquals("A simple command not returned properly", writer.toString(), "hello");

        AssertExtensions.assertThrows("Should throw exception on non-existent command",
                () -> processor.processHealthRequest(writer, "root", "wrong"),
                ex -> ex instanceof NoSuchHealthCommand);

        AssertExtensions.assertThrows("Should throw exception on non-existent processor",
                () -> processor.processHealthRequest(writer, "wrong", "wrong"),
                ex -> ex instanceof NoSuchHealthProcessor);
    }

    class TestHealthReporterImpl extends HealthReporterImpl {

        public TestHealthReporterImpl(String id, String[] commands) {
            super(id, commands);
        }

        @Override
        public void execute(String cmd, DataOutputStream out) throws IOException {
            out.writeBytes("hello");
        }
    }
}
