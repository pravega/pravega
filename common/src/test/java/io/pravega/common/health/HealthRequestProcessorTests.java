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
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assert;
import org.junit.Test;

public class HealthRequestProcessorTests {
    @Test
    public void testHealthReporter() throws IOException, NoSuchHealthProcessor, NoSuchHealthCommand {
        HealthReporterImpl root = new TestHealthReporterImpl("root", new String[]{"hi", "hello"});
        HealthRequestProcessorImpl processor = new HealthRequestProcessorImpl(root);

        AtomicReference<ByteArrayOutputStream> writer = new AtomicReference<>(new ByteArrayOutputStream());
        processor.processHealthRequest(writer.get(), "root", "hi");
        Assert.assertEquals("A simple command not returned properly", writer.toString(), "hello");

        writer.set(new ByteArrayOutputStream());
        processor.processHealthRequest(writer.get(), "root", "hello");
        Assert.assertEquals("A simple command not returned properly", writer.toString(), "hi");
        AssertExtensions.assertThrows("Should throw exception on non-existent command",
                () -> processor.processHealthRequest(writer.get(), "root", "wrong"),
                ex -> ex instanceof NoSuchHealthCommand);

        AssertExtensions.assertThrows("Should throw exception on non-existent processor",
                () -> processor.processHealthRequest(writer.get(), "wrong", "wrong"),
                ex -> ex instanceof NoSuchHealthProcessor);

        writer.set(new ByteArrayOutputStream());
        processor.processHealthRequest(writer.get(), "root/root", "hello");
        Assert.assertEquals("A simple command on a child not returned properly", writer.toString(), "hi");
        AssertExtensions.assertThrows("Should throw exception on non-existent command",
                () -> processor.processHealthRequest(writer.get(), "root/root", "wrong"),
                ex -> ex instanceof NoSuchHealthCommand);

        AssertExtensions.assertThrows("Should throw exception on non-existent child processor",
                () -> processor.processHealthRequest(writer.get(), "root/wrong", "wrong"),
                ex -> ex instanceof NoSuchHealthProcessor);
    }

    static class TestHealthReporterImpl extends HealthReporterImpl {

        public TestHealthReporterImpl(String id, String[] commands) {
            super(id, commands);
            this.addChild("root", this);
        }

        @Override
        public void execute(String cmd, DataOutputStream out) {
            try {
            if (cmd.equals("hi")) {
                    out.writeBytes("hello");
            } else if (cmd.equals("hello")) {
                out.writeBytes("hi");
            }
            } catch (IOException e) {
                throw new HealthReporterException(e);
            }
        }
    }
}
