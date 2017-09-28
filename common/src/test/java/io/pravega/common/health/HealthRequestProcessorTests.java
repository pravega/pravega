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

import io.pravega.common.health.processor.impl.HealthRequestProcessorImpl;
import io.pravega.test.common.AssertExtensions;
import java.io.DataOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.Assert;
import org.junit.Test;

public class HealthRequestProcessorTests {
    @Test
    public void testHealthReporter() throws IOException, NoSuchHealthProcessor, NoSuchHealthCommand {
        HealthReporter root = new TestHealthReporterImpl();
        HealthRequestProcessorImpl processor = new HealthRequestProcessorImpl();

        processor.registerHealthProcessor("root", root);
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
        processor.registerHealthProcessor("root/root", root);
        processor.processHealthRequest(writer.get(), "root/root", "hello");
        Assert.assertEquals("A simple command on a child not returned properly", writer.toString(), "hi");
        AssertExtensions.assertThrows("Should throw exception on non-existent command",
                () -> processor.processHealthRequest(writer.get(), "root/root", "wrong"),
                ex -> ex instanceof NoSuchHealthCommand);

        AssertExtensions.assertThrows("Should throw exception on non-existent child processor",
                () -> processor.processHealthRequest(writer.get(), "root/wrong", "wrong"),
                ex -> ex instanceof NoSuchHealthProcessor);
    }

    static class TestHealthReporterImpl extends HealthReporter {
        @Override
            public Map<String, Consumer<DataOutputStream>> createHandlers() {
                Map<String, Consumer<DataOutputStream>> handlersMap = new HashMap<>();
                handlersMap.put("ruok", this::handleRuok);
                handlersMap.put("hi", this::hiHandler);
                handlersMap.put("hello", this::helloHandler);
                return handlersMap;
            }

        private void helloHandler(DataOutputStream dataOutputStream) {
            try {
                dataOutputStream.writeBytes("hi");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private void hiHandler(DataOutputStream dataOutputStream) {
            try {
                dataOutputStream.writeBytes("hello");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
