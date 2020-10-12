/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.cli.user.utils;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;

public class BackgroundConsoleListenerTest {

    @Test
    public void testBackgroundConsoleListener() throws InterruptedException {
        BackgroundConsoleListener listener = new BackgroundConsoleListener();
        Assert.assertFalse(listener.isTriggered());
        listener.start();
        Assert.assertFalse(listener.isTriggered());
        Thread.sleep(1000);
        System.setIn(new ByteArrayInputStream("test input\r\n".getBytes()));
        Thread.sleep(1000);
        System.setIn(new ByteArrayInputStream("quit\r\n".getBytes()));
        listener.stop();
        Assert.assertTrue(listener.isTriggered());
        listener.close();
    }
}
