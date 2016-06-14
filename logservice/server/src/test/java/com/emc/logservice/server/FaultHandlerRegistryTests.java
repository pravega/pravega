package com.emc.logservice.server;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Unit tests for FaultHandlerRegistry.
 */
public class FaultHandlerRegistryTests {
    /**
     * Tests the register() and handle() methods.
     */
    @Test
    public void testHandle() {
        FaultHandlerRegistry r = new FaultHandlerRegistry();
        Exception toHandle = new Exception("intentional");

        // Nothing should happen now.
        r.handle(toHandle);

        AtomicReference<Throwable> ex1 = new AtomicReference<>();
        AtomicReference<Throwable> ex2 = new AtomicReference<>();
        r.register(ex1::set);
        r.register(ex2::set);

        Assert.assertNull("Handler 1 was invoked before any call to handle().", ex1.get());
        Assert.assertNull("Handler 2 was invoked before any call to handle().", ex2.get());

        r.handle(toHandle);
        Assert.assertNotNull("Handler 1 was not invoked after the call to handle().", ex1.get());
        Assert.assertNotNull("Handler 2 was not invoked after the call to handle().", ex2.get());
        Assert.assertEquals("Unexpected exception passed to handler 1.", toHandle, ex1.get());
        Assert.assertEquals("Unexpected exception passed to handler 2.", toHandle, ex2.get());
    }
}
