/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util;

import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ResourcePoolTest {
    AtomicInteger counter;
    @Before
    public void setUp() {
        counter = new AtomicInteger(0);
    }

    @Test(timeout = 10000)
    public void resourceTest() throws InterruptedException {
        LinkedBlockingQueue<ResourcePool.Event> eventQueue = new LinkedBlockingQueue<>();
        ResourcePool.Listener myListener = new ResourcePool.Listener(eventQueue);
        MyResourcePool pool = new MyResourcePool(() -> CompletableFuture.completedFuture(new MyResource(counter.incrementAndGet())), t -> {
        }, myListener);

        // we should be able to create two resources easily
        MyResource resource1 = pool.getResource().join();
        assertEquals(resource1.resourceId, 1);
        assertEquals(ResourcePool.Event.Created, eventQueue.take());
        assertEquals(pool.resourceCount(), 1);

        MyResource resource2 = pool.getResource().join();
        assertEquals(resource2.resourceId, 2);
        assertEquals(ResourcePool.Event.Created, eventQueue.take());
        assertEquals(pool.resourceCount(), 2);

        // return these resources
        pool.returnResource(resource1);
        // verify that available resources is 1
        assertEquals(pool.idleCount(), 1);
        assertEquals(pool.resourceCount(), 2);

        pool.returnResource(resource2);
        // pool should only have one resource as available resource. 
        // it should have destroyed the second resource.
        // verify that available resources is still 1
        assertEquals(pool.idleCount(), 1);
        assertEquals(pool.resourceCount(), 1);

        // verify that one resource was closed
        assertEquals(ResourcePool.Event.Destroyed, eventQueue.take());
        assertTrue(eventQueue.isEmpty());

        // now create two more resources
        // 1st should be delivered from available resources. 
        resource1 = pool.getResource().join();
        assertEquals(resource1.resourceId, 1);
        // verify its delivered from available resources 
        assertEquals(pool.resourceCount(), 1);
        assertEquals(pool.idleCount(), 0);
        // verify that no new resource was created
        assertTrue(eventQueue.isEmpty());

        // 2nd request should result in creation of new resource
        resource2 = pool.getResource().join();
        assertEquals(resource2.resourceId, 3);
        assertEquals(ResourcePool.Event.Created, eventQueue.take());
        assertEquals(pool.idleCount(), 0);
        // verify that there are two created resources
        assertEquals(pool.resourceCount(), 2);

        // attempt to create a third resource
        CompletableFuture<MyResource> resource3Future = pool.getResource();
        // this would not have completed. the waiting queue should have this entry
        assertEquals(pool.resourceCount(), 2);
        assertEquals(pool.waitingCount(), 1);
        assertEquals(pool.idleCount(), 0);
        assertFalse(resource3Future.isDone());
        assertTrue(eventQueue.isEmpty());

        CompletableFuture<MyResource> resource4Future = pool.getResource();
        assertEquals(pool.resourceCount(), 2);
        assertEquals(pool.waitingCount(), 2);
        assertEquals(pool.idleCount(), 0);
        assertTrue(eventQueue.isEmpty());

        // return resource1. it should be assigned to first waiting resource (resource3)
        pool.returnResource(resource1);
        MyResource resource3 = resource3Future.join();
        assertEquals(resource3.resourceId, 1);
        assertEquals(pool.resourceCount(), 2);
        assertEquals(pool.waitingCount(), 1);
        assertEquals(pool.idleCount(), 0);
        // verify that resource 3 received a resource object
        assertTrue(resource3Future.isDone());
        assertTrue(eventQueue.isEmpty());

        // now fail resource 2 and return it.
        pool.returnResource(resource2, false);
        // this should not be given to the waiting request. instead a new resource should be createed. 
        assertEquals(ResourcePool.Event.Destroyed, eventQueue.take());

        MyResource resource4 = resource4Future.join();
        assertEquals(resource4.resourceId, 4);
        assertEquals(ResourcePool.Event.Created, eventQueue.take());
        assertEquals(pool.resourceCount(), 2);
        assertEquals(pool.waitingCount(), 0);
        assertEquals(pool.idleCount(), 0);

        // create another waiting request
        CompletableFuture<MyResource> resource5Future = pool.getResource();
        assertEquals(pool.resourceCount(), 2);
        assertEquals(pool.waitingCount(), 1);
        assertEquals(pool.idleCount(), 0);
        assertFalse(resource5Future.isDone());
        assertTrue(eventQueue.isEmpty());

        // test shutdown
        pool.shutdown();
        pool.returnResource(resource3);
        assertEquals(pool.resourceCount(), 2);
        assertEquals(pool.waitingCount(), 0);
        assertEquals(pool.idleCount(), 0);

        // resource 5 should have been returned by using resource3
        MyResource resource5 = resource5Future.join();
        assertEquals(resource5.resourceId, 1);

        // since returned resource served the waiting request no new event should have been generated
        assertTrue(eventQueue.isEmpty());

        // return resource 4
        pool.returnResource(resource4);
        assertEquals(pool.resourceCount(), 1);
        assertEquals(pool.waitingCount(), 0);
        assertEquals(pool.idleCount(), 0);
        // returned resource should be closed
        assertEquals(ResourcePool.Event.Destroyed, eventQueue.take());

        // we should still be able to request new resources.. request resource 6.. this should be served immediately 
        // by way of new resource
        MyResource resource6 = pool.getResource().join();
        assertEquals(resource6.resourceId, 5);
        assertEquals(pool.resourceCount(), 2);
        assertEquals(pool.waitingCount(), 0);
        assertEquals(pool.idleCount(), 0);
        assertEquals(ResourcePool.Event.Created, eventQueue.take());

        // request connect 7. this should wait as resource could is 2. 
        CompletableFuture<MyResource> resource7Future = pool.getResource();
        assertEquals(pool.resourceCount(), 2);
        assertEquals(pool.waitingCount(), 1);
        assertEquals(pool.idleCount(), 0);

        // return resource 5.. resource7 should get resource5's object and no new resource should be createed
        pool.returnResource(resource5);
        MyResource resource7 = resource7Future.join();
        assertEquals(resource7.resourceId, 1);
        assertEquals(pool.resourceCount(), 2);
        assertEquals(pool.waitingCount(), 0);
        assertEquals(pool.idleCount(), 0);
        assertTrue(eventQueue.isEmpty());

        pool.returnResource(resource6);
        assertEquals(pool.resourceCount(), 1);
        // verify that returned resource is not included in available resource.
        assertEquals(pool.waitingCount(), 0);
        assertEquals(pool.idleCount(), 0);
        // also the returned resource is closed
        assertEquals(ResourcePool.Event.Destroyed, eventQueue.take());

        pool.returnResource(resource7);
        assertEquals(pool.resourceCount(), 0);
        assertEquals(pool.waitingCount(), 0);
        assertEquals(pool.idleCount(), 0);
        assertEquals(ResourcePool.Event.Destroyed, eventQueue.take());
    }
    
    // resource instantiation test
    
    private static class MyResourcePool extends ResourcePool<MyResource> {
        MyResourcePool(Supplier<CompletableFuture<MyResource>> tSupplier, Consumer<MyResource> tDestroyer, Listener myListener) {
            super(tSupplier, tDestroyer, 2, 1, myListener);
        }
    }
    
    private static class MyResource {
        private final int resourceId;

        private MyResource(int resourceId) {
            this.resourceId = resourceId;
        }
    }
}
