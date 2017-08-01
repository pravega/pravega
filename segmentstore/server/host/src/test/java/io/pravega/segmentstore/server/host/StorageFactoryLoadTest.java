/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host;

import io.pravega.common.util.ServiceBuilderConfig;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.test.common.AssertExtensions;
import io.pravega.test.common.InlineExecutor;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.Assert;
import org.junit.Test;


public class StorageFactoryLoadTest {
    @Test
    public void dynamicClassTest() throws IOException, InvocationTargetException, ClassNotFoundException, InstantiationException, IllegalAccessException {
        ServiceBuilderConfig config = ServiceBuilderConfig.builder().build();
        ScheduledExecutorService executor = new InlineExecutor();

        AssertExtensions.assertThrows("Storage creator should throw exception when a non-existent implementation is registered",
                () -> StorageFactoryCreator.createStorageFactoryFromClassName(config, "tp", null),
                (e) -> e instanceof ClassNotFoundException);

        /* Test whether a generic Storage Factory can be Created*/
        StorageFactory factory = StorageFactoryCreator.createStorageFactoryFromClassName(config,
                "io.pravega.segmentstore.server.host.ServiceStarterTest$DummyFactory", executor);

        Assert.assertNotEquals("Storage factory creator should return a valid factory object", factory, null );

        /* Test whether a HDFS Storage Factory can be Created*/
        factory = StorageFactoryCreator.createStorageFactoryFromClassName(config,
                "io.pravega.segmentstore.storage.impl.hdfs.HDFSStorageFactory", executor);

        Assert.assertNotEquals("Storage factory creator should return a valid factory object", factory, null );
    }

    static final class DummyFactory implements StorageFactory {

       public DummyFactory(ServiceBuilderConfig config, ScheduledExecutorService executor) {

        }

        @Override
        public Storage createStorageAdapter() {
            return null;
        }
    }
}
