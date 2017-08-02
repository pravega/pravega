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

import io.pravega.segmentstore.config.ServiceBuilderConfig;
import io.pravega.segmentstore.storage.StorageFactory;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Utility class to create a StorageFactory from given configuronion.
 */
public final class StorageFactoryCreator {
    public static StorageFactory createStorageFactoryFromClassName(ServiceBuilderConfig config, String storageImplementation, ScheduledExecutorService executor)
            throws IllegalAccessException, InvocationTargetException, InstantiationException, ClassNotFoundException {
        Class<? extends StorageFactory> cls = (Class<? extends StorageFactory>) Class.forName(storageImplementation);
        for (Constructor cstr : cls.getConstructors()) {
            if (cstr.getParameterTypes().length == 2 &&
                    cstr.getParameterTypes()[0].isAssignableFrom(ServiceBuilderConfig.class)) {
                return (StorageFactory) cstr.newInstance(config, executor);
            }
        }
        return null;
    }
}

