/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host.admin.commands;

import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.ScheduledExecutorService;
import lombok.Getter;

/**
 * Keeps state between commands.
 */
public class AdminCommandState implements AutoCloseable {
    @Getter
    private final ServiceBuilderConfig.Builder configBuilder;
    @Getter
    private final ScheduledExecutorService executor = ExecutorServiceHelpers.newScheduledThreadPool(3, "admin-tools");

    /**
     * Creates a new instance of the AdminCommandState class.
     *
     * @throws IOException If unable to read specified config properties file (assuming it exists).
     */
    public AdminCommandState() throws IOException {
        this.configBuilder = ServiceBuilderConfig.builder();
        try {
            this.configBuilder.include(System.getProperty(ServiceBuilderConfig.CONFIG_FILE_PROPERTY_NAME, "config.properties"));
        } catch (FileNotFoundException ex) {
            // Nothing to do here.
        }
    }

    @Override
    public void close() {
        ExecutorServiceHelpers.shutdown(this.executor);
    }
}
