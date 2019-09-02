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

import io.pravega.segmentstore.server.containers.ContainerConfig;
import io.pravega.segmentstore.server.store.ServiceConfig;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import lombok.val;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.curator.framework.CuratorFramework;

/**
 * Base for Container admin commands.
 */
abstract class ContainerCommand extends BookKeeperCommand {
    static final String COMPONENT = "container";

    ContainerCommand(CommandArgs args) {
        super(args);
    }

    /**
     * Creates a new Context to be used by the BookKeeper command.
     *
     * @return A new Context.
     * @throws DurableDataLogException If the BookKeeperLogFactory could not be initialized.
     */
    @Override
    protected Context createContext() throws DurableDataLogException {
        val serviceConfig = getServiceConfig();
        val containerConfig = getCommandArgs().getState().getConfigBuilder().build().getConfig(ContainerConfig::builder);
        val bkConfig = getCommandArgs().getState().getConfigBuilder()
                                       .include(BookKeeperConfig.builder().with(BookKeeperConfig.ZK_ADDRESS, serviceConfig.getZkURL()))
                                       .build().getConfig(BookKeeperConfig::builder);
        val zkClient = createZKClient();
        val factory = new BookKeeperLogFactory(bkConfig, zkClient, getCommandArgs().getState().getExecutor());
        try {
            factory.initialize();
        } catch (DurableDataLogException ex) {
            zkClient.close();
            throw ex;
        }

        val bkAdmin = new BookKeeperAdmin(factory.getBookKeeperClient());
        return new Context(serviceConfig, containerConfig, bkConfig, zkClient, factory, bkAdmin);
    }

    protected static class Context extends BookKeeperCommand.Context {
        final ContainerConfig containerConfig;

        protected Context(ServiceConfig serviceConfig, ContainerConfig containerConfig, BookKeeperConfig bookKeeperConfig,
                          CuratorFramework zkClient, BookKeeperLogFactory logFactory, BookKeeperAdmin bkAdmin) {
            super(serviceConfig, bookKeeperConfig, zkClient, logFactory, bkAdmin);
            this.containerConfig = containerConfig;
        }

        @Override
        public void close() {
            super.close();
        }
    }
}
