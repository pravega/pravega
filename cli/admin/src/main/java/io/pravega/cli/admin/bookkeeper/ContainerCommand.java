/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.cli.admin.bookkeeper;

import io.pravega.cli.admin.CommandArgs;
import io.pravega.segmentstore.server.containers.ContainerConfig;
import io.pravega.segmentstore.server.store.ServiceConfig;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import lombok.val;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeperAdmin;
import org.apache.curator.framework.CuratorFramework;

/**
 * Base for Container password commands.
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
    public Context createContext() throws DurableDataLogException {
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

        val bkAdmin = new BookKeeperAdmin((BookKeeper) factory.getBookKeeperClient());
        return new Context(serviceConfig, containerConfig, bkConfig, zkClient, factory, bkAdmin);
    }

    protected static class Context extends BookKeeperCommand.Context {
        final ContainerConfig containerConfig;

        Context(ServiceConfig serviceConfig, ContainerConfig containerConfig, BookKeeperConfig bookKeeperConfig,
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
