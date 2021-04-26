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
package io.pravega.cli.admin.dataRecovery;

import io.pravega.cli.admin.AdminCommand;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.segmentstore.server.host.StorageLoader;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.storage.StorageFactory;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ScheduledExecutorService;

/**
 * Base class for data recovery related commands' classes.
 */
@Slf4j
public abstract class DataRecoveryCommand extends AdminCommand {
    protected final static String COMPONENT = "storage";

    /**
     * Creates a new instance of the DataRecoveryCommand class.
     *
     * @param args The arguments for the command.
     */
    DataRecoveryCommand(CommandArgs args) {
        super(args);
    }

    /**
     * Creates the {@link StorageFactory} instance by reading the config values.
     *
     * @param executorService   A thread pool for execution.
     * @return                  A newly created {@link StorageFactory} instance.
     */
    StorageFactory createStorageFactory(ScheduledExecutorService executorService) {
        ServiceBuilder.ConfigSetupHelper configSetupHelper = new ServiceBuilder.ConfigSetupHelper(getCommandArgs().getState().getConfigBuilder().build());
        StorageLoader loader = new StorageLoader();
        return loader.load(configSetupHelper, getServiceConfig().getStorageImplementation().toString(),
                getServiceConfig().getStorageLayout(), executorService);
    }

    /**
     * Outputs the message to the console as well as to the log file as Info.
     *
     * @param messageTemplate   The message.
     * @param args              The arguments with the message.
     */
    protected void outputInfo(String messageTemplate, Object... args) {
        System.out.println(String.format(messageTemplate, args));
        log.info(String.format(messageTemplate, args));
    }

    /**
     * Outputs the message to the console as well as to the log file as error.
     *
     * @param messageTemplate   The message.
     * @param args              The arguments with the message.
     */
    protected void outputError(String messageTemplate, Object... args) {
        System.err.println(String.format(messageTemplate, args));
        log.error(String.format(messageTemplate, args));
    }
}
