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

import io.pravega.cli.admin.CommandArgs;
import io.pravega.common.concurrent.Services;
import io.pravega.segmentstore.server.OperationLogFactory;
import io.pravega.segmentstore.server.containers.ContainerRecoveryUtils;
import io.pravega.segmentstore.server.containers.DebugStreamSegmentContainer;
import io.pravega.segmentstore.server.logs.DurableLogFactory;
import io.pravega.segmentstore.storage.Storage;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import lombok.Cleanup;
import lombok.val;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Loads the storage instance, recovers all segments from there.
 */
public class DurableLogRecoveryCommand extends DataRecoveryCommand {
    private static final int CONTAINER_EPOCH = 1;
    private static final Duration TIMEOUT = Duration.ofMillis(100 * 1000);

    private final int containerCount;

    /**
     * Creates an instance of DurableLogRecoveryCommand class.
     *
     * @param args The arguments for the command.
     */
    public DurableLogRecoveryCommand(CommandArgs args) {
        super(args);
        this.containerCount = getServiceConfig().getContainerCount();
    }

    @Override
    public void execute() throws Exception {
        @Cleanup
        Storage storage = this.storageFactory.createStorageAdapter();
        @Cleanup
        val zkClient = createZKClient();

        val bkConfig = getCommandArgs().getState().getConfigBuilder()
                .include(BookKeeperConfig.builder().with(BookKeeperConfig.ZK_ADDRESS, getServiceConfig().getZkURL()))
                .build().getConfig(BookKeeperConfig::builder);
        @Cleanup
        val dataLogFactory = new BookKeeperLogFactory(bkConfig, zkClient, executorService);

        outputInfo("Container Count = %d", this.containerCount);

        dataLogFactory.initialize();
        outputInfo("Started ZK Client at %s.", getServiceConfig().getZkURL());

        storage.initialize(CONTAINER_EPOCH);
        outputInfo("Loaded %s Storage.", getServiceConfig().getStorageImplementation());

        outputInfo("Starting recovery...");
        // create back up of metadata segments
        Map<Integer, String> backUpMetadataSegments = ContainerRecoveryUtils.createBackUpMetadataSegments(storage,
                this.containerCount, executorService, TIMEOUT).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        @Cleanup
        Context context = createContext(executorService);

        // create debug segment container instances using new new dataLog and old storage.
        Map<Integer, DebugStreamSegmentContainer> debugStreamSegmentContainerMap = startDebugSegmentContainers(context, dataLogFactory);

        outputInfo("Containers started. Recovering all segments...");
        ContainerRecoveryUtils.recoverAllSegments(storage, debugStreamSegmentContainerMap, executorService, TIMEOUT);
        outputInfo("All segments recovered.");

        // Update core attributes from the backUp Metadata segments
        outputInfo("Updating core attributes for segments registered.");
        ContainerRecoveryUtils.updateCoreAttributes(backUpMetadataSegments, debugStreamSegmentContainerMap, executorService,
                TIMEOUT);

        // Flush new metadata segment to the storage
        flushToStorage(debugStreamSegmentContainerMap);

        // Waits for metadata segments to be flushed to LTS and then stops the debug segment containers
        stopDebugSegmentContainers(debugStreamSegmentContainerMap);

        outputInfo("Segments have been recovered.");
        outputInfo("Recovery Done!");
    }

    // Flushes data from Durable log to the storage
    private void flushToStorage(Map<Integer, DebugStreamSegmentContainer> debugStreamSegmentContainerMap) {
        for (val debugSegmentContainer : debugStreamSegmentContainerMap.values()) {
            outputInfo("Waiting for metadata segment of container %d to be flushed to the Long-Term storage.",
                    debugSegmentContainer.getId());
            debugSegmentContainer.flushToStorage(TIMEOUT).join();
        }
    }

    // Creates debug segment container instances, puts them in a map and returns it.
    private Map<Integer, DebugStreamSegmentContainer> startDebugSegmentContainers(Context context, BookKeeperLogFactory dataLogFactory)
            throws Exception {
        // Start a debug segment container corresponding to the given container Id and put it in the Hashmap with the Id.
        Map<Integer, DebugStreamSegmentContainer> debugStreamSegmentContainerMap = new HashMap<>();
        OperationLogFactory localDurableLogFactory = new DurableLogFactory(NO_TRUNCATIONS_DURABLE_LOG_CONFIG, dataLogFactory, executorService);

        // Create a debug segment container instances using a
        for (int containerId = 0; containerId < this.containerCount; containerId++) {
            DebugStreamSegmentContainer debugStreamSegmentContainer = new
                    DebugStreamSegmentContainer(containerId, CONTAINER_CONFIG, localDurableLogFactory,
                    context.getReadIndexFactory(), context.getAttributeIndexFactory(), context.getWriterFactory(), this.storageFactory,
                    context.getDefaultExtensions(), executorService);

            outputInfo("Starting debug segment container %d.", containerId);
            Services.startAsync(debugStreamSegmentContainer, executorService).get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
            debugStreamSegmentContainerMap.put(containerId, debugStreamSegmentContainer);
        }
        return debugStreamSegmentContainerMap;
    }

    // Closes the debug segment container instances in the given map after waiting for the metadata segment to be flushed to
    // the given storage.
    private void stopDebugSegmentContainers(Map<Integer, DebugStreamSegmentContainer> debugStreamSegmentContainerMap)
            throws Exception {
        for (val debugSegmentContainerEntry : debugStreamSegmentContainerMap.entrySet()) {
            outputInfo("Stopping debug segment container %d.", debugSegmentContainerEntry.getKey());
            debugSegmentContainerEntry.getValue().close();
        }
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "durableLog-recovery", "Recovers the state of the DurableLog from the storage.");
    }
}
