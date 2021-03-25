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
package io.pravega.cli.admin.controller;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.cli.admin.utils.CLIControllerConfig;
import io.pravega.client.ClientConfig;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.ConnectionPoolImpl;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.shared.security.auth.DefaultCredentials;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.server.security.auth.GrpcAuthHelper;
import io.pravega.controller.store.client.StoreClientFactory;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostMonitorConfig;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.store.stream.StreamMetadataStore;
import io.pravega.controller.store.stream.StreamStoreFactory;
import io.pravega.controller.store.stream.records.ActiveTxnRecord;
import io.pravega.controller.util.Config;
import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import lombok.Cleanup;
import org.apache.curator.framework.CuratorFramework;

/**
 * Gets a description of different characteristics related to a Stream (e.g., configuration, state, active txn).
 */
public class ControllerDescribeStreamCommand extends ControllerCommand {

    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    public ControllerDescribeStreamCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() {
        ensureArgCount(2);
        final String scope = getCommandArgs().getArgs().get(0);
        final String stream = getCommandArgs().getArgs().get(1);

        try {
            @Cleanup
            CuratorFramework zkClient = createZKClient();
            ScheduledExecutorService executor = getCommandArgs().getState().getExecutor();

            // The Pravega Controller service may store metadata either at Zookeeper or the Segment Store service
            // (tables). We need to instantiate the correct type of metadata store object based on the cluster at hand.
            StreamMetadataStore store;
            SegmentHelper segmentHelper = null;
            if (getCLIControllerConfig().getMetadataBackend().equals(CLIControllerConfig.MetadataBackends.ZOOKEEPER.name())) {
                store = StreamStoreFactory.createZKStore(zkClient, executor);
            } else {
                segmentHelper = instantiateSegmentHelper(zkClient);
                GrpcAuthHelper authHelper;
                authHelper = GrpcAuthHelper.getDisabledAuthHelper();
                store = StreamStoreFactory.createPravegaTablesStore(segmentHelper, authHelper, zkClient, executor);
            }

            // Output the configuration of this Stream.
            CompletableFuture<StreamConfiguration> streamConfig = store.getConfiguration(scope, stream, null, executor);
            prettyJSONOutput("stream_config", streamConfig.join());

            // Output the state for this Stream.
            prettyJSONOutput("stream_state", store.getState(scope, stream, true, null, executor).join());

            // Output the total number of segments for this Stream.
            Set<Long> segments = store.getAllSegmentIds(scope, stream, null, executor).join();
            prettyJSONOutput("segment_count", segments.size());

            // Check if the Stream is sealed.
            prettyJSONOutput("is_sealed", store.isSealed(scope, stream, null, executor).join());

            // Output the active epoch for this Stream.
            prettyJSONOutput("active_epoch", store.getActiveEpoch(scope, stream, null, true, executor).join());

            // Output the number of active Transactions for ths Stream.
            Map<UUID, ActiveTxnRecord> activeTxn = store.getActiveTxns(scope, stream, null, getCommandArgs().getState().getExecutor()).join();
            if (!activeTxn.isEmpty()) {
                prettyJSONOutput("active_transactions", activeTxn);
            }

            // Output Truncation point.
            prettyJSONOutput("truncation_record", store.getTruncationRecord(scope, stream, null, executor).join().getObject());

            // Output the metadata that describes all the scaling information for this Stream.
            prettyJSONOutput("scaling_info", store.getScaleMetadata(scope, stream, segments.stream().min(Long::compareTo).get(),
                    segments.stream().max(Long::compareTo).get(), null, executor).join());

            // Cleanup resources.
            if (segmentHelper != null) {
                segmentHelper.close();
                store.close();
            }
        } catch (Exception e) {
            System.err.println("Exception accessing the metadata store: " + e.getMessage());
        }
    }

    public static CommandDescriptor descriptor() {
        return new CommandDescriptor(COMPONENT, "describe-stream", "Get the details of a given Stream.",
                new ArgDescriptor("scope-name", "Name of the Scope where the Stream belongs to."),
                new ArgDescriptor("stream-name", "Name of the Stream to describe."));
    }

    @VisibleForTesting
    protected SegmentHelper instantiateSegmentHelper(CuratorFramework zkClient) {
        HostMonitorConfig hostMonitorConfig = HostMonitorConfigImpl.builder()
                                                                   .hostMonitorEnabled(true)
                                                                   .hostMonitorMinRebalanceInterval(Config.CLUSTER_MIN_REBALANCE_INTERVAL)
                                                                   .containerCount(getServiceConfig().getContainerCount())
                                                                   .build();
        HostControllerStore hostStore = HostStoreFactory.createStore(hostMonitorConfig, StoreClientFactory.createZKStoreClient(zkClient));
        ClientConfig clientConfig = ClientConfig.builder()
                                                .controllerURI(URI.create(getCLIControllerConfig().getControllerGrpcURI()))
                                                .validateHostName(getCLIControllerConfig().isAuthEnabled())
                                                .credentials(new DefaultCredentials(getCLIControllerConfig().getPassword(), getCLIControllerConfig().getUserName()))
                                                .build();
        ConnectionPool pool = new ConnectionPoolImpl(clientConfig, new SocketConnectionFactoryImpl(clientConfig));
        return new SegmentHelper(pool, hostStore, pool.getInternalExecutor());
    }
}
