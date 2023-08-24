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
package io.pravega.cli.admin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import io.pravega.cli.admin.bookkeeper.BookKeeperCleanupCommand;
import io.pravega.cli.admin.bookkeeper.BookKeeperDetailsCommand;
import io.pravega.cli.admin.bookkeeper.BookKeeperDisableCommand;
import io.pravega.cli.admin.bookkeeper.BookKeeperEnableCommand;
import io.pravega.cli.admin.bookkeeper.BookKeeperListAllLedgersCommand;
import io.pravega.cli.admin.bookkeeper.BookKeeperListCommand;
import io.pravega.cli.admin.bookkeeper.BookKeeperLogReconcileCommand;
import io.pravega.cli.admin.bookkeeper.BookkeeperDeleteLedgersCommand;
import io.pravega.cli.admin.bookkeeper.ContainerContinuousRecoveryCommand;
import io.pravega.cli.admin.bookkeeper.ContainerRecoverCommand;
import io.pravega.cli.admin.cluster.GetClusterNodesCommand;
import io.pravega.cli.admin.cluster.GetSegmentStoreByContainerCommand;
import io.pravega.cli.admin.cluster.ListContainersCommand;
import io.pravega.cli.admin.config.ConfigListCommand;
import io.pravega.cli.admin.config.ConfigSetCommand;
import io.pravega.cli.admin.controller.ControllerDeleteReaderGroupCommand;
import io.pravega.cli.admin.controller.ControllerDescribeReaderGroupCommand;
import io.pravega.cli.admin.controller.ControllerDescribeScopeCommand;
import io.pravega.cli.admin.controller.ControllerDescribeStreamCommand;
import io.pravega.cli.admin.controller.ControllerListReaderGroupsInScopeCommand;
import io.pravega.cli.admin.controller.ControllerListScopesCommand;
import io.pravega.cli.admin.controller.ControllerListStreamsInScopeCommand;
import io.pravega.cli.admin.controller.metadata.ControllerMetadataGetEntryCommand;
import io.pravega.cli.admin.controller.metadata.ControllerMetadataListEntriesCommand;
import io.pravega.cli.admin.controller.metadata.ControllerMetadataListKeysCommand;
import io.pravega.cli.admin.controller.metadata.ControllerMetadataTablesInfoCommand;
import io.pravega.cli.admin.controller.metadata.ControllerMetadataUpdateEntryCommand;
import io.pravega.cli.admin.controller.metadata.ControllerMetadataViewPendingEventsCommand;
import io.pravega.cli.admin.controller.metadata.ControllerMetadataViewReaderInfoCommand;
import io.pravega.cli.admin.dataRecovery.DurableDataLogRepairCommand;
import io.pravega.cli.admin.dataRecovery.DurableLogInspectCommand;
import io.pravega.cli.admin.dataRecovery.RecoverFromStorageCommand;
import io.pravega.cli.admin.dataRecovery.StorageListSegmentsCommand;
import io.pravega.cli.admin.dataRecovery.TableSegmentRecoveryCommand;
import io.pravega.cli.admin.password.PasswordFileCreatorCommand;
import io.pravega.cli.admin.readerGroup.ParseReaderGroupStreamCommand;
import io.pravega.cli.admin.segmentstore.CreateSegmentCommand;
import io.pravega.cli.admin.segmentstore.DeleteSegmentCommand;
import io.pravega.cli.admin.segmentstore.FlushToStorageCommand;
import io.pravega.cli.admin.segmentstore.GetContainerIdOfSegmentCommand;
import io.pravega.cli.admin.segmentstore.UpdateSegmentAttributeCommand;
import io.pravega.cli.admin.segmentstore.GetSegmentAttributeCommand;
import io.pravega.cli.admin.segmentstore.GetSegmentInfoCommand;
import io.pravega.cli.admin.segmentstore.ReadSegmentRangeCommand;
import io.pravega.cli.admin.segmentstore.storage.ListChunksCommand;
import io.pravega.cli.admin.segmentstore.storage.StorageUpdateSnapshotCommand;
import io.pravega.cli.admin.segmentstore.tableSegment.GetTableSegmentEntryCommand;
import io.pravega.cli.admin.segmentstore.tableSegment.GetTableSegmentInfoCommand;
import io.pravega.cli.admin.segmentstore.tableSegment.ListTableSegmentKeysCommand;
import io.pravega.cli.admin.segmentstore.tableSegment.ModifyTableSegmentEntry;
import io.pravega.cli.admin.segmentstore.tableSegment.PutTableSegmentEntryCommand;
import io.pravega.cli.admin.segmentstore.tableSegment.RemoveTableSegmentKeyCommand;
import io.pravega.cli.admin.segmentstore.tableSegment.SetSerializerCommand;
import io.pravega.cli.admin.utils.AdminSegmentHelper;
import io.pravega.cli.admin.utils.CLIConfig;
import io.pravega.client.ClientConfig;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.ConnectionPoolImpl;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import io.pravega.common.Exceptions;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.store.client.StoreClientFactory;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostMonitorConfig;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.util.Config;
import io.pravega.segmentstore.server.attributes.AttributeIndexConfig;
import io.pravega.segmentstore.server.store.ServiceConfig;
import io.pravega.shared.security.auth.DefaultCredentials;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.io.PrintStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Base class for any command to execute from the Admin tool.
 */
public abstract class AdminCommand {
    //region Private

    @Getter(AccessLevel.PROTECTED)
    private final CommandArgs commandArgs;

    @VisibleForTesting
    @Getter(AccessLevel.PUBLIC)
    @Setter(AccessLevel.PUBLIC)
    private PrintStream out = System.out;

    @VisibleForTesting
    @Getter(AccessLevel.PUBLIC)
    @Setter(AccessLevel.PUBLIC)
    private PrintStream err = System.err;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the Command class.
     *
     * @param args The arguments for the command.
     */
    public AdminCommand(CommandArgs args) {
        this.commandArgs = Preconditions.checkNotNull(args, "commandArgs");
    }

    //endregion

    //region Command Implementation

    /**
     * Executes the command with the arguments passed in via the Constructor. The command will allocate whatever resources
     * it needs to execute and will clean up after its execution completes (successful or not). The only expected side
     * effect may be the modification of the shared AdminCommandState that is passed in via the Constructor.
     *
     * @throws IllegalArgumentException If the arguments passed in via the Constructor are invalid.
     * @throws Exception                If the command failed to execute.
     */
    public abstract void execute() throws Exception;

    /**
     * Creates a new instance of the ServiceConfig class from the shared AdminCommandState passed in via the Constructor.
     */
    protected ServiceConfig getServiceConfig() {
        return getCommandArgs().getState().getConfigBuilder().build().getConfig(ServiceConfig::builder);
    }

    /**
     * Creates a new instance of the CLIConfig class from the shared AdminCommandState passed in via the Constructor.
     */
    protected CLIConfig getCLIControllerConfig() {
        return getCommandArgs().getState().getConfigBuilder().build().getConfig(CLIConfig::builder);
    }

    /**
     * Creates a new instance of the AttributeIndexConfig class from the shared AdminCommandState passed in via the Constructor.
     */
    protected AttributeIndexConfig getAttributeIndexConfig() {
        return getCommandArgs().getState().getConfigBuilder().build().getConfig(AttributeIndexConfig::builder);
    }

    /**
     * Creates a new instance of the CuratorFramework class using configuration from the shared AdminCommandState.
     */
    protected CuratorFramework createZKClient() {
        val serviceConfig = getServiceConfig();
        CuratorFramework zkClient = CuratorFrameworkFactory
                .builder()
                .connectString(serviceConfig.getZkURL())
                .namespace(serviceConfig.getClusterName())
                .retryPolicy(new ExponentialBackoffRetry(serviceConfig.getZkRetrySleepMs(), serviceConfig.getZkRetryCount()))
                .sessionTimeoutMs(serviceConfig.getZkSessionTimeoutMs())
                .build();
        zkClient.start();
        return zkClient;
    }

    /**
     * Outputs the message to the console.
     *
     * @param messageTemplate   The message.
     * @param args              The arguments with the message.
     */
    protected void output(String messageTemplate, Object... args) {
        this.out.printf(messageTemplate + System.lineSeparator(), args);
    }

    /**
     * Outputs the message to the console (error out).
     *
     * @param messageTemplate   The message.
     * @param args              The arguments with the message.
     */
    protected void outputError(String messageTemplate, Object... args) {
        this.err.printf(messageTemplate + System.lineSeparator(), args);
    }

    /**
     * Gets an exception and prints the stacktrace to the console (error out).
     *
     * @param exception   The exception.
     */
    protected void outputException(Throwable exception) {
        exception.printStackTrace(this.err);
    }

    protected void prettyJSONOutput(String jsonString) {
        @SuppressWarnings("deprecation")
        JsonElement je = new JsonParser().parse(jsonString);
        output(new GsonBuilder().disableHtmlEscaping().setPrettyPrinting().create().toJson(je));
    }

    protected void prettyJSONOutput(String key, Object value) {
        @SuppressWarnings("deprecation")
        JsonElement je = new JsonParser().parse(objectToJSON(new Tuple(key, value)));
        output(new GsonBuilder().disableHtmlEscaping().setPrettyPrinting().create().toJson(je));
    }

    @VisibleForTesting
    public boolean confirmContinue() {
        output("Do you want to continue?[yes|no]");
        @SuppressWarnings("resource")
        Scanner s = new Scanner(System.in);
        String input = s.nextLine();
        return input.equals("yes");
    }

    @VisibleForTesting
    public String getStringUserInput(String message) {
        output(message);
        @SuppressWarnings("resource")
        Scanner s = new Scanner(System.in);
        return s.nextLine();
    }

    @VisibleForTesting
    public long getLongUserInput(String message) {
        output(message);
        @SuppressWarnings("resource")
        Scanner s = new Scanner(System.in);
        return s.nextLong();
    }

    @VisibleForTesting
    public int getIntUserInput(String message) {
        output(message);
        @SuppressWarnings("resource")
        Scanner s = new Scanner(System.in);
        return s.nextInt();
    }

    @VisibleForTesting
    public boolean getBooleanUserInput(String message) {
        output(message);
        @SuppressWarnings("resource")
        Scanner s = new Scanner(System.in);
        return s.nextBoolean();
    }

    //endregion

    //region Arguments

    protected void ensureArgCount(int expectedCount) {
        Preconditions.checkArgument(this.commandArgs.getArgs().size() == expectedCount, "Incorrect argument count.");
    }

    protected int getArgCount() {
        return this.commandArgs.getArgs().size();
    }

    protected int getIntArg(int index) {
        return getArg(index, Integer::parseInt);
    }

    protected long getLongArg(int index) {
        return getArg(index, Long::parseLong);
    }

    protected UUID getUUIDArg(int index) {
        return getArg(index, UUID::fromString);
    }

    protected String getArg(int index) {
        return getArg(index, String::toString);
    }

    private <T> T getArg(int index, Function<String, T> converter) {
        String s = null;
        try {
            s = this.commandArgs.getArgs().get(index);
            return converter.apply(s);
        } catch (Exception ex) {
            throw new IllegalArgumentException(String.format("Unexpected argument '%s' at position %d: %s.", s, index, ex.getMessage()));
        }
    }

    //endregion

    //region Descriptors

    /**
     * Describes an argument.
     */
    @RequiredArgsConstructor
    @Getter
    public static class ArgDescriptor {
        private final String name;
        private final String description;
        private boolean optional = false;

        public ArgDescriptor(String name, String description, boolean optional) {
            this.name = name;
            this.description = description;
            this.optional = optional;
        }
    }

    /**
     * Describes a Command.
     */
    @Getter
    public static class CommandDescriptor {
        private final String component;
        private final String name;
        private final String description;
        private final ArgDescriptor[] args;
        public CommandDescriptor(String component, String name, String description, ArgDescriptor... args) {
            this.component = Exceptions.checkNotNullOrEmpty(component, "component");
            this.name = Exceptions.checkNotNullOrEmpty(name, "name");
            this.description = Exceptions.checkNotNullOrEmpty(description, "description");
            this.args = args;
        }
    }

    //endregion

    //region Factory

    /**
     * Helps create new Command instances.
     */
    public static class Factory {
        private static final Map<String, Map<String, CommandInfo>> COMMANDS = registerAll(
                ImmutableMap.<Supplier<CommandDescriptor>, CommandCreator>builder()
                        .put(ConfigListCommand::descriptor, ConfigListCommand::new)
                        .put(ConfigSetCommand::descriptor, ConfigSetCommand::new)
                        .put(BookKeeperCleanupCommand::descriptor, BookKeeperCleanupCommand::new)
                        .put(BookKeeperListCommand::descriptor, BookKeeperListCommand::new)
                        .put(BookkeeperDeleteLedgersCommand::descriptor, BookkeeperDeleteLedgersCommand::new)
                        .put(BookKeeperDetailsCommand::descriptor, BookKeeperDetailsCommand::new)
                        .put(BookKeeperEnableCommand::descriptor, BookKeeperEnableCommand::new)
                        .put(BookKeeperDisableCommand::descriptor, BookKeeperDisableCommand::new)
                        .put(BookKeeperLogReconcileCommand::descriptor, BookKeeperLogReconcileCommand::new)
                        .put(BookKeeperListAllLedgersCommand::descriptor, BookKeeperListAllLedgersCommand::new)
                        .put(ContainerRecoverCommand::descriptor, ContainerRecoverCommand::new)
                        .put(ContainerContinuousRecoveryCommand::descriptor, ContainerContinuousRecoveryCommand::new)
                        .put(ControllerListScopesCommand::descriptor, ControllerListScopesCommand::new)
                        .put(ControllerDescribeScopeCommand::descriptor, ControllerDescribeScopeCommand::new)
                        .put(ControllerListStreamsInScopeCommand::descriptor, ControllerListStreamsInScopeCommand::new)
                        .put(ControllerListReaderGroupsInScopeCommand::descriptor, ControllerListReaderGroupsInScopeCommand::new)
                        .put(ControllerDeleteReaderGroupCommand::descriptor, ControllerDeleteReaderGroupCommand::new)
                        .put(ControllerDescribeReaderGroupCommand::descriptor, ControllerDescribeReaderGroupCommand::new)
                        .put(ControllerDescribeStreamCommand::descriptor, ControllerDescribeStreamCommand::new)
                        .put(GetClusterNodesCommand::descriptor, GetClusterNodesCommand::new)
                        .put(ListContainersCommand::descriptor, ListContainersCommand::new)
                        .put(GetSegmentStoreByContainerCommand::descriptor, GetSegmentStoreByContainerCommand::new)
                        .put(PasswordFileCreatorCommand::descriptor, PasswordFileCreatorCommand::new)
                        .put(StorageListSegmentsCommand::descriptor, StorageListSegmentsCommand::new)
                        .put(StorageUpdateSnapshotCommand::descriptor, StorageUpdateSnapshotCommand::new)
                        .put(DurableLogInspectCommand::descriptor, DurableLogInspectCommand::new)
                        .put(RecoverFromStorageCommand::descriptor, RecoverFromStorageCommand::new)
                        .put(DurableDataLogRepairCommand::descriptor, DurableDataLogRepairCommand::new)
                        .put(TableSegmentRecoveryCommand::descriptor, TableSegmentRecoveryCommand::new)
                        .put(GetSegmentInfoCommand::descriptor, GetSegmentInfoCommand::new)
                        .put(CreateSegmentCommand::descriptor, CreateSegmentCommand::new)
                        .put(DeleteSegmentCommand::descriptor, DeleteSegmentCommand::new)
                        .put(ReadSegmentRangeCommand::descriptor, ReadSegmentRangeCommand::new)
                        .put(GetSegmentAttributeCommand::descriptor, GetSegmentAttributeCommand::new)
                        .put(UpdateSegmentAttributeCommand::descriptor, UpdateSegmentAttributeCommand::new)
                        .put(FlushToStorageCommand::descriptor, FlushToStorageCommand::new)
                        .put(GetContainerIdOfSegmentCommand::descriptor, GetContainerIdOfSegmentCommand::new)
                        .put(RemoveTableSegmentKeyCommand::descriptor, RemoveTableSegmentKeyCommand::new)
                        .put(GetTableSegmentInfoCommand::descriptor, GetTableSegmentInfoCommand::new)
                        .put(GetTableSegmentEntryCommand::descriptor, GetTableSegmentEntryCommand::new)
                        .put(PutTableSegmentEntryCommand::descriptor, PutTableSegmentEntryCommand::new)
                        .put(SetSerializerCommand::descriptor, SetSerializerCommand::new)
                        .put(ListTableSegmentKeysCommand::descriptor, ListTableSegmentKeysCommand::new)
                        .put(ModifyTableSegmentEntry::descriptor, ModifyTableSegmentEntry::new)
                        .put(ControllerMetadataGetEntryCommand::descriptor, ControllerMetadataGetEntryCommand::new)
                        .put(ControllerMetadataTablesInfoCommand::descriptor, ControllerMetadataTablesInfoCommand::new)
                        .put(ControllerMetadataListKeysCommand::descriptor, ControllerMetadataListKeysCommand::new)
                        .put(ControllerMetadataListEntriesCommand::descriptor, ControllerMetadataListEntriesCommand::new)
                        .put(ParseReaderGroupStreamCommand::descriptor, ParseReaderGroupStreamCommand::new)
                        .put(ControllerMetadataUpdateEntryCommand::descriptor, ControllerMetadataUpdateEntryCommand::new)
                        .put(ControllerMetadataViewReaderInfoCommand::descriptor, ControllerMetadataViewReaderInfoCommand::new)
                        .put(ListChunksCommand::descriptor, ListChunksCommand::new)
                        .put(ControllerMetadataViewPendingEventsCommand::descriptor, ControllerMetadataViewPendingEventsCommand::new)
                        .build());

        /**
         * Gets a Collection of CommandDescriptors for all registered commands.
         *
         * @return A new Collection.
         */
        public static Collection<CommandDescriptor> getDescriptors() {
            ArrayList<CommandDescriptor> result = new ArrayList<>();
            COMMANDS.values().forEach(g -> g.values().forEach(c -> result.add(c.getDescriptor())));
            return result;
        }

        /**
         * Gets a Collection of CommandDescriptors for all registered commands for the given component.
         *
         * @param component The component to query.
         * @return A new Collection.
         */
        public static Collection<CommandDescriptor> getDescriptors(String component) {
            Map<String, CommandInfo> componentCommands = COMMANDS.getOrDefault(component, null);
            return componentCommands == null
                    ? Collections.emptyList()
                    : componentCommands.values().stream().map(CommandInfo::getDescriptor).collect(Collectors.toList());
        }

        /**
         * Gets a CommandDescriptor for the given commandArgs.
         *
         * @param component The name of the Component to get the descriptor for.
         * @param command   The name of the Command (within the Component) to get.
         * @return The CommandDescriptor for the given argument, or null if no such command is registered.
         */
        public static CommandDescriptor getDescriptor(String component, String command) {
            CommandInfo ci = getCommand(component, command);
            return ci == null ? null : ci.getDescriptor();
        }

        /**
         * Gets a new instance of a Command for the given commandArgs.
         *
         * @param component The name of the Component to get the Command for.
         * @param command   The name of the Command (within the Component) to get.
         * @param args      CommandArgs for the command.
         * @return A new instance of a Command base, already initialized with the command's commandArgs.
         */
        public static AdminCommand get(String component, String command, CommandArgs args) {
            CommandInfo ci = getCommand(component, command);
            return ci == null ? null : ci.getCreator().apply(args);
        }

        private static CommandInfo getCommand(String component, String command) {
            Map<String, CommandInfo> componentCommands = COMMANDS.getOrDefault(component, null);
            return componentCommands == null ? null : componentCommands.getOrDefault(command, null);
        }

        private static Map<String, Map<String, CommandInfo>> registerAll(Map<Supplier<CommandDescriptor>, CommandCreator> items) {
            val result = new HashMap<String, Map<String, CommandInfo>>();
            for (val e : items.entrySet()) {
                AdminCommand.CommandDescriptor d = e.getKey().get();
                Map<String, CommandInfo> componentCommands = result.getOrDefault(d.getComponent(), null);
                if (componentCommands == null) {
                    componentCommands = new HashMap<>();
                    result.put(d.getComponent(), componentCommands);
                }

                if (componentCommands.putIfAbsent(d.getName(), new CommandInfo(d, e.getValue())) != null) {
                    throw new IllegalArgumentException(String.format("A command is already registered for '%s'-'%s'.", d.getComponent(), d.getName()));
                }
            }
            return Collections.unmodifiableMap(result);
        }

        @Data
        private static class CommandInfo {
            private final CommandDescriptor descriptor;
            private final CommandCreator creator;
        }

        @FunctionalInterface
        private interface CommandCreator extends Function<CommandArgs, AdminCommand> {
        }
    }

    @Data
    private static class Tuple {
        private final String key;
        private final Object value;
    }

    @SneakyThrows
    private String objectToJSON(Object object) {
        return new ObjectMapper().writeValueAsString(object);
    }

    protected Controller instantiateController(ConnectionPool pool) {
        return new ControllerImpl(ControllerImplConfig.builder()
                                    .clientConfig(getClientConfig())
                                    .build(), pool.getInternalExecutor());
    }

    @VisibleForTesting
    public SegmentHelper instantiateSegmentHelper(CuratorFramework zkClient, ConnectionPool pool) {
        HostControllerStore hostStore = createHostControllerStore(zkClient);
        return new SegmentHelper(pool, hostStore, pool.getInternalExecutor());
    }

    @VisibleForTesting
    public AdminSegmentHelper instantiateAdminSegmentHelper(CuratorFramework zkClient) {
        HostControllerStore hostStore = createHostControllerStore(zkClient);
        ConnectionPool pool = createConnectionPool();
        return new AdminSegmentHelper(pool, hostStore, pool.getInternalExecutor());
    }

    private HostControllerStore createHostControllerStore(CuratorFramework zkClient) {
        HostMonitorConfig hostMonitorConfig = HostMonitorConfigImpl.builder()
                .hostMonitorEnabled(true)
                .hostMonitorMinRebalanceInterval(Config.CLUSTER_MIN_REBALANCE_INTERVAL)
                .containerCount(getServiceConfig().getContainerCount())
                .build();
        return HostStoreFactory.createStore(hostMonitorConfig, StoreClientFactory.createZKStoreClient(zkClient));
    }

    private ClientConfig getClientConfig() {
        ClientConfig.ClientConfigBuilder clientConfigBuilder = ClientConfig.builder()
                .controllerURI(URI.create(getCLIControllerConfig().getControllerGrpcURI()));

        if (getCLIControllerConfig().isAuthEnabled()) {
            clientConfigBuilder.credentials(new DefaultCredentials(getCLIControllerConfig().getPassword(),
                    getCLIControllerConfig().getUserName()));
        }
        if (getCLIControllerConfig().isTlsEnabled()) {
            clientConfigBuilder.trustStore(getCLIControllerConfig().getTruststore())
                    .validateHostName(false);
        }

        ClientConfig clientConfig = clientConfigBuilder.build();
        return clientConfig;
    }

    protected ConnectionPool createConnectionPool() {
        ClientConfig clientConfig = getClientConfig();
        return new ConnectionPoolImpl(clientConfig, new SocketConnectionFactoryImpl(clientConfig));
    }

    //endregion
}
