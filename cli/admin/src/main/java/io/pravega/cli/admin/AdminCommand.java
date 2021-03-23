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
import io.pravega.cli.admin.bookkeeper.BookKeeperListCommand;
import io.pravega.cli.admin.bookkeeper.ContainerRecoverCommand;
import io.pravega.cli.admin.controller.ControllerDescribeReaderGroupCommand;
import io.pravega.cli.admin.controller.ControllerDescribeScopeCommand;
import io.pravega.cli.admin.controller.ControllerDescribeStreamCommand;
import io.pravega.cli.admin.controller.ControllerListReaderGroupsInScopeCommand;
import io.pravega.cli.admin.controller.ControllerListScopesCommand;
import io.pravega.cli.admin.controller.ControllerListStreamsInScopeCommand;
import io.pravega.cli.admin.dataRecovery.DurableLogRecoveryCommand;
import io.pravega.cli.admin.dataRecovery.StorageListSegmentsCommand;
import io.pravega.cli.admin.password.PasswordFileCreatorCommand;
import io.pravega.cli.admin.cluster.GetClusterNodesCommand;
import io.pravega.cli.admin.cluster.GetSegmentStoreByContainerCommand;
import io.pravega.cli.admin.cluster.ListContainersCommand;
import io.pravega.cli.admin.config.ConfigListCommand;
import io.pravega.cli.admin.config.ConfigSetCommand;
import io.pravega.cli.admin.utils.CLIControllerConfig;
import io.pravega.common.Exceptions;
import io.pravega.segmentstore.server.store.ServiceConfig;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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
     * Creates a new instance of the CLIControllerConfig class from the shared AdminCommandState passed in via the Constructor.
     */
    protected CLIControllerConfig getCLIControllerConfig() {
        return getCommandArgs().getState().getConfigBuilder().build().getConfig(CLIControllerConfig::builder);
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

    protected void output(String messageTemplate, Object... args) {
        this.out.println(String.format(messageTemplate, args));
    }

    protected void prettyJSONOutput(String jsonString) {
        JsonElement je = new JsonParser().parse(jsonString);
        output(new GsonBuilder().disableHtmlEscaping().setPrettyPrinting().create().toJson(je));
    }

    protected void prettyJSONOutput(String key, Object value) {
        JsonElement je = new JsonParser().parse(objectToJSON(new Tuple(key, value)));
        output(new GsonBuilder().disableHtmlEscaping().setPrettyPrinting().create().toJson(je));
    }

    protected boolean confirmContinue() {
        output("Do you want to continue?[yes|no]");
        Scanner s = new Scanner(System.in);
        String input = s.nextLine();
        return input.equals("yes");
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
                        .put(BookKeeperDetailsCommand::descriptor, BookKeeperDetailsCommand::new)
                        .put(BookKeeperEnableCommand::descriptor, BookKeeperEnableCommand::new)
                        .put(BookKeeperDisableCommand::descriptor, BookKeeperDisableCommand::new)
                        .put(ContainerRecoverCommand::descriptor, ContainerRecoverCommand::new)
                        .put(ControllerListScopesCommand::descriptor, ControllerListScopesCommand::new)
                        .put(ControllerDescribeScopeCommand::descriptor, ControllerDescribeScopeCommand::new)
                        .put(ControllerListStreamsInScopeCommand::descriptor, ControllerListStreamsInScopeCommand::new)
                        .put(ControllerListReaderGroupsInScopeCommand::descriptor, ControllerListReaderGroupsInScopeCommand::new)
                        .put(ControllerDescribeReaderGroupCommand::descriptor, ControllerDescribeReaderGroupCommand::new)
                        .put(ControllerDescribeStreamCommand::descriptor, ControllerDescribeStreamCommand::new)
                        .put(GetClusterNodesCommand::descriptor, GetClusterNodesCommand::new)
                        .put(ListContainersCommand::descriptor, ListContainersCommand::new)
                        .put(GetSegmentStoreByContainerCommand::descriptor, GetSegmentStoreByContainerCommand::new)
                        .put(PasswordFileCreatorCommand::descriptor, PasswordFileCreatorCommand::new)
                        .put(StorageListSegmentsCommand::descriptor, StorageListSegmentsCommand::new)
                        .put(DurableLogRecoveryCommand::descriptor, DurableLogRecoveryCommand::new)
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

    //endregion
}
