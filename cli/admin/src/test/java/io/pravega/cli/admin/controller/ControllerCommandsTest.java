/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.cli.admin.controller;

import com.google.common.base.Preconditions;
import io.pravega.cli.admin.AdminCommandState;
import io.pravega.cli.admin.CommandArgs;
import io.pravega.cli.admin.Parser;
import io.pravega.cli.admin.utils.CLIControllerConfig;
import io.pravega.cli.admin.utils.TestUtils;
import io.pravega.client.ClientConfig;
import io.pravega.client.connection.impl.ConnectionPool;
import io.pravega.client.connection.impl.ConnectionPoolImpl;
import io.pravega.client.connection.impl.SocketConnectionFactoryImpl;
import io.pravega.client.stream.impl.DefaultCredentials;
import io.pravega.common.Exceptions;
import io.pravega.common.cluster.Host;
import io.pravega.controller.server.SegmentHelper;
import io.pravega.controller.store.client.StoreClientFactory;
import io.pravega.controller.store.host.HostControllerStore;
import io.pravega.controller.store.host.HostMonitorConfig;
import io.pravega.controller.store.host.HostStoreFactory;
import io.pravega.controller.store.host.impl.HostMonitorConfigImpl;
import io.pravega.controller.util.Config;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Validate basic controller commands.
 */
public class ControllerCommandsTest extends TLSEnabledControllerCommandsTest {

    @BeforeClass
    public static void setUp() throws Exception {
        setUpCluster(false, false);
    }

    @Test
    @SneakyThrows
    public void testDescribeReaderGroupCommand() {
        // Check that the system reader group can be listed.
        String commandResult = TestUtils.executeCommand("controller describe-readergroup _system commitStreamReaders", STATE.get());
        Assert.assertTrue(commandResult.contains("commitStreamReaders"));
        Assert.assertNotNull(ControllerDescribeReaderGroupCommand.descriptor());
    }

    @Test
    @SneakyThrows
    public void testDescribeStreamCommand() {
        String scope = "testScope";
        String testStream = "testStream";

        String commandResult = executeCommand("controller describe-stream " + scope + " " + testStream, STATE.get());
        Assert.assertTrue(commandResult.contains("stream_config"));
        Assert.assertTrue(commandResult.contains("stream_state"));
        Assert.assertTrue(commandResult.contains("segment_count"));
        Assert.assertTrue(commandResult.contains("is_sealed"));
        Assert.assertTrue(commandResult.contains("active_epoch"));
        Assert.assertTrue(commandResult.contains("truncation_record"));
        Assert.assertTrue(commandResult.contains("scaling_info"));

        // Exercise actual instantiateSegmentHelper
        CommandArgs commandArgs = new CommandArgs(Arrays.asList(scope, testStream), STATE.get());
        ControllerDescribeStreamCommand command = new ControllerDescribeStreamCommand(commandArgs);
        @Cleanup
        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient(CLUSTER.get().zookeeperConnectString(),
                new RetryOneTime(5000));
        curatorFramework.start();
        @Cleanup
        SegmentHelper sh = command.instantiateSegmentHelper(curatorFramework);
        Assert.assertNotNull(sh);

        // Try the Zookeeper backend, which is expected to fail and be handled by the command.
        Properties properties = new Properties();
        properties.setProperty("cli.store.metadata.backend", CLIControllerConfig.MetadataBackends.ZOOKEEPER.name());
        STATE.get().getConfigBuilder().include(properties);
        commandArgs = new CommandArgs(Arrays.asList(scope, testStream), STATE.get());
        new ControllerDescribeStreamCommand(commandArgs).execute();
        properties.setProperty("cli.store.metadata.backend", CLIControllerConfig.MetadataBackends.SEGMENTSTORE.name());
        STATE.get().getConfigBuilder().include(properties);
    }

    static String executeCommand(String inputCommand, AdminCommandState state) throws Exception {
        Parser.Command pc = Parser.parse(inputCommand);
        Assert.assertNotNull(pc.toString());
        CommandArgs args = new CommandArgs(pc.getArgs(), state);
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        TestingDescribeStreamCommand cmd = new TestingDescribeStreamCommand(args);
        try (PrintStream ps = new PrintStream(baos, true, StandardCharsets.UTF_8)) {
            cmd.setOut(ps);
            cmd.execute();
        }
        return new String(baos.toByteArray(), StandardCharsets.UTF_8);
    }

    private static class TestingDescribeStreamCommand extends ControllerDescribeStreamCommand {

        /**
         * Creates a new instance of the Command class.
         *
         * @param args The arguments for the command.
         */
        public TestingDescribeStreamCommand(CommandArgs args) {
            super(args);
        }

        @Override
        protected SegmentHelper instantiateSegmentHelper(CuratorFramework zkClient) {
            HostMonitorConfig hostMonitorConfig = HostMonitorConfigImpl.builder()
                    .hostMonitorEnabled(false)
                    .hostContainerMap(getHostContainerMap(Collections.singletonList("localhost:" + CLUSTER.get().getSegmentStorePort()),
                            getServiceConfig().getContainerCount()))
                    .hostMonitorMinRebalanceInterval(Config.CLUSTER_MIN_REBALANCE_INTERVAL)
                    .containerCount(getServiceConfig().getContainerCount())
                    .build();
            HostControllerStore hostStore = HostStoreFactory.createStore(hostMonitorConfig, StoreClientFactory.createZKStoreClient(zkClient));
            ClientConfig clientConfig = ClientConfig.builder()
                    .controllerURI(URI.create(getCLIControllerConfig().getControllerGrpcURI()))
                    .validateHostName(false)
                    .credentials(new DefaultCredentials(getCLIControllerConfig().getPassword(),
                            getCLIControllerConfig().getUserName()))
                    .build();
            ConnectionPool pool = new ConnectionPoolImpl(clientConfig, new SocketConnectionFactoryImpl(clientConfig));
            return new SegmentHelper(pool, hostStore, pool.getInternalExecutor());
        }

        private Map<Host, Set<Integer>> getHostContainerMap(List<String> uri, int containerCount) {
            Exceptions.checkNotNullOrEmpty(uri, "uri");

            Map<Host, Set<Integer>> hostContainerMap = new HashMap<>();
            uri.forEach(x -> {
                // Get the host and port from the URI
                String host = x.split(":")[0];
                int port = Integer.parseInt(x.split(":")[1]);
                Preconditions.checkNotNull(host, "host");
                Preconditions.checkArgument(port > 0, "port");
                Preconditions.checkArgument(containerCount > 0, "containerCount");
                hostContainerMap.put(new Host(host, port, null), IntStream.range(0, containerCount).boxed().collect(Collectors.toSet()));
            });
            return hostContainerMap;
        }

        @Override
        public void execute() {
            super.execute();
        }
    }

    public static class AuthEnabledControllerCommandsTest extends ControllerCommandsTest {
        @BeforeClass
        public static void setUp() throws Exception {
            setUpCluster(true, false);
        }
    }
}
