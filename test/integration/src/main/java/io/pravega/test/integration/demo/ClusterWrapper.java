/**
 * Copyright (c) 2019 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.demo;

import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.controller.server.rpc.auth.StrongPasswordProcessor;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.delegationtoken.TokenVerifierImpl;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.host.stat.AutoScalerConfig;
import io.pravega.segmentstore.server.host.stat.SegmentStatsRecorder;
import io.pravega.segmentstore.server.host.stat.TableSegmentStatsRecorder;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.server.store.ServiceConfig;
import io.pravega.segmentstore.storage.DurableDataLogException;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.test.integration.utils.PasswordAuthHandlerInput;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;

import java.io.File;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import io.pravega.test.integration.utils.PasswordAuthHandlerInput.Entry;

/**
 * This class is intended to be used in integration tests for setting up and running a Pravega cluster. It is
 * much like the ControllerWrapper; This one wraps both Controller and Segment Store.
 */
@Slf4j
public class ClusterWrapper {

    private File passwordInputFile;

    @Getter
    private final int controllerPort = TestUtils.getAvailableListenPort();

    @Getter
    private final int segmentStorePort = TestUtils.getAvailableListenPort();

    @Getter
    private final String serviceHost = "localhost";

    // The servers
    private TestingServer zookeeperServer;
    private PravegaConnectionListener segmentStoreServer;
    private ControllerWrapper controllerServerWrapper;

    private ServiceBuilder serviceBuilder;

    private ScheduledExecutorService executor;

    // Configuration
    private boolean isAuthEnabled;
    private String tokenSigningKeyBasis;
    private int tokenTtlInSeconds;
    private List<PasswordAuthHandlerInput.Entry> passwordAuthHandlerEntries;
    private int containerCount = 4; // default container count

    public ClusterWrapper(boolean isAuthEnabled, int tokenTtlInSeconds) {
         this(isAuthEnabled, "secret", tokenTtlInSeconds,  null, 4);
    }

    public ClusterWrapper(boolean isAuthEnabled, String tokenSigningKeyBasis, int tokenTtlInSeconds,
                          List<PasswordAuthHandlerInput.Entry> passwordAuthHandlerEntries, int containerCount) {
        executor = Executors.newSingleThreadScheduledExecutor();

        this.isAuthEnabled = isAuthEnabled;
        this.tokenSigningKeyBasis = tokenSigningKeyBasis;
        this.tokenTtlInSeconds = tokenTtlInSeconds;
        if (isAuthEnabled) {
            if (passwordAuthHandlerEntries == null) {
                this.passwordAuthHandlerEntries = Arrays.asList(defaultAuthHandlerEntry());

            } else {
                this.passwordAuthHandlerEntries = passwordAuthHandlerEntries;
            }
        }
        this.containerCount = containerCount;
    }

    public void initialize() {
        try {
            startZookeeper();
            startSegmentStore();
            startController();
            log.info("Done initializing.");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void startZookeeper() throws Exception {
        zookeeperServer = new TestingServerStarter().start();
        log.info("Done starting Zookeeper.");
    }

    private void startController() {
        // Create and start controller service
        controllerServerWrapper = createControllerWrapper();
        controllerServerWrapper.awaitRunning();
        log.info("Done starting Controller");
    }

    private void startSegmentStore() throws DurableDataLogException {
        serviceBuilder = createServiceBuilder();
        serviceBuilder.initialize();

        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();

        if (isAuthEnabled) {
            passwordInputFile = createAuthFile(this.passwordAuthHandlerEntries);
        }

        segmentStoreServer = new PravegaConnectionListener(false, "localhost", segmentStorePort, store, tableStore,
            SegmentStatsRecorder.noOp(), TableSegmentStatsRecorder.noOp(),
            new TokenVerifierImpl(isAuthEnabled, tokenSigningKeyBasis),
            null, null, true);

        segmentStoreServer.startListening();
        log.info("Done starting Segment Store");
    }

    public void close() {
        ExecutorServiceHelpers.shutdown(executor);
        try {
            controllerServerWrapper.close();
            segmentStoreServer.close();
            serviceBuilder.close();
            zookeeperServer.close();
            if (passwordInputFile.exists()) {
                passwordInputFile.delete();
            }
        } catch (Exception e) {
            log.warn(e.getMessage(), e);
            // ignore
        }
    }

    private ServiceBuilder createServiceBuilder() {
        ServiceBuilderConfig.Builder configBuilder = ServiceBuilderConfig
                .builder()
                .include(ServiceConfig.builder()
                        .with(ServiceConfig.CONTAINER_COUNT, 1))
                .include(AutoScalerConfig.builder()
                        .with(AutoScalerConfig.CONTROLLER_URI, this.controllerUri())
                        .with(AutoScalerConfig.TOKEN_SIGNING_KEY, this.tokenSigningKeyBasis)
                        .with(AutoScalerConfig.AUTH_ENABLED, this.isAuthEnabled));

        return ServiceBuilder.newInMemoryBuilder(configBuilder.build());
    }

    private ControllerWrapper createControllerWrapper() {
        return new ControllerWrapper(zookeeperServer.getConnectString(),
                false, true,
                controllerPort, serviceHost, segmentStorePort, containerCount, -1,
                isAuthEnabled, passwordInputFile.getPath(), tokenSigningKeyBasis, tokenTtlInSeconds);
    }

    public String controllerUri() {
        return "tcp://localhost:" + controllerPort;
    }

    private Entry defaultAuthHandlerEntry() {
        StrongPasswordProcessor passwordProcessor = StrongPasswordProcessor.builder().build();
        try {
            String encryptedPassword = passwordProcessor.encryptPassword("1111_aaaa");
            return Entry.of("admin", encryptedPassword, "*,READ_UPDATE");
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new RuntimeException(e);
        }
    }

    private File createAuthFile(List<PasswordAuthHandlerInput.Entry> entries) {
        PasswordAuthHandlerInput result = new PasswordAuthHandlerInput(
                UUID.randomUUID().toString(), ".txt");
        result.postEntries(entries);
        return result.getFile();
    }
}