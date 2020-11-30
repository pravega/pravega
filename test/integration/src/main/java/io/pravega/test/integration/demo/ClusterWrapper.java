/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.test.integration.demo;

import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.shared.security.crypto.StrongPasswordProcessor;
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
import io.pravega.shared.security.auth.PasswordAuthHandlerInput;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.ToString;
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

import io.pravega.shared.security.auth.PasswordAuthHandlerInput.Entry;

/**
 * This class is intended to be used in integration tests for setting up and running a Pravega cluster. It is
 * much like the ControllerWrapper; This one wraps both Controller and Segment Store.
 */
@ToString
@Slf4j
@Builder
@AllArgsConstructor
public class ClusterWrapper implements AutoCloseable {

    private File passwordInputFile;

    @Getter
    @Builder.Default
    private int controllerPort = TestUtils.getAvailableListenPort();

    @Getter
    @Builder.Default
    private int segmentStorePort = TestUtils.getAvailableListenPort();

    @Getter
    @Builder.Default
    private String serviceHost = "localhost";

    // The servers
    private TestingServer zookeeperServer;
    private PravegaConnectionListener segmentStoreServer;
    private ControllerWrapper controllerServerWrapper;

    private ServiceBuilder serviceBuilder;

    @Builder.Default
    private ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    @Getter
    @Builder.Default
    private boolean authEnabled = false;

    @Getter
    @Builder.Default
    private String tokenSigningKeyBasis = "super-secret";

    @Getter
    @Builder.Default
    private boolean rgWritesWithReadPermEnabled = true;

    @Getter
    @Builder.Default
    private int tokenTtlInSeconds = 600;

    @Getter
    private List<PasswordAuthHandlerInput.Entry> passwordAuthHandlerEntries;

    @Getter
    @Builder.Default
    private int containerCount = 4;

    @Getter
    @Builder.Default
    private boolean tlsEnabled = false;

    @Getter
    private String tlsServerCertificatePath;

    @Getter
    private String tlsServerKeyPath;

    @Getter
    @Builder.Default
    private boolean tlsHostVerificationEnabled = false;

    @Getter
    private String tlsServerKeystorePath;

    @Getter
    private String tlsServerKeystorePasswordPath;

    private ClusterWrapper() {}

    public void initialize() {
        try {
            if (this.isAuthEnabled() && passwordAuthHandlerEntries == null) {
                this.passwordAuthHandlerEntries = Arrays.asList(defaultAuthHandlerEntry());
            }
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

        if (authEnabled) {
            passwordInputFile = createAuthFile(this.passwordAuthHandlerEntries);
        }

        segmentStoreServer = new PravegaConnectionListener(this.tlsEnabled, false, "localhost", segmentStorePort, store, tableStore,
            SegmentStatsRecorder.noOp(), TableSegmentStatsRecorder.noOp(),
            authEnabled ? new TokenVerifierImpl(tokenSigningKeyBasis) : null,
            this.tlsServerCertificatePath, this.tlsServerKeyPath, true, serviceBuilder.getLowPriorityExecutor());

        segmentStoreServer.startListening();
        log.info("Done starting Segment Store");
    }

    @Override
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
                        .with(AutoScalerConfig.CONTROLLER_URI, controllerUri())
                        .with(AutoScalerConfig.TOKEN_SIGNING_KEY, tokenSigningKeyBasis)
                        .with(AutoScalerConfig.AUTH_ENABLED, authEnabled));

        return ServiceBuilder.newInMemoryBuilder(configBuilder.build());
    }

    private ControllerWrapper createControllerWrapper() {
        String passwordInputFilePath = null;
        if (passwordInputFile != null) {
            passwordInputFilePath = passwordInputFile.getPath();
        }

        return ControllerWrapper.builder()
                .connectionString(zookeeperServer.getConnectString())
                .disableEventProcessor(false)
                .disableControllerCluster(true)
                .controllerPort(controllerPort)
                .serviceHost(serviceHost)
                .servicePort(segmentStorePort)
                .containerCount(containerCount)
                .restPort(-1)
                .enableAuth(authEnabled)
                .passwordAuthHandlerInputFilePath(passwordInputFilePath)
                .tokenSigningKey(tokenSigningKeyBasis)
                .isRGWritesWithReadPermEnabled(rgWritesWithReadPermEnabled)
                .accessTokenTtlInSeconds(tokenTtlInSeconds)
                .enableTls(tlsEnabled)
                .serverCertificatePath(tlsServerCertificatePath)
                .serverKeyPath(tlsServerKeyPath)
                .serverKeystorePath(tlsServerKeystorePath)
                .serverKeystorePasswordPath(tlsServerKeystorePasswordPath)
                .build();
    }

    public String controllerUri() {
        return String.format("%s://localhost:%d", isTlsEnabled() ? "tls" : "tcp", controllerPort);
    }

    private Entry defaultAuthHandlerEntry() {
        StrongPasswordProcessor passwordProcessor = StrongPasswordProcessor.builder().build();
        try {
            String encryptedPassword = passwordProcessor.encryptPassword("1111_aaaa");
            return Entry.of("admin", encryptedPassword, "prn::*,READ_UPDATE");
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new RuntimeException(e);
        }
    }

    private File createAuthFile(List<PasswordAuthHandlerInput.Entry> entries) {
        PasswordAuthHandlerInput result = new PasswordAuthHandlerInput(
                UUID.randomUUID().toString(), ".txt");
        result.postEntries(entries);
        log.debug("Posted entries to [{}] file", result.getFile().getPath());
        return result.getFile();
    }
}
