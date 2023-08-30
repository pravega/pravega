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
package io.pravega.test.integration.utils;

import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.segmentstore.server.host.handler.IndexAppendProcessor;
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
import io.pravega.test.common.SecurityConfigDefaults;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;
import io.pravega.shared.security.auth.PasswordAuthHandlerInput;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;

import java.io.File;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.time.Duration;
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

    /**
     * Represents the port on which the Controller REST API listens.
     *
     * We don't want to enable Controller REST APIs by default, as most tests do not exercise them. While the port
     * can be set directly via the builder method, it is recommended that callers use {@code controllerRestEnabled}
     * instead, which results in dynamic assignment of this port.
     */
    @Getter
    @Builder.Default
    private int controllerRestPort = -1;

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
    @Builder.Default
    private String[] tlsProtocolVersion = SecurityConfigDefaults.TLS_PROTOCOL_VERSION;

    @Builder.Default
    private boolean controllerRestEnabled = false;

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

    @Getter
    @Builder.Default
    private Duration accessTokenTtl = Duration.ofSeconds(300);

    private ClusterWrapper() {}

    @SneakyThrows
    public void start() {
        if (this.isAuthEnabled() && passwordAuthHandlerEntries == null) {
            this.passwordAuthHandlerEntries = Arrays.asList(defaultAuthHandlerEntry());
        }
        if (this.controllerRestEnabled && this.controllerRestPort <= 0) {
            this.controllerRestPort = TestUtils.getAvailableListenPort();
        }
        startZookeeper();
        log.info("Started Zookeeper");
        startSegmentStore();
        log.info("Started Segment store");
        startController();
        log.info("Started Controller");
    }

    @Override
    public void close() {
        ExecutorServiceHelpers.shutdown(executor);
        try {
            controllerServerWrapper.close();
            segmentStoreServer.close();
            serviceBuilder.close();
            zookeeperServer.close();
            if (passwordInputFile != null && passwordInputFile.exists()) {
                passwordInputFile.delete();
            }
        } catch (Exception e) {
            log.warn(e.getMessage(), e);
            // ignore
        }
    }

    /**
     * Returns the Zookeeper Server's connection string in the format: `ip-address:port`.
     *
     * @return Zookeeper connection string. Example: "127.0.0.1:33051".
     */
    public String zookeeperConnectString() {
        return this.zookeeperServer != null ? this.zookeeperServer.getConnectString() : "";
    }

    public String controllerUri() {
        return String.format("%s://localhost:%d", isTlsEnabled() ? "tls" : "tcp", controllerPort);
    }

    public String controllerRestUri() {
        return String.format("%s://localhost:%d", isTlsEnabled() ? "https" : "http", controllerRestPort);
    }

    private void startZookeeper() throws Exception {
        zookeeperServer = new TestingServerStarter().start();
    }

    private void startController() {
        // Create and start controller service
        controllerServerWrapper = createControllerWrapper();
        controllerServerWrapper.awaitRunning();
    }

    private void startSegmentStore() throws DurableDataLogException {
        serviceBuilder = createServiceBuilder();
        serviceBuilder.initialize();

        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();

        if (authEnabled) {
            passwordInputFile = createAuthFile(this.passwordAuthHandlerEntries);
        }
        IndexAppendProcessor indexAppendProcessor = new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store);
        segmentStoreServer = new PravegaConnectionListener(this.tlsEnabled, false, "localhost", segmentStorePort, store, tableStore,
            SegmentStatsRecorder.noOp(), TableSegmentStatsRecorder.noOp(),
            authEnabled ? new TokenVerifierImpl(tokenSigningKeyBasis) : null,
            this.tlsServerCertificatePath, this.tlsServerKeyPath, true, serviceBuilder.getLowPriorityExecutor(),
                tlsProtocolVersion, indexAppendProcessor);

        segmentStoreServer.startListening();
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
                .restPort(controllerRestPort)
                .enableAuth(authEnabled)
                .passwordAuthHandlerInputFilePath(passwordInputFilePath)
                .tokenSigningKey(tokenSigningKeyBasis)
                .isRGWritesWithReadPermEnabled(rgWritesWithReadPermEnabled)
                .accessTokenTtlInSeconds(tokenTtlInSeconds)
                .enableTls(tlsEnabled)
                .tlsProtocolVersion(tlsProtocolVersion)
                .serverCertificatePath(tlsServerCertificatePath)
                .serverKeyPath(tlsServerKeyPath)
                .serverKeystorePath(tlsServerKeystorePath)
                .serverKeystorePasswordPath(tlsServerKeystorePasswordPath)
                .build();
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
