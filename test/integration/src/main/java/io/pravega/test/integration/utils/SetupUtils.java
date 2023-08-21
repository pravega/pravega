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

import com.google.common.base.Preconditions;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.control.impl.Controller;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.control.impl.ControllerImplConfig;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReaderGroupConfig;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.ClientFactoryImpl;
import io.pravega.common.concurrent.ExecutorServiceHelpers;
import io.pravega.controller.util.Config;
import io.pravega.segmentstore.contracts.StreamSegmentStore;
import io.pravega.segmentstore.contracts.tables.TableStore;
import io.pravega.segmentstore.server.host.delegationtoken.PassingTokenVerifier;
import io.pravega.segmentstore.server.host.handler.AdminConnectionListener;
import io.pravega.segmentstore.server.host.handler.IndexAppendProcessor;
import io.pravega.segmentstore.server.host.handler.PravegaConnectionListener;
import io.pravega.segmentstore.server.host.stat.SegmentStatsRecorder;
import io.pravega.segmentstore.server.host.stat.TableSegmentStatsRecorder;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.shared.security.auth.DefaultCredentials;
import io.pravega.test.common.SecurityConfigDefaults;
import io.pravega.test.common.TestUtils;
import io.pravega.test.common.TestingServerStarter;

import java.net.URI;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Cleanup;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.test.TestingServer;

import static io.pravega.test.integration.utils.TestUtils.pathToConfig;

/**
 * Utility functions for creating the test setup.
 */
@Slf4j
@NotThreadSafe
public final class SetupUtils {
    
    // The different services.
    @Getter
    private ScheduledExecutorService executor = null;
    @Getter
    private Controller controller = null;
    @Getter
    private EventStreamClientFactory clientFactory = null;
    private ControllerWrapper controllerWrapper = null;
    private ServiceBuilder serviceBuilder = null;
    private PravegaConnectionListener server = null;
    private AdminConnectionListener adminListener = null;
    @Getter
    private TestingServer zkTestServer = null;

    // Manage the state of the class.
    private final AtomicBoolean started = new AtomicBoolean(false);

    // The test Scope name.
    @Getter
    private final String scope = "scope";
    private final int controllerRPCPort = TestUtils.getAvailableListenPort();
    private final int controllerRESTPort = TestUtils.getAvailableListenPort();
    @Getter
    private final int servicePort = TestUtils.getAvailableListenPort();
    @Getter
    private final int adminPort = TestUtils.getAvailableListenPort();
    private ClientConfig.ClientConfigBuilder clientConfigBuilder = ClientConfig.builder();

    /**
     * Returns the cli config for this instance.
     */
    public ClientConfig getClientConfig() {
        return clientConfigBuilder.build();
    }

    /**
     * Start all pravega related services required for the test deployment.
     *
     * @throws Exception on any errors.
     */
    public void startAllServices() throws Exception {
        startAllServices(null, false, false);
    }

    /**
     * Start all pravega related services required for the test deployment.
     *
     * @param numThreads the number of threads for the internal client threadpool.
     * @throws Exception on any errors.
     */
    public void startAllServices(Integer numThreads) throws Exception {
        startAllServices(null, false, false);
    }

    /**
     * Start all pravega related services required for the test deployment.
     *
     * @param enableAuth set to enale authentication
     * @param enableTls set to enable tls
     * @throws Exception on any errors.
     */
    public void startAllServices(boolean enableAuth, boolean enableTls) throws Exception {
        startAllServices(null, enableAuth, enableTls);
    }
    
    /**
     * Start all pravega related services required for the test deployment.
     *
     * @param numThreads the number of threads for the internal client threadpool.
     * @param enableAuth set to enale authentication
     * @param enableTls set to enable tls
     * @throws Exception on any errors.
     */
    public void startAllServices(Integer numThreads, boolean enableAuth, boolean enableTls) throws Exception {
        if (!this.started.compareAndSet(false, true)) {
            log.warn("Services already started, not attempting to start again");
            return;
        }

        if (enableAuth) {
            clientConfigBuilder = clientConfigBuilder.credentials(new DefaultCredentials(SecurityConfigDefaults.AUTH_ADMIN_PASSWORD,
                    SecurityConfigDefaults.AUTH_ADMIN_USERNAME));
        }

        if (enableTls) {
            clientConfigBuilder = clientConfigBuilder.trustStore(pathToConfig() + SecurityConfigDefaults.TLS_CA_CERT_FILE_NAME)
                    .controllerURI(URI.create("tls://localhost:" + controllerRPCPort))
                    .validateHostName(false);
        } else {
            clientConfigBuilder = clientConfigBuilder.controllerURI(URI.create("tcp://localhost:" + controllerRPCPort));
        }

        this.executor = ExecutorServiceHelpers.newScheduledThreadPool(2, "Controller pool");
        this.controller = new ControllerImpl(ControllerImplConfig.builder().clientConfig(getClientConfig()).build(),
                                             executor);
        this.clientFactory = new ClientFactoryImpl(scope, controller, getClientConfig());
        
        // Start zookeeper.
        this.zkTestServer = new TestingServerStarter().start();
        this.zkTestServer.start();

        // Start Pravega Service.
        this.serviceBuilder = ServiceBuilder.newInMemoryBuilder(ServiceBuilderConfig.getDefaultConfig());

        this.serviceBuilder.initialize();
        StreamSegmentStore store = serviceBuilder.createStreamSegmentService();
        TableStore tableStore = serviceBuilder.createTableStoreService();
        IndexAppendProcessor indexAppendProcessor = new IndexAppendProcessor(serviceBuilder.getLowPriorityExecutor(), store);
        this.server = new PravegaConnectionListener(enableTls, false, "localhost",
                servicePort, store, tableStore, SegmentStatsRecorder.noOp(), TableSegmentStatsRecorder.noOp(),  new PassingTokenVerifier(),
                pathToConfig() + SecurityConfigDefaults.TLS_SERVER_CERT_FILE_NAME,
                pathToConfig() + SecurityConfigDefaults.TLS_SERVER_PRIVATE_KEY_FILE_NAME, true,
                serviceBuilder.getLowPriorityExecutor(), SecurityConfigDefaults.TLS_PROTOCOL_VERSION, indexAppendProcessor);

        this.server.startListening();
        log.info("Started Pravega Service");

        this.adminListener = new AdminConnectionListener(enableTls, false, "localhost", adminPort,
                store, tableStore, new PassingTokenVerifier(), pathToConfig() + SecurityConfigDefaults.TLS_SERVER_CERT_FILE_NAME,
                pathToConfig() + SecurityConfigDefaults.TLS_SERVER_PRIVATE_KEY_FILE_NAME, SecurityConfigDefaults.TLS_PROTOCOL_VERSION, indexAppendProcessor);
        this.adminListener.startListening();
        log.info("AdminConnectionListener started successfully.");

        // Start Controller.
        this.controllerWrapper = new ControllerWrapper(
                this.zkTestServer.getConnectString(), false, true, controllerRPCPort,
                "localhost", servicePort, Config.HOST_STORE_CONTAINER_COUNT, controllerRESTPort, enableAuth,
                pathToConfig() + SecurityConfigDefaults.AUTH_HANDLER_INPUT_FILE_NAME,
                "secret", true, 600, enableTls, SecurityConfigDefaults.TLS_PROTOCOL_VERSION,
                pathToConfig() + SecurityConfigDefaults.TLS_SERVER_CERT_FILE_NAME,
                pathToConfig() + SecurityConfigDefaults.TLS_SERVER_PRIVATE_KEY_FILE_NAME,
                pathToConfig() + SecurityConfigDefaults.TLS_SERVER_KEYSTORE_NAME,
                pathToConfig() + SecurityConfigDefaults.TLS_PASSWORD_FILE_NAME);

        this.controllerWrapper.awaitRunning();
        this.controllerWrapper.getController().createScope(scope).get();
        log.info("Initialized Pravega Controller");
    }

    /**
     * Stop the pravega cluster and release all resources.
     *
     * @throws Exception on any errors.
     */
    public void stopAllServices() throws Exception {
        if (!this.started.compareAndSet(true, false)) {
            log.warn("Services not yet started or already stopped, not attempting to stop");
            return;
        }

        this.controllerWrapper.close();
        this.server.close();
        this.serviceBuilder.close();
        this.adminListener.close();
        this.zkTestServer.close();
        this.clientFactory.close();
        this.controller.close();
        this.executor.shutdown();
    }

    /**
     * Create the test stream.
     *
     * @param streamName     Name of the test stream.
     * @param numSegments    Number of segments to be created for this stream.
     *
     */
    public void createTestStream(final String streamName, final int numSegments) {
        Preconditions.checkState(this.started.get(), "Services not yet started");
        Preconditions.checkNotNull(streamName);
        Preconditions.checkArgument(numSegments > 0);

        @Cleanup
        StreamManager streamManager = StreamManager.create(getClientConfig());
        streamManager.createScope(scope);
        streamManager.createStream(scope, streamName,
                                   StreamConfiguration.builder()
                                                      .scalingPolicy(ScalingPolicy.fixed(numSegments))
                                                      .build());
        log.info("Created stream: " + streamName);
    }

    /**
     * Create a stream writer for writing Integer events.
     *
     * @param streamName    Name of the test stream.
     *
     * @return Stream writer instance.
     */
    public EventStreamWriter<Integer> getIntegerWriter(final String streamName) {
        Preconditions.checkState(this.started.get(), "Services not yet started");
        Preconditions.checkNotNull(streamName);

        return clientFactory.createEventWriter(streamName, new IntegerSerializer(),
                                               EventWriterConfig.builder().build());
    }

    /**
     * Create a stream reader for reading Integer events.
     *
     * @param streamName         Name of the test stream.
     * @param readerGroupManager RGM.
     * @return Stream reader instance.
     */
    public EventStreamReader<Integer> getIntegerReader(final String streamName, ReaderGroupManager readerGroupManager) {
        Preconditions.checkState(this.started.get(), "Services not yet started");
        Preconditions.checkNotNull(streamName);
        Preconditions.checkNotNull(readerGroupManager);

        final String readerGroup = "testReaderGroup" + scope + streamName;
        readerGroupManager.createReaderGroup(
                readerGroup,
                ReaderGroupConfig.builder().stream(Stream.of(scope, streamName)).build());

        final String readerGroupId = UUID.randomUUID().toString();
        return clientFactory.createReader(readerGroupId, readerGroup, new IntegerSerializer(),
                ReaderConfig.builder().build());
    }

    public ReaderGroupManager createReaderGroupManager(final String streamName) {
        Preconditions.checkState(this.started.get(), "Services not yet started");
        Preconditions.checkNotNull(streamName);

        return ReaderGroupManager.withScope(scope, getClientConfig());
    }

    public URI getControllerUri() {
        return getClientConfig().getControllerURI();
    }

    public URI getControllerRestUri() {
        return URI.create("http://localhost:" + controllerRESTPort);
    }
}
