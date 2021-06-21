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
package io.pravega.test.integration.selftest.adapters;

import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.KeyValueTableFactory;
import io.pravega.client.admin.KeyValueTableManager;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.shared.security.auth.DefaultCredentials;
import io.pravega.common.Exceptions;
import io.pravega.common.util.Retry;
import io.pravega.test.common.SecurityConfigDefaults;
import io.pravega.test.integration.selftest.TestConfig;
import java.net.URI;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import lombok.SneakyThrows;

/**
 * StoreAdapter for tests that target a Pravega Cluster out-of-process. This uses a real Client, Controller and StreamManager.
 * This class does not create or destroy Pravega Clusters; it just provides a means of accessing them. Derived classes
 * may choose to start such clusters or connect to existing ones.
 */
class ExternalAdapter extends ClientAdapterBase {
    //region Members

    private final AtomicReference<StreamManager> streamManager;
    private final AtomicReference<EventStreamClientFactory> clientFactory;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the ExternalAdapter class.
     *
     * @param testConfig   The TestConfig to use.
     * @param testExecutor An Executor to use for async client-side operations.
     */
    ExternalAdapter(TestConfig testConfig, ScheduledExecutorService testExecutor) {
        super(testConfig, testExecutor);
        this.streamManager = new AtomicReference<>();
        this.clientFactory = new AtomicReference<>();
    }

    //endregion

    //region ClientAdapterBase and StorageAdapter Implementation

    @Override
    protected void startUp() throws Exception {
        try {
            URI controllerUri = new URI(getControllerUrl());

            // Create Stream Manager, Scope and Client Factory.
            this.streamManager.set(StreamManager.create(ClientConfig.builder()
                            .trustStore(String.format("../../config/%s", SecurityConfigDefaults.TLS_CA_CERT_FILE_NAME))
                            .credentials(new DefaultCredentials("1111_aaaa", "admin"))
                    .validateHostName(false)
                    .controllerURI(controllerUri)
                    .build()));
            Retry.withExpBackoff(500, 2, 10)
                 .retryWhen(ex -> true)
                 .run(() -> this.streamManager.get().createScope(SCOPE));

            // Create Client Factory.
            this.clientFactory.set(EventStreamClientFactory.withScope(SCOPE, ClientConfig.builder()
                    .trustStore(String.format("../../config/%s", SecurityConfigDefaults.TLS_CA_CERT_FILE_NAME))
                    .credentials(new DefaultCredentials("1111_aaaa", "admin"))
                    .validateHostName(false)
                    .controllerURI(controllerUri).build()));

            // Create, Seal and Delete a dummy segment - this verifies that the client is properly setup and that all the
            // components are running properly.
            String testStreamName = "Ping" + Long.toHexString(System.currentTimeMillis());
            this.streamManager.get().createStream(SCOPE, testStreamName, StreamConfiguration.builder().build());
            this.streamManager.get().sealStream(SCOPE, testStreamName);
            this.streamManager.get().deleteStream(SCOPE, testStreamName);
            log("Client initialized; using scope '%s'.", SCOPE);
        } catch (Throwable ex) {
            if (!Exceptions.mustRethrow(ex)) {
                close();
            }

            throw ex;
        }

        super.startUp();
    }

    @Override
    protected void shutDown() {
        super.shutDown();

        // Stop clients.
        stopComponent(this.clientFactory);
        stopComponent(this.streamManager);
    }

    @SneakyThrows(Exception.class)
    private void stopComponent(AtomicReference<? extends AutoCloseable> componentReference) {
        AutoCloseable p = componentReference.getAndSet(null);
        if (p != null) {
            p.close();
        }
    }

    @Override
    public boolean isFeatureSupported(Feature feature) {
        // Even though it does support it, Feature.RandomRead is not enabled because it currently has very poor performance.
        return feature == Feature.CreateStream
                || feature == Feature.Append
                || feature == Feature.TailRead
                || feature == Feature.Transaction
                || feature == Feature.SealStream
                || feature == Feature.DeleteStream;
    }

    @Override
    protected StreamManager getStreamManager() {
        return this.streamManager.get();
    }

    @Override
    protected EventStreamClientFactory getClientFactory() {
        return this.clientFactory.get();
    }

    @Override
    protected KeyValueTableManager getKVTManager() {
        throw new UnsupportedOperationException("getKVTManager is not supported for ExternalAdapter.");
    }

    @Override
    protected KeyValueTableFactory getKVTFactory() {
        throw new UnsupportedOperationException("getKVTFactory is not supported for ExternalAdapter.");
    }

    @Override
    protected String getControllerUrl() {
        return String.format((this.testConfig.isEnableSecurity() ? "ssl" : "tcp") + "://%s:%d", this.testConfig.getControllerHost(), this.testConfig.getControllerPort(0));
    }

    //endregion

}
