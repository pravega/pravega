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
package io.pravega.cli.admin.segmentstore;

import io.pravega.cli.admin.AdminCommandState;
import io.pravega.test.integration.utils.LocalServiceStarter;
import io.pravega.cli.admin.utils.TestUtils;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.common.io.FileHelpers;
import io.pravega.segmentstore.server.store.ServiceBuilder;
import io.pravega.segmentstore.storage.StorageFactory;
import io.pravega.segmentstore.storage.chunklayer.ChunkedSegmentStorageConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperLogFactory;
import io.pravega.storage.filesystem.FileSystemSimpleStorageFactory;
import io.pravega.storage.filesystem.FileSystemStorageConfig;
import io.pravega.test.common.ThreadPooledTestSuite;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.File;
import java.nio.file.Files;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class StorageCommandsTest extends ThreadPooledTestSuite {

    private static final String SCOPE = "testScope";
    // Setup utility.
    private static final AtomicReference<AdminCommandState> STATE = new AtomicReference<>();

    private static final Duration TIMEOUT = Duration.ofMillis(30 * 1000);
    @Rule
    public final Timeout globalTimeout = new Timeout(120, TimeUnit.SECONDS);

    private final ScalingPolicy scalingPolicy = ScalingPolicy.fixed(1);
    private final StreamConfiguration config = StreamConfiguration.builder().scalingPolicy(scalingPolicy).build();

    /**
     * A directory for FILESYSTEM storage as LTS.
     */
    private File baseDir = null;
    private StorageFactory storageFactory = null;

    /**
     * A directory for storing logs and CSV files generated during the test..
     */
    private File logsDir = null;
    private BookKeeperLogFactory factory = null;

    @Override
    protected int getThreadPoolSize() {
        return 10;
    }

    @Before
    public void setUp() throws Exception {
        this.baseDir = Files.createTempDirectory("TestStorage").toFile().getAbsoluteFile();
        this.logsDir = Files.createTempDirectory("Storage").toFile().getAbsoluteFile();
        FileSystemStorageConfig adapterConfig = FileSystemStorageConfig.builder()
                .with(FileSystemStorageConfig.ROOT, this.baseDir.getAbsolutePath())
                .with(FileSystemStorageConfig.REPLACE_ENABLED, true)
                .build();
        ChunkedSegmentStorageConfig config = ChunkedSegmentStorageConfig.DEFAULT_CONFIG;
        this.storageFactory = new FileSystemSimpleStorageFactory(config, adapterConfig, executorService());
    }

    @After
    public void tearDown() {
        STATE.get().close();
        if (this.factory != null) {
            this.factory.close();
        }
        FileHelpers.deleteFileOrDirectory(this.baseDir);
        FileHelpers.deleteFileOrDirectory(this.logsDir);
    }

    @Test
    public void testListChunksCommand() throws Exception {
        int instanceId = 0;
        int bookieCount = 3;
        int containerCount = 1;
        @Cleanup
        LocalServiceStarter.PravegaRunner pravegaRunner = new LocalServiceStarter.PravegaRunner(bookieCount, containerCount);
        pravegaRunner.startBookKeeperRunner(instanceId++);
        this.factory = new BookKeeperLogFactory(pravegaRunner.getBookKeeperRunner().getBkConfig().get(), pravegaRunner.getBookKeeperRunner().getZkClient().get(),
                executorService());
        pravegaRunner.startControllerAndSegmentStore(this.storageFactory, this.factory, true);
        String streamName = "testListChunksCommand";

        TestUtils.createScopeStream(pravegaRunner.getControllerRunner().getController(), SCOPE, streamName, config);
        try (val clientRunner = new TestUtils.ClientRunner(pravegaRunner.getControllerRunner(), SCOPE)) {
            // Write events to the streams.
            TestUtils.writeEvents(streamName, clientRunner.getClientFactory());
        }

        ServiceBuilder.ComponentSetup componentSetup = new ServiceBuilder.ComponentSetup(pravegaRunner.getSegmentStoreRunner().getServiceBuilder());
        for (int containerId = 0; containerId < containerCount; containerId++) {
            componentSetup.getContainerRegistry().getContainer(containerId).flushToStorage(TIMEOUT).join();
        }

        STATE.set(new AdminCommandState());
        Properties pravegaProperties = new Properties();
        pravegaProperties.setProperty("pravegaservice.admin.gateway.port", String.valueOf(pravegaRunner.getSegmentStoreRunner().getAdminPort()));
        pravegaProperties.setProperty("pravegaservice.container.count", "1");
        pravegaProperties.setProperty("pravegaservice.storage.impl.name", "FILESYSTEM");
        pravegaProperties.setProperty("pravegaservice.storage.layout", "CHUNKED_STORAGE");
        pravegaProperties.setProperty("filesystem.root", this.baseDir.getAbsolutePath());
        STATE.get().getConfigBuilder().include(pravegaProperties);

        String commandResult = TestUtils.executeCommand("storage list-chunks _system/containers/metadata_0 localhost", STATE.get());
        Assert.assertTrue(commandResult.contains("List of chunks for _system/containers/metadata_0"));
    }
}
