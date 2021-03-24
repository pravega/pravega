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
package io.pravega.segmentstore.server.host;

import io.pravega.segmentstore.server.store.ServiceBuilderConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperConfig;
import io.pravega.segmentstore.storage.impl.bookkeeper.BookKeeperServiceRunner;
import io.pravega.test.common.TestUtils;
import java.util.ArrayList;
import javax.annotation.concurrent.NotThreadSafe;
import lombok.Getter;
import lombok.val;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * Helper class for Segment store integration tests.
 * This class sets up the bookkeeper and zookeeper and sets up the correct values in config.
 */
@NotThreadSafe
public class BookKeeperRunner implements AutoCloseable {

    private final ServiceBuilderConfig.Builder configBuilder;
    private final int bookieCount;
    private BookKeeperServiceRunner bkRunner;
    @Getter
    private CuratorFramework zkClient;

    public BookKeeperRunner(ServiceBuilderConfig.Builder configBuilder, int bookieCount) {
        this.configBuilder = configBuilder;
        this.bookieCount = bookieCount;
    }

    public void initialize() throws Exception {
        // BookKeeper
        // Pick random ports to reduce chances of collisions during concurrent test executions.
        int zkPort = TestUtils.getAvailableListenPort();
        val bookiePorts = new ArrayList<Integer>();
        for (int i = 0; i < this.bookieCount; i++) {
            bookiePorts.add(TestUtils.getAvailableListenPort());
        }

        this.bkRunner = BookKeeperServiceRunner.builder()
                                               .startZk(true)
                                               .zkPort(zkPort)
                                               .secureZK(false)
                                               .secureBK(false)
                                               .ledgersPath("/ledgers/" + zkPort)
                                               .bookiePorts(bookiePorts)
                                               .build();
        this.bkRunner.startAll();

        // Create a ZKClient with a base namespace.
        String baseNamespace = "pravega/" + zkPort;
        this.zkClient = CuratorFrameworkFactory
                .builder()
                .connectString("localhost:" + zkPort)
                .namespace(baseNamespace)
                .retryPolicy(new ExponentialBackoffRetry(1000, 5))
                .connectionTimeoutMs(5000)
                .sessionTimeoutMs(5000)
                .build();
        this.zkClient.start();

        // Attach a sub-namespace for the Container Metadata.
        String logMetaNamespace = "segmentstore/containers";
        this.configBuilder.include(BookKeeperConfig
                .builder()
                .with(BookKeeperConfig.ZK_ADDRESS, "localhost:" + zkPort)
                .with(BookKeeperConfig.ZK_METADATA_PATH, logMetaNamespace)
                .with(BookKeeperConfig.BK_LEDGER_PATH, "/ledgers/" + zkPort)
                .with(BookKeeperConfig.BK_ACK_QUORUM_SIZE, this.bookieCount)
                .with(BookKeeperConfig.BK_WRITE_QUORUM_SIZE, this.bookieCount)
                .with(BookKeeperConfig.BK_ENSEMBLE_SIZE, this.bookieCount));
    }

    @Override
    public void close() throws Exception {
        // BookKeeper
        val bk = this.bkRunner;
        if (bk != null) {
            bk.close();
            this.bkRunner = null;
        }

        val zk = this.zkClient;
        if (zk != null) {
            zk.close();
            this.zkClient = null;
        }
    }
}
