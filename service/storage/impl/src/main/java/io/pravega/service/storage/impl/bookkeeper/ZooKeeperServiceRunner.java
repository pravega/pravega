/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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

package io.pravega.service.storage.impl.bookkeeper;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReference;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.util.LocalBookKeeper;
import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

/**
 * Helps run ZooKeeper Server in process.
 */
@RequiredArgsConstructor
@Slf4j
public class ZooKeeperServiceRunner implements AutoCloseable {
    private static final InetAddress LOOPBACK_ADDRESS = InetAddress.getLoopbackAddress();
    private final AtomicReference<ZooKeeperServer> server = new AtomicReference<>();
    private final int zkPort;

    @Override
    public void close() throws Exception {
        if (this.server.get() != null) {
            this.server.get().shutdown();
        }
    }

    /**
     * Starts the ZooKeeper Service in process.
     *
     * @throws Exception If an exception occurred.
     */
    public void start() throws Exception {
        val tmpDir = IOUtils.createTempDir("zookeeper", "inproc");
        tmpDir.deleteOnExit();

        val s = new ZooKeeperServer(tmpDir, tmpDir, ZooKeeperServer.DEFAULT_TICK_TIME);
        this.server.set(s);
        val serverFactory = new NIOServerCnxnFactory();

        val address = LOOPBACK_ADDRESS.getHostAddress() + ":" + this.zkPort;
        log.info("Starting Zookeeper server at " + address + " ...");
        serverFactory.configure(new InetSocketAddress(LOOPBACK_ADDRESS, this.zkPort), 1000);
        serverFactory.startup(s);

        boolean b = LocalBookKeeper.waitForServerUp(address, LocalBookKeeper.CONNECTION_TIMEOUT);
        log.info("ZooKeeper server {}.", b ? "up" : "not up");
    }
}
