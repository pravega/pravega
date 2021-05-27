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

package io.pravega.cli.admin.utils;

import com.google.common.annotations.VisibleForTesting;
import io.pravega.common.cluster.Host;
import io.pravega.common.cluster.HostContainerMap;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Zookeeper helper functions.
 */
public class ZKHelper implements AutoCloseable {

    // region constants

    private static final String BASE_NAMESPACE = "pravega";
    private static final String BK_PATH = "/bookkeeper/ledgers/available";
    private static final String CONTROLLER_PATH = "/cluster/controllers";
    private static final String SEGMENTSTORE_PATH = "/cluster/hosts";
    private static final String HOST_MAP_PATH = "/cluster/segmentContainerHostMapping";
    private static final String SEPARATOR = "/";

    // endregion

    // region instance variables

    private CuratorFramework zkClient;

    // endregion

    // region constructor

    /**
     * Create a new instance of the ZKHelper class.
     * @param zkURL The address of this helper instance connect to.
     * @throws ZKConnectionFailedException If cannot connect to the given address.
     */
    private ZKHelper(String zkURL, String clusterName) throws ZKConnectionFailedException {
        createZKClient(zkURL, clusterName);
    }

    // endregion

    /**
     * Get the list of controllers in the cluster.
     * @return A list of controllers.
     */
    public List<String> getControllers() {
        return getChild(CONTROLLER_PATH);
    }

    /**
     * Get the list of segment stores in the cluster.
     * @return A list of segment stores.
     */
    public List<String> getSegmentStores() {
        return getChild(SEGMENTSTORE_PATH);
    }

    /**
     * Get the list of bookies in the cluster.
     * @return A list of bookies.
     */
    public List<String> getBookies() {
        List<String> bookies = getChild(BK_PATH);
        return bookies != null ? bookies.stream().filter(s -> s.contains(":")).collect(Collectors.toList()) : null;
    }

    /**
     * Get the host to container map.
     * @return A map from segment store host to containers it holds.
     */
    public Map<Host, Set<Integer>> getCurrentHostMap() {
        return HostContainerMap.fromBytes(getData(HOST_MAP_PATH)).getHostContainerMap();
    }

    /**
     * Get the host with given container.
     * @param containerId The target container's id.
     * @return Host of the container.
     */
    public Optional<Host> getHostForContainer(int containerId) {
        Map<Host, Set<Integer>> mapping = getCurrentHostMap();
        return mapping.entrySet().stream()
                                 .filter(x -> x.getValue().contains(containerId))
                                 .map(Map.Entry::getKey)
                                 .findAny();

    }

    /**
     * Create a new instance of the ZKHelper class.
     * @param zkURL The address of this helper instance connect to.
     * @param clusterName The name of the Zookeeper cluster.
     * @throws ZKConnectionFailedException If cannot connect to the given address.
     * @return The new ZKHelper instance.
     */
    public static ZKHelper create(String zkURL, String clusterName) throws ZKConnectionFailedException {
        return new ZKHelper(zkURL, clusterName);
    }

    /**
     * Get the list of children of a zookeeper node.
     * @param path The path of the target zookeeper node.
     * @return The list of its child nodes' name.
     */
    @VisibleForTesting
    List<String> getChild(String path) {
        List<String> ret = null;
        try {
            ret = zkClient.getChildren().forPath(path);
        } catch (Exception e) {
            System.err.println("An error occurred executing getChild against Zookeeper: " + e.getMessage());
        }
        return ret;
    }

    /**
     * Get the data stored in the zookeeper node.
     * @param path The path of the target zookeeper node.
     * @return The data as byte array stored in the target zookeeper node.
     */
    @VisibleForTesting
    byte[] getData(String path) {
        byte[] ret = null;
        try {
            ret = zkClient.getData().forPath(path);
        } catch (Exception e) {
            System.err.println("An error occurred executing getData against Zookeeper: " + e.getMessage());
        }
        return ret;
    }

    /**
     * Create a curator framework's zookeeper client with given address.
     * @param zkURL The zookeeper address to connect.
     * @param clusterName The name of the Zookeeper cluster.
     * @throws ZKConnectionFailedException If cannot connect to the zookeeper with given address.
     */
    private void createZKClient(String zkURL, String clusterName) throws ZKConnectionFailedException {
        zkClient = CuratorFrameworkFactory
                .builder()
                .connectString(zkURL)
                .namespace(BASE_NAMESPACE + SEPARATOR + clusterName)
                .retryPolicy(new ExponentialBackoffRetry(2000, 2))
                .build();

        startZKClient();
    }

    /**
     * Start the zookeeper client.
     * @throws ZKConnectionFailedException If cannot connect to the zookeeper wothc given address.
     */
    private void startZKClient() throws ZKConnectionFailedException {
        zkClient.start();
        try {
            if (!zkClient.blockUntilConnected(5, TimeUnit.SECONDS)) {
                throw new ZKConnectionFailedException();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Close the ZKHelper.
     */
    public void close() {
        zkClient.close();
    }
}
