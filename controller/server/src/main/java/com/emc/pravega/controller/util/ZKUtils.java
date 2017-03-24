/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.util;

import com.google.common.base.Preconditions;

import lombok.extern.slf4j.Slf4j;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.concurrent.CountDownLatch;

/**
 * Helper ZK functions.
 */
@Slf4j
public final class ZKUtils {

    /**
     * Creates the znode if is doesn't already exist in zookeeper.
     *
     * @param client                The curator client to access zookeeper.
     * @param basePath              The znode path string.
     * @throws RuntimeException     If checking or creating path on zookeeper fails.
     */
    public static void createPathIfNotExists(final CuratorFramework client, final String basePath) {
        Preconditions.checkNotNull(client, "client");
        Preconditions.checkNotNull(basePath, "basePath");

        try {
            if (client.checkExists().forPath(basePath) == null) {
                client.create().creatingParentsIfNeeded().forPath(basePath);
            }
        } catch (KeeperException.NodeExistsException e) {
            log.debug("Path exists {}, ignoring exception", basePath);
        } catch (Exception e) {
            throw new RuntimeException("Exception while creating znode: " + basePath, e);
        }
    }

    /**
     * Creates the znode if is doesn't already exist in zookeeper.
     *
     * @param client                The curator client to access zookeeper.
     * @param basePath              The znode path string.
     * @param initData              Initialize the znode using the supplied data if not already created.
     * @throws RuntimeException     If checking or creating path on zookeeper fails.
     */
    public static void createPathIfNotExists(final CuratorFramework client, final String basePath,
            final byte[] initData) {
        Preconditions.checkNotNull(client, "client");
        Preconditions.checkNotNull(basePath, "basePath");
        Preconditions.checkNotNull(initData, "initData");

        try {
            if (client.checkExists().forPath(basePath) == null) {
                client.create().creatingParentsIfNeeded().forPath(basePath, initData);
            }
        } catch (KeeperException.NodeExistsException e) {
            log.debug("Path exists {}, ignoring exception", basePath);
        } catch (Exception e) {
            throw new RuntimeException("Exception while creating znode: " + basePath, e);
        }
    }

    /**
     * Simulates ZK session expiry. Refer https://wiki.apache.org/hadoop/ZooKeeper/FAQ#A4
     *
     * @param curatorClient Curator client object
     * @throws Exception Error fetching sessionId or sessionPassword from curator client
     */
    public static void simulateZkSessionExpiry(CuratorFramework curatorClient) throws Exception {
        final long sessionId = curatorClient.getZookeeperClient().getZooKeeper().getSessionId();
        final byte[] sessionPwd = curatorClient.getZookeeperClient().getZooKeeper().getSessionPasswd();
        CountDownLatch connectedLatch = new CountDownLatch(1);
        // Create a new ZK client with the same sessionId an sessionPwd as original one.
        ZooKeeper zk2 = new ZooKeeper(curatorClient.getZookeeperClient().getCurrentConnectionString(),
                5000, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connectedLatch.countDown();
            }
        }, sessionId, sessionPwd);
        // Await its connection.
        connectedLatch.await();
        // Close connection. This will cause original session to expire.
        zk2.close();
    }

}
