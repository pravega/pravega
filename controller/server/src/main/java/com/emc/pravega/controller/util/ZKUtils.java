/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.util;

import com.google.common.base.Preconditions;

import lombok.extern.slf4j.Slf4j;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.KeeperException;

/**
 * Helper ZK functions.
 */
@Slf4j
public final class ZKUtils {

    /**
     * Helper utility to lazily create and fetch only one instance of the Curator client to be used by the controller.
     */
    private enum CuratorSingleton {
        CURATOR_INSTANCE;

        //Single instance of the curator client which we want to be used in all of the controller code.
        private final CuratorFramework zkClient;

        CuratorSingleton() {
            //Create and initialize the curator client framework.
            zkClient = CuratorFrameworkFactory.builder()
                    .connectString(Config.ZK_URL)
                    .namespace("pravega/" + Config.CLUSTER_NAME)
                    .retryPolicy(new ExponentialBackoffRetry(Config.ZK_RETRY_SLEEP_MS, Config.ZK_MAX_RETRIES))
                    .build();
            zkClient.start();
        }
    }
    
    public static CuratorFramework getCuratorClient() {
        return CuratorSingleton.CURATOR_INSTANCE.zkClient;
    }

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

}
