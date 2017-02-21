/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.store.stream;

import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.KeeperException;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class ZKScope implements Scope {

    private static final String SCOPE_PATH = "/store/%s";
    private static CuratorFramework client;
    private final String scopePath;
    private final String scopeName;

    protected ZKScope(final String scopeName) {
        this.scopeName = scopeName;
        scopePath = String.format(SCOPE_PATH, scopeName);
    }

    @Override
    public String getName() {
        return this.scopeName;
    }

    @Override
    public CompletableFuture<Void> createScope() {
        return addNode(scopePath);
    }

    @Override
    public CompletableFuture<Void> deleteScope() {
        return deleteNode(scopePath);
    }

    @Override
    public CompletableFuture<List<String>> listStreamsInScope() {
        return getChildren(scopePath);
    }

    @Override
    public void refresh() {
    }

    /**
     * Initialize the curator client.
     *
     * @param cf Curator framework client.
     */
    public static void initialize(final CuratorFramework cf) {
        client = cf;
    }

    /**
     * List Scopes in the cluster.
     *
     * @return A list of scopes.
     */
    public static CompletableFuture<List<String>> listScopes() {
        return getChildren("/store");
    }

    private static CompletableFuture<Void> addNode(final String path) {
        return CompletableFuture.runAsync(() -> {
            try {
                client.create().creatingParentsIfNeeded().forPath(path);
            } catch (KeeperException.NodeExistsException e) {
                throw new StoreException(StoreException.Type.NODE_EXISTS, e);
            } catch (Exception e) {
                throw new StoreException(StoreException.Type.UNKNOWN, e);
            }
        });
    }

    private static CompletableFuture<Void> deleteNode(final String path) {
        return CompletableFuture.runAsync(() -> {
            try {
                client.delete().forPath(path);
            } catch (KeeperException.NoNodeException e) {
                throw new StoreException(StoreException.Type.NODE_NOT_FOUND, e);
            } catch (KeeperException.NotEmptyException e) {
                throw new StoreException(StoreException.Type.NODE_NOT_EMPTY, e);
            } catch (Exception e) {
                throw new StoreException(StoreException.Type.UNKNOWN, e);
            }
        });
    }

    private static CompletableFuture<List<String>> getChildren(final String path) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return client.getChildren().forPath(path);
            } catch (KeeperException.NoNodeException e) {
                throw new StoreException(StoreException.Type.NODE_NOT_FOUND, e);
            } catch (Exception e) {
                throw new StoreException(StoreException.Type.UNKNOWN, e);
            }
        });
    }
}
