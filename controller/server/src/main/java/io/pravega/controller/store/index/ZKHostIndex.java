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
package io.pravega.controller.store.index;

import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * Zookeeper based host index.
 */
@Slf4j
public class ZKHostIndex implements HostIndex {
    private final CuratorFramework client;
    private final Executor executor;
    private final String hostRoot;

    public ZKHostIndex(CuratorFramework client, String hostRoot, Executor executor) {
        this.client = client;
        this.executor = executor;
        this.hostRoot = hostRoot;
    }

    @Override
    public CompletableFuture<Void> addEntity(final String hostId, final String entity) {
        return addEntity(hostId, entity, new byte[0]);
    }

    @Override
    public CompletableFuture<Void> addEntity(String hostId, String entity, byte[] entityData) {
        return CompletableFuture.supplyAsync(() -> {
            Preconditions.checkNotNull(hostId);
            Preconditions.checkNotNull(entity);

            try {

                client.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .forPath(getHostPath(hostId, entity), entityData);

                return null;

            } catch (KeeperException.NodeExistsException e) {
                log.debug("Node {} exists.", getHostPath(hostId, entity));
                return null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, executor);
    }

    @Override
    public CompletableFuture<Void> removeEntity(final String hostId, final String entity, final boolean deleteEmptyHost) {
        return CompletableFuture.supplyAsync(() -> {
            Preconditions.checkNotNull(hostId);
            Preconditions.checkNotNull(entity);

            try {
                client.delete()
                        .forPath(getHostPath(hostId, entity));

                if (deleteEmptyHost) {
                    // if there are no children for the failed host, remove failed host znode
                    Stat stat = new Stat();
                    client.getData()
                            .storingStatIn(stat)
                            .forPath(getHostPath(hostId));

                    if (stat.getNumChildren() == 0) {
                        client.delete()
                                .withVersion(stat.getVersion())
                                .forPath(getHostPath(hostId));
                    }
                }
                return null;
            } catch (KeeperException.NoNodeException e) {
                log.debug("Node {} does not exist.", entity);
                return null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, executor);
    }

    @Override
    public CompletableFuture<Void> removeHost(final String hostId) {
        return CompletableFuture.supplyAsync(() -> {
            Preconditions.checkNotNull(hostId);

            try {

                client.delete().forPath(getHostPath(hostId));
                return null;

            } catch (KeeperException.NoNodeException e) {
                log.debug("Node {} does not exist.", getHostPath(hostId));
                return null;
            } catch (KeeperException.NotEmptyException e) {
                log.debug("Node {} not empty.", getHostPath(hostId));
                return null;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, executor);
    }

    @Override
    public CompletableFuture<Optional<String>> getRandomEntity(final String hostId) {
        return CompletableFuture.supplyAsync(() -> {
            Preconditions.checkNotNull(hostId);

            try {

                List<String> children = client.getChildren().forPath(getHostPath(hostId));
                if (children.isEmpty()) {
                    return Optional.empty();
                } else {
                    Random random = new Random();
                    return Optional.of(children.get(random.nextInt(children.size())));
                }

            } catch (KeeperException.NoNodeException e) {
                log.debug("Node {} does not exist.", getHostPath(hostId));
                return Optional.empty();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, executor);
    }

    @Override
    public CompletableFuture<Set<String>> getHosts() {
        return CompletableFuture.supplyAsync(() -> {
            try {
                List<String> children = client.getChildren().forPath(hostRoot);
                return children.stream().collect(Collectors.toSet());
            } catch (KeeperException.NoNodeException e) {
                return Collections.emptySet();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }, executor);
    }

    private String getHostPath(final String hostId, final String child) {
        return hostRoot + "/" + hostId + "/" + child;
    }

    private String getHostPath(final String hostId) {
        return hostRoot + "/" + hostId;
    }

}
