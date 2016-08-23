/**
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  * <p>
  * http://www.apache.org/licenses/LICENSE-2.0
  * <p>
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package com.emc.pravega.cluster.zkutils.zkimplementation;

import com.emc.pravega.cluster.zkutils.abstraction.ConfigChangeListener;
import com.emc.pravega.cluster.zkutils.common.CommonConfigSyncManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import java.io.Closeable;

@Slf4j
public class ZookeeperClient extends CommonConfigSyncManager
        implements PathChildrenCacheListener, AutoCloseable{

    private final CuratorFramework curatorFramework;
    private final PathChildrenCache controllerCache;
    private final PathChildrenCache nodeCache;


    public ZookeeperClient(String connectString, int sessionTimeoutMS, ConfigChangeListener listener) throws Exception {
        super(listener);
        curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(connectString)
                .sessionTimeoutMs(sessionTimeoutMS)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
        curatorFramework.start();

        controllerCache = new PathChildrenCache(curatorFramework, this.controllerInfoRoot, true);
        nodeCache = new PathChildrenCache(curatorFramework, this.nodeInfoRoot, true);

        controllerCache.getListenable().addListener(this);
        nodeCache.getListenable().addListener(this);

        controllerCache.start();
        nodeCache.start();
    }

    /**
     * Sample configuration/synchronization methods. Will add more as implementation progresses
     *  @param path
     * @param value
     */
    @Override
    public void createEntry(String path, byte[] value) throws Exception {
            curatorFramework.create().withMode(CreateMode.EPHEMERAL).forPath(path, value);
    }

    @Override
    public void deleteEntry(String path) throws Exception {
        curatorFramework.delete().forPath(path);

    }

    @Override
    public void refreshCluster() throws Exception {
        this.nodeCache.clearAndRefresh();
        this.controllerCache.clearAndRefresh();
    }

    /**
     * Called when a change has occurred
     *
     * @param client the client
     * @param event  describes the change
     * @throws Exception errors
     */
    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        switch(event.getType()) {
            case CHILD_ADDED:
                if(event.getData().getPath().startsWith(this.controllerInfoRoot)) {
                    String path = event.getData().getPath();
                    path = path.substring((this.controllerInfoRoot+"/").length());
                    String[] parts = path.split(":");
                    listener.controllerAddedNotification(parts[0], Integer.valueOf(parts[1]));
                } else if (event.getData().getPath().startsWith(this.nodeInfoRoot)){
                    String path = event.getData().getPath();
                    path = path.substring((this.controllerInfoRoot+"/").length());
                    String[] parts = path.split(":");
                    listener.nodeAddedNotification(parts[0], Integer.valueOf(parts[1]));
                }
                break;
            case CHILD_REMOVED:
                if(event.getData().getPath().startsWith(this.controllerInfoRoot)) {
                    String path = event.getData().getPath();
                    path = path.substring((this.controllerInfoRoot+"/").length());
                    String[] parts = path.split(":");
                    listener.controllerRemovedNotification(parts[0], Integer.valueOf(parts[1]));
                } else if (event.getData().getPath().startsWith(this.nodeInfoRoot)){
                    String path = event.getData().getPath();
                    path = path.substring((this.controllerInfoRoot+"/").length());
                    String[] parts = path.split(":");
                    listener.nodeRemovedNotification(parts[0], Integer.valueOf(parts[1]));
                }
                break;
        }

    }

    /**
     * Closes this resource, relinquishing any underlying resources.
     * This method is invoked automatically on objects managed by the
     * {@code try}-with-resources statement.
     * <p>
     * <p>While this interface method is declared to throw {@code
     * Exception}, implementers are <em>strongly</em> encouraged to
     * declare concrete implementations of the {@code close} method to
     * throw more specific exceptions, or to throw no exception at all
     * if the close operation cannot fail.
     * <p>
     * <p> Cases where the close operation may fail require careful
     * attention by implementers. It is strongly advised to relinquish
     * the underlying resources and to internally <em>mark</em> the
     * resource as closed, prior to throwing the exception. The {@code
     * close} method is unlikely to be invoked more than once and so
     * this ensures that the resources are released in a timely manner.
     * Furthermore it reduces problems that could arise when the resource
     * wraps, or is wrapped, by another resource.
     * <p>
     * <p><em>Implementers of this interface are also strongly advised
     * to not have the {@code close} method throw {@link
     * InterruptedException}.</em>
     * <p>
     * This exception interacts with a thread's interrupted status,
     * and runtime misbehavior is likely to occur if an {@code
     * InterruptedException} is {@linkplain Throwable#addSuppressed
     * suppressed}.
     * <p>
     * More generally, if it would cause problems for an
     * exception to be suppressed, the {@code AutoCloseable.close}
     * method should not throw it.
     * <p>
     * <p>Note that unlike the {@link Closeable#close close}
     * method of {@link Closeable}, this {@code close} method
     * is <em>not</em> required to be idempotent.  In other words,
     * calling this {@code close} method more than once may have some
     * visible side effect, unlike {@code Closeable.close} which is
     * required to have no effect if called more than once.
     * <p>
     * However, implementers of this interface are strongly encouraged
     * to make their {@code close} methods idempotent.
     *
     * @throws Exception if this resource cannot be closed
     */
    @Override
    public void close() throws Exception {
        if(this.curatorFramework != null) {
            this.curatorFramework.close();
        }
    }
}
