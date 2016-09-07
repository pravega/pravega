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
import com.emc.pravega.cluster.zkutils.common.Endpoint;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

@Slf4j
public class ZookeeperClient extends CommonConfigSyncManager
        implements PathChildrenCacheListener, AutoCloseable {

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

        controllerCache = new PathChildrenCache(curatorFramework, this.CONTROLLER_INFO_ROOT, true);
        nodeCache = new PathChildrenCache(curatorFramework, this.NODE_INFO_ROOT, true);

        controllerCache.getListenable().addListener(this);
        nodeCache.getListenable().addListener(this);

        controllerCache.start();
        nodeCache.start();
    }


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


    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
        switch (event.getType()) {
            case CHILD_ADDED:
                if (event.getData().getPath().startsWith(this.CONTROLLER_INFO_ROOT)) {
                    listener.controllerAddedNotification(Endpoint.constructFrom(event.getData().getPath()));
                } else if (event.getData().getPath().startsWith(this.NODE_INFO_ROOT)) {
                    listener.nodeAddedNotification(Endpoint.constructFrom(event.getData().getPath()));
                }
                break;
            case CHILD_REMOVED:
                if (event.getData().getPath().startsWith(this.CONTROLLER_INFO_ROOT)) {
                    listener.controllerRemovedNotification(Endpoint.constructFrom(event.getData().getPath()));
                } else if (event.getData().getPath().startsWith(this.NODE_INFO_ROOT)) {
                    listener.nodeRemovedNotification(Endpoint.constructFrom(event.getData().getPath()));
                }
                break;
        }

    }


    @Override
    public void close() throws Exception {
        if (this.curatorFramework != null) {
            this.curatorFramework.close();
        }
    }
}
