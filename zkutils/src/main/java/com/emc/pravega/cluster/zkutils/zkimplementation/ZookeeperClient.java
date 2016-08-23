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
import com.emc.pravega.cluster.zkutils.abstraction.ConfigSyncManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class ZookeeperClient
        implements ConfigSyncManager, PathChildrenCacheListener {

    private final ConfigChangeListener listener;
    private final CuratorFramework curatorFramework;
    private final PathChildrenCache controllerCache;
    private final PathChildrenCache nodeCache;


    public ZookeeperClient(String connectString, int sessionTimeoutMS, ConfigChangeListener listener) throws Exception {
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

        this.listener = listener;
    }

    /**
     * Place in the configuration manager where the data about live nodes is stored.
     *
     */
    String nodeInfoRoot = "/pravega/nodes";
    String controllerInfoRoot = "/pravega/controllers";

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

    @Override
    public void registerPravegaNode(String host, int port, String jsonMetadata) throws Exception {
        Map jsonMap = new HashMap<String,Object>();
        jsonMap.put ("host" ,host);
        jsonMap.put ("port" , port);
        jsonMap.put ("metadata", jsonMetadata);

        createEntry(nodeInfoRoot + "/" + host + ":" + port, jsonEncode(jsonMap).getBytes());
    }

    @Override
    public void registerPravegaController(String host, int port, String jsonMetadata) throws Exception {
        Map jsonMap = new HashMap<String,Object>();
        jsonMap.put ("host" ,host);
        jsonMap.put ("port" , port);
        jsonMap.put ("metadata", jsonMetadata);

        createEntry(controllerInfoRoot + "/" + host + ":" + port, jsonEncode(jsonMap).getBytes());
    }

    @Override
    public void unregisterPravegaController(String host, int port) throws Exception {
        deleteEntry(controllerInfoRoot + "/" + host + ":" + port);
    }

    @Override
    public void unregisterPravegaNode(String host, int port) throws Exception {
        deleteEntry(nodeInfoRoot + "/" + host + ":" + port);

    }

    String jsonEncode(Object obj ) {
        final String retVal ="";

        if (obj instanceof Map) {
            retVal.concat("{");
            ((Map)obj).forEach((k,v) -> {
                retVal.concat(jsonEncode(k)+ ":" + jsonEncode(v));
            });
            retVal.concat("}");
        } else if (obj instanceof String) {
            retVal.concat("\"" + obj + "\"");
        } else {
            retVal.concat(obj.toString());
        }

        return retVal;
    }

    /**
     * Called when a background task has completed or a watch has triggered
     *
     * @param client client
     * @param event  the event
     * @throws Exception any errors
    @Override
    public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
        //TODO: Handle events

    }
     */

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
}
