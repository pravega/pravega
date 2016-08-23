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

package com.emc.pravega.cluster.zkutils.dummy;


import com.emc.pravega.cluster.zkutils.abstraction.ConfigChangeListener;
import com.emc.pravega.cluster.zkutils.abstraction.ConfigSyncManager;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
  * Created by kandha on 8/8/16.
  */
public class DummyZK implements ConfigSyncManager {
    private final ConfigChangeListener listener;
    /**
     * Place in the configuration manager where the data about live nodes is stored.
     *
     */
    String nodeInfoRoot = "/pravega/nodes/";
    String controllerInfoRoot = "/pravega/controllers";

    ConcurrentHashMap<String,byte[]> valueMap;

    public DummyZK(String connectString, int sessionTimeout, ConfigChangeListener listener) {
        valueMap = new ConcurrentHashMap<>();
        this.listener = listener;
    }

    /**
     * Sample configuration/synchronization methods. Will add more as implementation progresses
     *  @param path
     * @param value
     */
    @Override
    public void createEntry(String path, byte[] value) {
        valueMap.put(path,value);
    }

    @Override
    public void deleteEntry(String path) {
        valueMap.remove(path);

    }

    @Override
    public void refreshCluster() {

    }

    @Override
    public void registerPravegaNode(String host, int port, String jsonMetadata) {
        Map jsonMap = new HashMap<String,Object>();
        jsonMap.put ("host" ,host);
        jsonMap.put ("port" , port);
        jsonMap.put ("metadata", jsonMetadata);

        createEntry(nodeInfoRoot + "/" + host + ":" + port, jsonEncode(jsonMap).getBytes());
        listener.nodeAddedNotification(host,port);

    }

    @Override
    public void registerPravegaController(String host, int port, String jsonMetadata) {
        Map jsonMap = new HashMap<String,Object>();
        jsonMap.put ("host" ,host);
        jsonMap.put ("port" , port);
        jsonMap.put ("metadata", jsonMetadata);

        createEntry(controllerInfoRoot + "/" + host + ":" + port, jsonEncode(jsonMap).getBytes());
        listener.controllerAddedNotification(host,port);
    }

    @Override
    public void unregisterPravegaController(String host, int port) {
        this.deleteEntry(controllerInfoRoot + "/" + host + ":" + port);
        listener.controllerRemovedNotification(host, port);
    }

    @Override
    public void unregisterPravegaNode(String host, int port) {
        this.deleteEntry(nodeInfoRoot + "/" + host + ":" + port);
        listener.nodeRemovedNotification(host, port);
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



}


