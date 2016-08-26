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
package com.emc.pravega.cluster.zkutils.common;

import com.emc.pravega.cluster.zkutils.abstraction.ConfigChangeListener;
import com.emc.pravega.cluster.zkutils.abstraction.ConfigSyncManager;
import com.google.gson.Gson;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by kandha on 8/23/16.
 */
public abstract class CommonConfigSyncManager implements ConfigSyncManager {

    /**
     * Place in the configuration manager where the data about live nodes is stored.
     */
    protected static final String NODE_INFO_ROOT = "/pravega/nodes";
    protected static final String CONTROLLER_INFO_ROOT = "/pravega/controllers";


    private static final String HOST_STRING = "\"host\"";
    private static final String PORT_STRING = "\"port\"";
    private static final String METADATA_STRING = "\"metadata\"";


    protected final ConfigChangeListener listener;

    public CommonConfigSyncManager(ConfigChangeListener listener) {
        this.listener = listener;
    }

    @Override
    public void registerPravegaNode(String host, int port, String jsonMetadata) throws Exception {
        Map jsonMap = new HashMap<String, Object>();
        jsonMap.put(HOST_STRING, host);
        jsonMap.put(PORT_STRING, port);
        jsonMap.put(METADATA_STRING, jsonMetadata);
        Gson gson = new Gson();

        createEntry(NODE_INFO_ROOT + "/" + host + ":" + port, gson.toJson(jsonMap).getBytes());
    }

    @Override
    public void registerPravegaController(String host, int port, String jsonMetadata) throws Exception {
        Map jsonMap = new HashMap<String, Object>();
        jsonMap.put(HOST_STRING, host);
        jsonMap.put(PORT_STRING, port);
        jsonMap.put(METADATA_STRING, jsonMetadata);
        Gson gson = new Gson();

        createEntry(CONTROLLER_INFO_ROOT + "/" + host + ":" + port, gson.toJson(jsonMap).getBytes());
    }

    @Override
    public void unregisterPravegaController(String host, int port) throws Exception {
        deleteEntry(CONTROLLER_INFO_ROOT + "/" + host + ":" + port);
    }

    @Override
    public void unregisterPravegaNode(String host, int port) throws Exception {
        deleteEntry(NODE_INFO_ROOT + "/" + host + ":" + port);
    }
}
