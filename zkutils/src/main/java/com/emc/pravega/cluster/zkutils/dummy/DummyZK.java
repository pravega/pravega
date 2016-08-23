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
import com.emc.pravega.cluster.zkutils.common.CommonConfigSyncManager;
import com.google.common.base.Preconditions;

import java.util.concurrent.ConcurrentHashMap;

/**
  * Created by kandha on 8/8/16.
  */
public class DummyZK extends CommonConfigSyncManager {

    ConcurrentHashMap<String,byte[]> valueMap;

    public DummyZK(String connectString, int sessionTimeout, ConfigChangeListener listener) {
        super(listener);
        Preconditions.checkNotNull(listener);

        valueMap = new ConcurrentHashMap<>();
    }

    /**
     * Sample configuration/synchronization methods. Will add more as implementation progresses
     *  @param path
     * @param value
     */
    @Override
    public void createEntry(String path, byte[] value) {
        Preconditions.checkNotNull(path);
        Preconditions.checkNotNull(value);
        valueMap.put(path,value);
    }

    @Override
    public void deleteEntry(String path) {
        valueMap.remove(path);

    }

    @Override
    public void refreshCluster() {

    }

}


