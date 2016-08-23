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

package com.emc.pravega.cluster.zkutils.abstraction;

import com.emc.pravega.cluster.zkutils.common.Endpoint;

/**
  * Callback interface for the configuration change events
  */
public interface ConfigChangeListener {
    /**
     * Notification for addition of a node
     * @param endpoint
     */
  void nodeAddedNotification(Endpoint endpoint);

    /**
     * Notification for addition of a controller
     * @param endpoint
     */
  void  controllerAddedNotification(Endpoint endpoint);

    /**
     * Notification for removal of a node
     * @param endpoint
     */
  void nodeRemovedNotification(Endpoint endpoint);

    /**
     * Notification for removal of a controller
     * @param endpoint
     */
  void controllerRemovedNotification(Endpoint endpoint);

}
