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
package com.emc.pravega.controller.util;

import com.typesafe.config.ConfigFactory;

/**
 * This is a utility used to read configuration. It can be configured to read custom configuration
 * files by setting the following system properties conf.file= < FILE PATH > or conf.resource=< Resource Name>. By default
 * it reads application.conf if no system property is set. Reference: {@link ConfigFactory#defaultApplication()}
 */
public final class Config {
    private final static com.typesafe.config.Config CONFIG = ConfigFactory.defaultApplication();

    //RPC Server configuration
    public static final int SERVER_PORT = CONFIG.getInt("config.controller.server.port");
    public static final int SERVER_SELECTOR_THREAD_COUNT = CONFIG.getInt("config.controller.server.selectorThreadCount");
    public static final int SERVER_WORKER_THREAD_COUNT = CONFIG.getInt("config.controller.server.workerThreadCount");

    //Store configuration.
    //Stream store configuration.
    public static final String STREAM_STORE_TYPE = CONFIG.getString("config.controller.server.store.stream.type");

    //HostStore configuration.
    public static final String HOST_STORE_TYPE = CONFIG.getString("config.controller.server.store.host.type");
    public static final int HOST_STORE_CONTAINER_COUNT = CONFIG.getInt("config.controller.server.store.host.containerCount");
}
