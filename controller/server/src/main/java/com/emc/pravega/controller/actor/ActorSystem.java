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
package com.emc.pravega.controller.actor;

import com.emc.pravega.ClientFactory;
import com.emc.pravega.StreamManager;
import com.emc.pravega.stream.impl.Controller;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;

public class ActorSystem {

    protected final Controller controller;
    protected final ClientFactory clientFactory;
    protected final StreamManager streamManager;

    public ActorSystem(Controller controller, ClientFactory clientFactory, StreamManager streamManager) {
        this.controller = controller;
        this.clientFactory = clientFactory;
        this.streamManager = streamManager;
    }

    public ActorGroup createActorGroup(ActorGroupConfig config, Class clazz, Object... args) {
        validate(clazz, args);
        Constructor constructor = getValidConstructor(clazz, args);
        return new ActorGroup(this, config, clazz, constructor, args);
    }

    public ActorGroupRef getActorGroupRef(String scope, String stream) {
        return new ActorGroupRef(this, scope, stream);
    }

    private boolean validate(Class clazz, Object... args) {
        return Actor.class.isAssignableFrom(clazz) && Modifier.isAbstract(clazz.getModifiers());
    }

    private Constructor getValidConstructor(Class clazz, Object... args) {
        throw new NotImplementedException();
    }
}
