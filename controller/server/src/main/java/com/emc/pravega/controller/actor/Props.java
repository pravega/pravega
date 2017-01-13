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

import lombok.Data;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.Optional;

@Data
public class Props {

    private final ActorGroupConfig config;
    private final PositionPersistence persister;
    private final Class clazz;
    private final Object[] args;
    private final Constructor constructor;

    public Props(ActorGroupConfig config, PositionPersistence persister, Class clazz, Object... args) {
        if (!validate(clazz)) {
            throw new IllegalArgumentException("Non-actor type or non-instantiable type");
        }
        this.config = config;
        this.persister = persister;
        this.clazz = clazz;
        this.args = args;

        Optional<Constructor> c = getValidConstructor(clazz, args);
        if (c.isPresent()) {
            this.constructor = c.get();
        } else {
            throw new IllegalArgumentException("invalid argument set");
        }
    }

    private boolean validate(Class clazz) {
        return Actor.class.isAssignableFrom(clazz) && Modifier.isAbstract(clazz.getModifiers());
    }

    private Optional<Constructor> getValidConstructor(Class clazz, Object... args) {
        int n = args.length;
        Class[] argumentTypes = new Class[n];
        for (int i = 0; i < n; i++) {
            argumentTypes[i] = args[i].getClass();
        }
        try {
            return Optional.of(clazz.getDeclaredConstructor(argumentTypes));
        } catch (NoSuchMethodException e) {
            return Optional.empty();
        }
    }


}
