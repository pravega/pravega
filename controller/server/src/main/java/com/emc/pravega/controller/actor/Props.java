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

import com.emc.pravega.controller.actor.impl.Actor;
import lombok.Builder;
import lombok.Data;
import lombok.Singular;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.Optional;

/**
 * Configuration object for creating Actors via actorOf method of ActorSystem or ActorContext.
 */
@Data
public class Props {

    private final ActorGroupConfig config;
    private final ReaderStatePersistence persister;
    private final Class<? extends Actor> clazz;
    @Singular
    private final Object[] args;
    private final Constructor<? extends Actor> constructor;

    @Builder
    public Props(ActorGroupConfig config, ReaderStatePersistence persister, Class<? extends Actor> clazz, Object... args) {
        if (!validate(clazz)) {
            throw new IllegalArgumentException("Non-actor type or non-instantiable type");
        }
        this.config = config;
        this.persister = persister;
        this.clazz = clazz;
        this.args = args;

        Optional<Constructor<? extends Actor>> optional = getValidConstructor(clazz, args);
        if (optional.isPresent()) {
            this.constructor = optional.get();
        } else {
            throw new IllegalArgumentException("Invalid constructor arguments");
        }
    }

    private boolean validate(Class<? extends Actor> clazz) {
        return !Modifier.isAbstract(clazz.getModifiers());
    }

    private Optional<Constructor<? extends Actor>> getValidConstructor(Class<? extends Actor> clazz, Object... args) {
        int n = args.length;
        Class[] argumentTypes = new Class[n];
        for (int i = 0; i < n; i++) {
            argumentTypes[i] = args[i].getClass();
        }
        try {
            return Optional.of(clazz.getConstructor(argumentTypes));
        } catch (NoSuchMethodException e) {
            return Optional.empty();
        }
    }
}
