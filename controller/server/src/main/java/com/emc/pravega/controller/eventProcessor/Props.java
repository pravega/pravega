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
package com.emc.pravega.controller.eventProcessor;

import com.emc.pravega.controller.eventProcessor.impl.EventProcessor;
import com.emc.pravega.stream.Serializer;
import lombok.Data;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Optional;

/**
 * Configuration object for creating Actors via actorOf method of ActorSystem or ActorContext.
 */
@Data
public class Props<T extends StreamEvent> {

    private final EventProcessorGroupConfig config;
    private final Decider decider;
    private final Serializer<T> serializer;
    private final Class<? extends EventProcessor<T>> clazz;
    private final Object[] args;
    private final Constructor<? extends EventProcessor<T>> constructor;

    private Props(final EventProcessorGroupConfig config,
                 final Decider decider,
                 final Serializer<T> serializer,
                 final Class<? extends EventProcessor<T>> clazz,
                 final Object... args) {
        if (!validate(clazz)) {
            throw new IllegalArgumentException("Non-actor type or non-instantiable type");
        }
        this.config = config;
        if (decider == null) {
            this.decider = Decider.DEFAULT_DECIDER;
        } else {
            this.decider = decider;
        }
        this.serializer = serializer;
        this.clazz = clazz;
        this.args = args;

        Optional<Constructor<? extends EventProcessor<T>>> optional = getValidConstructor(clazz, args);
        if (optional.isPresent()) {
            this.constructor = optional.get();
        } else {
            throw new IllegalArgumentException("Invalid constructor arguments");
        }
    }

    private boolean validate(Class<? extends EventProcessor<T>> clazz) {
        return !Modifier.isAbstract(clazz.getModifiers());
    }

    private Optional<Constructor<? extends EventProcessor<T>>> getValidConstructor(Class<? extends EventProcessor<T>> clazz, Object... args) {

        if (args == null || args.length == 0) {

            for (Constructor constructor : clazz.getConstructors()) {
                if (constructor.getParameterCount() == 0) {
                    return Optional.of(constructor);
                }
            }
            return Optional.empty();

        } else {

            int n = args.length;
            Class[] argumentTypes = new Class[n];
            for (int i = 0; i < n; i++) {
                argumentTypes[i] = args[i].getClass();
            }
            Constructor[] constructors = clazz.getConstructors();
            for (Constructor constructor : constructors) {
                if (arrayMatches(argumentTypes,
                        constructor.getParameterTypes())) {
                    return Optional.of(constructor);
                }
            }
            return Optional.empty();
        }
    }

    private boolean arrayMatches(Class<?>[] parameterTypes, Class<?>[] constructorTypes) {
        if (parameterTypes.length != constructorTypes.length) {
            return false;
        } else {
            for (int i = 0; i < parameterTypes.length; i++) {
                if (!constructorTypes[i].isAssignableFrom(parameterTypes[i])) {
                    return false;
                }
            }
            return true;
        }
    }

    public static <T extends StreamEvent> Props.PropsBuilder<T> builder() {
        return new Props.PropsBuilder<>();
    }

    /**
     * PropsBuilder.
     * @param <T> Type parameter
     */
    public static class PropsBuilder<T extends StreamEvent> {
        private EventProcessorGroupConfig config;
        private Decider decider;
        private Serializer<T> serializer;
        private Class<? extends EventProcessor<T>> clazz;
        private Object[] args;

        PropsBuilder() {
        }

        public Props.PropsBuilder<T> config(EventProcessorGroupConfig config) {
            this.config = config;
            return this;
        }

        public Props.PropsBuilder<T> decider(Decider decider) {
            this.decider = decider;
            return this;
        }

        public Props.PropsBuilder<T> serializer(Serializer<T> serializer) {
            this.serializer = serializer;
            return this;
        }

        public Props.PropsBuilder<T> clazz(Class<? extends EventProcessor<T>> clazz) {
            this.clazz = clazz;
            return this;
        }

        public Props.PropsBuilder<T> args(Object... args) {
            this.args = args;
            return this;
        }

        public Props<T> build() {
            return new Props<>(this.config, this.decider, this.serializer, this.clazz, this.args);
        }

        public String toString() {
            return "Props.PropsBuilder(config=" + this.config + ", decider=" + this.decider + ", serializer=" +
                    this.serializer + ", clazz=" + this.clazz + ", args=" + Arrays.deepToString(this.args) + ")";
        }
    }
}
