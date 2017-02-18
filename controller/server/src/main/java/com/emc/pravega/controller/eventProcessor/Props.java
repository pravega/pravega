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
import com.google.common.base.Preconditions;
import lombok.Data;

import java.util.function.Supplier;

/**
 * Configuration object for creating EventProcessors via createEventProcessorGroup method of EventProcessorSystem.
 */
@Data
public class Props<T extends StreamEvent> {

    private final EventProcessorGroupConfig config;
    private final Decider decider;
    private final Serializer<T> serializer;
    private final Supplier<EventProcessor<T>> supplier;

    private Props(final EventProcessorGroupConfig config,
                  final Decider decider,
                  final Serializer<T> serializer,
                  final Supplier<EventProcessor<T>> supplier) {

        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(serializer);
        Preconditions.checkNotNull(supplier);
        this.config = config;
        if (decider == null) {
            this.decider = Decider.DEFAULT_DECIDER;
        } else {
            this.decider = decider;
        }
        this.serializer = serializer;
        this.supplier = supplier;
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
        private Supplier<EventProcessor<T>> supplier;

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

        public Props.PropsBuilder<T> supplier(Supplier<EventProcessor<T>> supplier) {
            this.supplier = supplier;
            return this;
        }

        public Props<T> build() {
            return new Props<>(this.config, this.decider, this.serializer, this.supplier);
        }

        public String toString() {
            return "Props.PropsBuilder(config=" + this.config + ", decider=" + this.decider + ", serializer=" +
                    this.serializer + ", supplier=" + this.supplier + ")";
        }
    }
}
