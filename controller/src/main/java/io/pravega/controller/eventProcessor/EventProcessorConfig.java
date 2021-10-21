/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.controller.eventProcessor;

import io.pravega.controller.eventProcessor.impl.EventProcessor;
import io.pravega.shared.controller.event.ControllerEvent;
import io.pravega.client.stream.Serializer;
import com.google.common.base.Preconditions;
import lombok.Data;

import java.util.function.Supplier;

/**
 * Configuration object for creating EventProcessors via createEventProcessorGroup method of EventProcessorSystem.
 */
@Data
public class EventProcessorConfig<T extends ControllerEvent> {

    private final EventProcessorGroupConfig config;
    private final ExceptionHandler exceptionHandler;
    private final Serializer<T> serializer;
    private final Supplier<EventProcessor<T>> supplier;
    private final long rebalancePeriodMillis;

    private EventProcessorConfig(final EventProcessorGroupConfig config,
                                 final ExceptionHandler exceptionHandler,
                                 final Serializer<T> serializer,
                                 final Supplier<EventProcessor<T>> supplier, 
                                 final long rebalancePeriodMillis) {
        Preconditions.checkNotNull(config);
        Preconditions.checkNotNull(serializer);
        Preconditions.checkNotNull(supplier);
        this.config = config;
        if (exceptionHandler == null) {
            this.exceptionHandler = ExceptionHandler.DEFAULT_EXCEPTION_HANDLER;
        } else {
            this.exceptionHandler = exceptionHandler;
        }
        this.serializer = serializer;
        this.supplier = supplier;
        this.rebalancePeriodMillis = rebalancePeriodMillis;
    }

    public static <T extends ControllerEvent> EventProcessorConfigBuilder<T> builder() {
        return new EventProcessorConfigBuilder<>();
    }

    /**
     * EventProcessorConfigBuilder.
     * @param <T> Type parameter
     */
    public static class EventProcessorConfigBuilder<T extends ControllerEvent> {
        private EventProcessorGroupConfig config;
        private ExceptionHandler exceptionHandler;
        private Serializer<T> serializer;
        private Supplier<EventProcessor<T>> supplier;
        private long rebalancePeriodMillis = Long.MIN_VALUE; // default is rebalancing disabled

        EventProcessorConfigBuilder() {
        }

        public EventProcessorConfigBuilder<T> config(EventProcessorGroupConfig config) {
            this.config = config;
            return this;
        }

        public EventProcessorConfigBuilder<T> decider(ExceptionHandler exceptionHandler) {
            this.exceptionHandler = exceptionHandler;
            return this;
        }

        public EventProcessorConfigBuilder<T> serializer(Serializer<T> serializer) {
            this.serializer = serializer;
            return this;
        }

        public EventProcessorConfigBuilder<T> supplier(Supplier<EventProcessor<T>> supplier) {
            this.supplier = supplier;
            return this;
        }
        
        public EventProcessorConfigBuilder<T> minRebalanceIntervalMillis(long rebalancePeriodMillis) {
            this.rebalancePeriodMillis = rebalancePeriodMillis;
            return this;
        }

        public EventProcessorConfig<T> build() {
            return new EventProcessorConfig<>(this.config, this.exceptionHandler, this.serializer, this.supplier, this.rebalancePeriodMillis);
        }

        @Override
        public String toString() {
            return "Props.PropsBuilder(config=" + this.config + ", exceptionHandler=" + this.exceptionHandler + ", serializer=" +
                    this.serializer + ", supplier=" + this.supplier + ", rebalancePeriodMillis=" + this.rebalancePeriodMillis + ")";
        }
    }
}
