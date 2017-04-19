/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package com.emc.pravega.controller.eventProcessor;

import com.emc.pravega.controller.eventProcessor.impl.EventProcessor;
import com.emc.pravega.controller.requests.ControllerEvent;
import com.emc.pravega.stream.Serializer;
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

    private EventProcessorConfig(final EventProcessorGroupConfig config,
                                 final ExceptionHandler exceptionHandler,
                                 final Serializer<T> serializer,
                                 final Supplier<EventProcessor<T>> supplier) {

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

        public EventProcessorConfig<T> build() {
            return new EventProcessorConfig<>(this.config, this.exceptionHandler, this.serializer, this.supplier);
        }

        public String toString() {
            return "Props.PropsBuilder(config=" + this.config + ", exceptionHandler=" + this.exceptionHandler + ", serializer=" +
                    this.serializer + ", supplier=" + this.supplier + ")";
        }
    }
}
