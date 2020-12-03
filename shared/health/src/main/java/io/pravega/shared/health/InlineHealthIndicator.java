/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.health;

import lombok.NonNull;

import java.util.function.BiConsumer;

/**
 * {@link InlineHealthIndicator} provides a syntactic alternative to the {@link HealthIndicator}, but is ultimately functionally
 * equivalent. Instead of creating a {@link HealthIndicator} by subclassing it and implementing {@link HealthIndicator#doHealthCheck(Health.HealthBuilder)},
 * this simply provides the option to supply that abstract method as a lambda expression.
 */
public class InlineHealthIndicator extends HealthIndicator {

    private final BiConsumer<Health.HealthBuilder, DetailsProvider> doHealthCheck;

    @NonNull
    InlineHealthIndicator(String name, BiConsumer<Health.HealthBuilder, DetailsProvider> doHealthCheck) {
        this(name, doHealthCheck, new DetailsProvider());
    }

    @NonNull
    InlineHealthIndicator(String name, BiConsumer<Health.HealthBuilder, DetailsProvider> doHealthCheck, DetailsProvider provider) {
        super(name, provider);
        this.doHealthCheck = doHealthCheck;
    }

    @Override
    public void doHealthCheck(Health.HealthBuilder builder) throws Exception {
        doHealthCheck.accept(builder, this.provider);
    }
}
