/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.health;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.function.BiConsumer;

@Slf4j
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
