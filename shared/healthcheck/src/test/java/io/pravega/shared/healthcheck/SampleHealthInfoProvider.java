/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.shared.healthcheck;

import lombok.Data;

import java.util.function.Supplier;

@Data
public class SampleHealthInfoProvider implements AutoCloseable {

    HealthUnit sampleHealthUnit;

    public SampleHealthInfoProvider(String healthUnitId, HealthAspect aspect, Supplier<HealthInfo> supplier) {
        this.sampleHealthUnit = new HealthUnit(healthUnitId, aspect, supplier);
        HealthRegistryImpl.getInstance().registerHealthUnit(this.sampleHealthUnit);
    }

    @Override
    public void close() {
        HealthRegistryImpl.getInstance().unregisterHealthUnit(sampleHealthUnit);
    }
}

