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

/**
 * HealthUnit is the smallest unit able to perform health-check and return HealthInfo.
 * A HealthUnit must belong to one and only one HealthAspect.
 * HealthUnitId is to uniquely identify the unit within the aspect.
 * HealthUnit holds a Supplier to supply HealthInfo of the unit.
 */

@Data
public class HealthUnit {

    /**
     * Id to uniquely identify the HealthUnit from the aspect it belongs to.
     * Usually this id can be derived from an existing id, such as the id of the hosting component.
     */
    final String healthUnitId;

    /**
     * The HealthAspect this HealthUnit is coming from.
     */
    final HealthAspect healthAspect;

    /**
     * Supplier to supply HealthInfo of the hosting component upon health-check request.
     */
    final Supplier<HealthInfo> healthInfoSupplier;
}
