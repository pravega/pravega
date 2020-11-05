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

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

/**
 * The {@link HealthComponent} class is used to provide a logical grouping of components. Each registered {@link  HealthComponent}
 * will export it's JSON representation via some HTTP route.
 *
 * The children/dependencies of a {@link HealthComponent} are used to determine the {@link Status} of this component, based
 * on some {@link StatusAggregationRule}.
 */
@Slf4j
public class HealthComponent extends CompositeHealthContributor {

    public static final HealthComponent ROOT = new HealthComponent("ROOT");

    @Getter
    private final String name;

    private final HealthComponent parent;

    HealthComponent(String name) {
        this(name, null);
    }

    HealthComponent(String name, HealthComponent parent) {
        this.name = name;
        this.parent = parent;
    }

    @Override
    public String toString() {
        return String.format("HealthComponent::%s", this.name);
    }
}
