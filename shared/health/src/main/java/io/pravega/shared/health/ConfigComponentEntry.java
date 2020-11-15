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

import java.util.HashSet;
import java.util.Set;

public class ConfigComponentEntry {

    public String name;

    public StatusAggregator aggregator;

    public Set<ConfigComponentEntry> dependencies = new HashSet<>();

    public ConfigComponentEntry(String name, StatusAggregator aggregator) {
        this.name = name;
        this.aggregator = aggregator;
    }
}
