/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.health;

import io.pravega.shared.health.HealthProvider;
import io.pravega.shared.health.HealthService;

public class AbstractControllerHealthIndicator {

    static final HealthService HEALTH_SERVICE = HealthProvider.getHealthService();
}
