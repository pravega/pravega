/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.metrics;

import static io.pravega.shared.MetricsNames.CONTROLLER_ZK_SESSION_EXPIRATION;
import static io.pravega.shared.MetricsNames.globalMetricName;

/**
 * Class to encapsulate the logic to report Controller metrics related to Zookeeper.
 */
public class ZookeeperConnectionMetrics extends AbstractControllerMetrics {

    /**
     * This method reports a new session expiration event in a Controller instance.
     */
    public void reportZKSessionExpiration() {
        DYNAMIC_LOGGER.incCounterValue(globalMetricName(CONTROLLER_ZK_SESSION_EXPIRATION), 1);
    }
}
