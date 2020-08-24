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

/**
 * HealthInfo holds the health-check result which is comprised of status and details.
 */

@Data
public class HealthInfo {
    public enum Status {
        /* The result of the health-check is considered healthy */
        HEALTH,
        /* The result of the health-check is considered unhealthy */
        UNHEALTH,
        /* The result of the health-check is unknown, due to time-out, interruption or other exception happened */
        UNKNOWN
    }

    /*
     * The status of the health-check
     */
    private final Status status;

    /*
     * The details of the health-check
     */
    private final String details;
}
