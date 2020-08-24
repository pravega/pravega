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

/**
 * The interface of the container holds HealthUnit references, which must provide the ability to
 * register and unregister HealthUnit.
 */
public interface HealthRegistry {

    /**
     * Register an HealthUnit.
     *
     * @param unit HealthUnit object
     */
    void registerHealthUnit(HealthUnit unit);

    /**
     * Unregister an HealthUnit.
     *
     * @param unit HealthUnit object
     */
    void unregisterHealthUnit(HealthUnit unit);

}
