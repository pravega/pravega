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

/**
 * The top level interface used to provide any and all health related information for a particular component
 * of Pravega. It holds the {@link ContributorRegistry} and provides the endpoint used to make health information
 * accessible to clients.
 *
 * A {@link HealthService} should provide four endpoints:
 *  * /health - A route providing the summation of the three subroutes.
 *  * /health/readiness - A route exporting
 */
public interface HealthService {

}
