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
 * The {@link HealthContributor} interface is the primary interface a client (some arbitrary class) uses to export health
 * information to the {@link HealthService}. A {@link HealthContributor} is something that *contributes* health {@link Status}
 * information used to determine the well-being of one or more components.
 *
 * A {@link HealthContributor} will implicitly register itself with the {@link ContributorRegistry}.
 */
public interface HealthContributor {
}
