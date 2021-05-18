/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.pravega.shared.health;

import org.junit.Assert;
import org.junit.Test;
import io.pravega.shared.health.TestHealthContributors.HealthyContributor;
import io.pravega.shared.health.TestHealthContributors.ThrowingContributor;

public class HealthIndicatorTests {

    /**
     * Test a {@link Health} check, but request its details.
     */
    @Test
    public void testHealthDetails() {
        // Details implicitly added during class construction.
        HealthyContributor indicator = new HealthyContributor();
        Health health = indicator.getHealthSnapshot();
        Assert.assertEquals("Should exactly one detail entry.", 1, health.getDetails().size());
        Assert.assertEquals("HealthIndicator should report an 'UP' Status.", Status.UP, health.getStatus());
    }


    /**
     * If the {@link io.pravega.shared.health.impl.HealthContributorImpl#doHealthCheck(Health.HealthBuilder)} throws an error, test that it is caught and
     * a {@link Health} result that reflects an 'unhealthy' state is returned.
     */
    @Test
    public void testIndicatorThrows() {
        ThrowingContributor indicator = new ThrowingContributor();
        Health health = indicator.getHealthSnapshot();
        Assert.assertEquals("HealthIndicator should have a 'TERMINATED' Status.", Status.DOWN, health.getStatus());
        Assert.assertTrue("HealthIndicator should be not be marked ready OR alive.", !health.isAlive() && !health.isReady());
    }

}
