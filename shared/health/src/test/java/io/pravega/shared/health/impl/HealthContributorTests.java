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
package io.pravega.shared.health.impl;

import io.pravega.common.ObjectClosedException;
import io.pravega.shared.health.Health;
import io.pravega.shared.health.HealthContributor;
import io.pravega.shared.health.Status;
import io.pravega.test.common.AssertExtensions;
import lombok.Cleanup;
import org.junit.Assert;
import org.junit.Test;
import io.pravega.shared.health.TestHealthContributors.HealthyContributor;
import io.pravega.shared.health.TestHealthContributors.ThrowingContributor;
import io.pravega.shared.health.TestHealthContributors.FailingContributor;

public class HealthContributorTests {

    /**
     * Perform a basic {@link Health} check.
     */
    @Test
    public void testHealth() {
        @Cleanup
        HealthyContributor contributor = new HealthyContributor();
        Health health = contributor.getHealthSnapshot();
        Assert.assertEquals("Should exactly one detail entry.", 1, health.getDetails().size());
        Assert.assertEquals("HealthContributor should report an 'UP' Status.", Status.UP, health.getStatus());
    }

    /**
     * If the {@link AbstractHealthContributor#doHealthCheck(Health.HealthBuilder)} throws an error, test that it is caught and
     * a {@link Health} result that reflects an 'unhealthy' state is returned.
     */
    @Test
    public void testIndicatorThrows() {
        @Cleanup
        ThrowingContributor contributor = new ThrowingContributor();
        Health health = contributor.getHealthSnapshot();
        Assert.assertEquals("HealthContributor should have a 'DOWN' Status.", Status.DOWN, health.getStatus());
        Assert.assertTrue("HealthContributor should be not be marked ready OR alive.", !health.isAlive() && !health.isReady());
    }

    /**
     * Tests that children {@link HealthContributor} can be properly registered to another contributor and the {@link Health}
     * of the individual children is properly reflected in the overall health of the top most contributor.
     */
    @Test
    public void testChildHealths() {
        @Cleanup
        HealthContributor contributor = new HealthyContributor();
        @Cleanup
        HealthContributor first = new HealthyContributor("first");
        @Cleanup
        HealthContributor second = new FailingContributor("second");
        contributor.register(first, second);

        Health health = contributor.getHealthSnapshot();
        Assert.assertEquals("Expected 'contributor' to report an unhealthy status.", Status.DOWN, health.getStatus());
        Assert.assertEquals("Expected to see two children registered to 'contributor'.", 2, health.getChildren().size());
    }

    /**
     * Verifies that a closed {@link HealthContributor} can no longer be acted upon.
     */
    @Test
    public void testClosedContributor() {
        @Cleanup
        HealthContributor root = new HealthyContributor();
        HealthContributor contributor = new HealthyContributor();
        contributor.close();

        AssertExtensions.assertThrows("Expected an exception requesting the health of a closed contributor.",
                () -> contributor.getHealthSnapshot(),
                ex -> ex instanceof ObjectClosedException);
        AssertExtensions.assertThrows("Expected an exception adding a child to a closed contributor.",
                () -> contributor.register(new HealthyContributor("")),
                ex -> ex instanceof ObjectClosedException);

        HealthContributor parent = new HealthyContributor("parent");
        root.register(parent);
        @Cleanup
        HealthContributor child = new FailingContributor("child");
        parent.register(child);

        parent.close();
        Assert.assertEquals("Expecting child contributor to be unreachable after closing parent",
                0,
               root.getHealthSnapshot().getChildren().size());
        Assert.assertEquals("Expecting default Status (UP) from empty HealthContributor.",
                Status.UP,
                root.getHealthSnapshot().getStatus());

    }

    /**
     * Verifies that if the {@link Health} of the child {@link HealthContributor} changes, it properly
     * determines the overall health before and after the change.
     */
    @Test
    public void testDynamicContributor() {
        @Cleanup
        HealthContributor root = new HealthyContributor();
        root.register(new HealthyContributor("first"));
        Assert.assertEquals("Expecting healthy status.", Status.UP, root.getHealthSnapshot().getStatus());
        // Add a failing contributor to the root, which uses the 'UNANIMOUS' aggregation rule.
        HealthContributor failing = new FailingContributor();
        root.register(failing);
        Assert.assertEquals("Expecting failing status.", Status.DOWN, root.getHealthSnapshot().getStatus());
        // Remove the failing contributor and now expect it is healthy again.
        failing.close();
        Assert.assertEquals("Expecting healthy status.", Status.UP, root.getHealthSnapshot().getStatus());
    }
}
