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

import lombok.Getter;
import org.junit.Test;
import io.pravega.shared.health.TestHealthContributors.HealthyContributor;
import io.pravega.shared.health.TestHealthContributors.FailingContributor;

import static org.junit.Assert.assertEquals;

public class HealthConnectorTests {

    @Test
    public void testHealthCheck() {
        TestHealthConnector parent = new TestHealthConnector("parent", new HealthyContributor("parent"));
        TestHealthConnector child = new TestHealthConnector("child", new FailingContributor("child"));
        // Test parent on its own.
        assert parent.getContributor().getHealthSnapshot().getStatus() == Status.RUNNING;
        // Connect the child, expect it to fail.
        child.connect(parent.getContributor());
        // Check the registration is successful.
        assertEquals("Unexpected number of child contributors.", 1, parent.getContributor().getHealthSnapshot().getChildren().size());
        // Check the correct status is reported.
        assertEquals(parent.getContributor().getHealthSnapshot().getStatus(), Status.TERMINATED);

        // Try to add a contributor that forgot to implement getContributor.
        TestHealthConnector nullConnector = new TestHealthConnector("nullConnector", null);
        // No contributor should have been added.
        nullConnector.connect(child);
        assertEquals("Unexpected number of child contributors.", 0, child.getContributor().getHealthSnapshot().getChildren().size());
    }

    class TestHealthConnector implements HealthConnector {
        @Getter
        private final HealthContributor contributor;
        private final String name;

        TestHealthConnector(String name, HealthContributor contributor) {
            this.contributor = contributor;
            this.name = name;
        }
    }
}
