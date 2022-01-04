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
package io.pravega.segmentstore.server.logs.health;

import io.pravega.segmentstore.server.logs.InMemoryLog;
import io.pravega.segmentstore.server.logs.operations.MetadataCheckpointOperation;
import io.pravega.shared.health.Health;
import io.pravega.shared.health.HealthContributor;
import io.pravega.shared.health.Status;
import lombok.val;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class InMemoryLogHealthContributorTests {

    private InMemoryLog log;

    @Before
    public void setup() {
        log = new InMemoryLog();
    }

    @After
    public void tearDown() {
        this.log.close();
    }

    @Test
    public void testHealthContributor() {
        HealthContributor contributor = new InMemoryLogHealthContributor("log", log);
        Health health = contributor.getHealthSnapshot();
        Assert.assertEquals(health.getStatus(), Status.RUNNING);
        Assert.assertEquals(health.getDetails().get("Size"), 0);

        val op = new MetadataCheckpointOperation();
        op.setSequenceNumber(1);
        log.add(op);

        health = contributor.getHealthSnapshot();
        Assert.assertEquals(health.getDetails().get("Size"), 1);
    }
}
