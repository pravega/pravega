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

import io.pravega.shared.health.Health;
import io.pravega.shared.health.Status;
import org.junit.Assert;
import org.junit.Test;

public class HealthTests {

    /**
     * Tests that the default/empty {@link Health} reports the expected readiness result.
     */
    @Test
    public void testDefaultReadyLogic() {
        Health health = Health.builder().build();
        Assert.assertEquals("isReady() should be false by default if no Status is set.", false, health.isReady());
        health = Health.builder().status(Status.UP).build();
        Assert.assertEquals("isReady() should be true by default if an UP Status is supplied.", true, health.isReady());
    }

    /**
     * Tests that the default/empty {@link Health} reports the expected liveness result.
     */
    @Test
    public void testDefaultAliveLogic() {
        Health health = Health.builder().build();
        Assert.assertEquals("isAlive() should be false by default if no Status is set.", false, health.isAlive());
        health = Health.builder().status(Status.UP).build();
        Assert.assertEquals("isAlive() should be true by default if an UP Status is supplied.", true, health.isAlive());
    }
}
