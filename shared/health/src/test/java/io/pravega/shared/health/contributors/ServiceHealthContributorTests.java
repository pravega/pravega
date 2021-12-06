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
package io.pravega.shared.health.contributors;

import com.google.common.util.concurrent.Service;
import io.pravega.shared.health.Status;
import lombok.Cleanup;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ServiceHealthContributorTests {

    private Service service;
    private Service.State state;

    @Before
    public void setup() {
        service = mock(Service.class);
        state = mock(Service.State.class);
    }

    @After
    public void tearDown() {
        service.stopAsync();
        service.awaitTerminated();
    }

    /**
     * Validate all state mappings.
     */
    @Test
    public void testHealthCheck() {
        @Cleanup
        ServiceHealthContributor contributor = new ServiceHealthContributor("test-service", service);
        when(service.state()).thenReturn(Service.State.RUNNING);
        assert contributor.getHealthSnapshot().getStatus() == Status.RUNNING;
        when(service.state()).thenReturn(Service.State.STARTING);
        assert contributor.getHealthSnapshot().getStatus() == Status.STARTING;
        when(service.state()).thenReturn(Service.State.NEW);
        assert contributor.getHealthSnapshot().getStatus() == Status.NEW;
        when(service.state()).thenReturn(Service.State.STOPPING);
        assert contributor.getHealthSnapshot().getStatus() == Status.STOPPING;
        when(service.state()).thenReturn(Service.State.TERMINATED);
        assert contributor.getHealthSnapshot().getStatus() == Status.TERMINATED;
        when(service.state()).thenReturn(Service.State.FAILED);
        assert contributor.getHealthSnapshot().getStatus() == Status.FAILED;
    }

}
