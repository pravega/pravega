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
package io.pravega.client.stream;

import org.junit.Test;

import static io.pravega.test.common.AssertExtensions.assertThrows;

public class ScalingPolicyTest {

    @Test
    public void testScalingPolicyArguments() {
        // Check that we do not allow incorrect arguments for creating a scaling policy.
        assertThrows(RuntimeException.class, () -> ScalingPolicy.fixed(0));
        assertThrows(RuntimeException.class, () -> ScalingPolicy.byEventRate(0, 1, 1));
        assertThrows(RuntimeException.class, () -> ScalingPolicy.byDataRate(1, 0, 0));
    }
}
