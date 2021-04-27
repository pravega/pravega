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
package io.pravega.client.tables;

import io.pravega.test.common.AssertExtensions;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for the {@link KeyValueTableConfiguration} class.
 */
public class KeyValueTableConfigurationTest {
    @Test
    public void testBuilder() {
        val c = KeyValueTableConfiguration.builder()
                .partitionCount(4)
                .primaryKeyLength(8)
                .secondaryKeyLength(6).build();
        Assert.assertEquals(4, c.getPartitionCount());
        Assert.assertEquals(8, c.getPrimaryKeyLength());
        Assert.assertEquals(6, c.getSecondaryKeyLength());

        AssertExtensions.assertThrows("build() accepted bad partitionCount.",
                () -> KeyValueTableConfiguration.builder().partitionCount(0).primaryKeyLength(8).secondaryKeyLength(4).build(),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows("build() accepted bad primaryKeyLength.",
                () -> KeyValueTableConfiguration.builder().partitionCount(1).primaryKeyLength(0).secondaryKeyLength(4).build(),
                ex -> ex instanceof IllegalArgumentException);

        AssertExtensions.assertThrows("build() accepted bad secondaryKeyLength.",
                () -> KeyValueTableConfiguration.builder().partitionCount(1).primaryKeyLength(8).secondaryKeyLength(-1).build(),
                ex -> ex instanceof IllegalArgumentException);
    }
}
