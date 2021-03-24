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

import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link TableEntry} class.
 */
public class TableEntryTests {
    @Test
    public void testConstructor() {
        String keyContents = "KeyContents";
        long valueContents = 1234L;
        val k = TableKey.notExists(keyContents);
        val e = TableEntry.notExists(k, valueContents);
        Assert.assertEquals(k.getVersion(), e.getKey().getVersion());
        Assert.assertEquals(keyContents, e.getKey().getKey().getKey());
        Assert.assertEquals(valueContents, (long) e.getValue());
    }
}
