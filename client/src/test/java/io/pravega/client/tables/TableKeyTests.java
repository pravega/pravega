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

import java.nio.ByteBuffer;
import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link TableKey} class.
 */
public class TableKeyTests {
    @Test
    public void testConstructor() {
        val pk = ByteBuffer.wrap("PrimaryKey".getBytes());
        val sk = ByteBuffer.wrap("SecondaryKey".getBytes());
        val ne = TableKey.notExists(pk, sk);
        Assert.assertSame(pk, ne.getPrimaryKey());
        Assert.assertSame(sk, ne.getSecondaryKey());
        Assert.assertSame(Version.NOT_EXISTS, ne.getVersion());

        val uv = TableKey.unversioned(pk, sk);
        Assert.assertSame(pk, uv.getPrimaryKey());
        Assert.assertSame(sk, uv.getSecondaryKey());
        Assert.assertSame(Version.NO_VERSION, uv.getVersion());
    }
}
