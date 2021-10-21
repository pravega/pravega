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
package io.pravega.client.tables.impl;

import io.pravega.shared.protocol.netty.WireCommands;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import lombok.Cleanup;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TableSegmentKeyVersionTest {

    @Test
    public void testKeyVersionSerialization() throws Exception {
        TableSegmentKeyVersion kv = TableSegmentKeyVersion.from(5L);
        assertEquals(kv, TableSegmentKeyVersion.fromBytes(kv.toBytes()));
        byte[] buf = serialize(kv);
        assertEquals(kv, deSerializeKeyVersion(buf));
    }

    @Test
    public void testNotExistsKeySerialization() throws Exception {
        TableSegmentKeyVersion kv = TableSegmentKeyVersion.NOT_EXISTS;
        assertEquals(kv, TableSegmentKeyVersion.fromBytes(kv.toBytes()));
        byte[] buf = serialize(kv);
        assertEquals(kv, deSerializeKeyVersion(buf));
    }

    @Test
    public void testNoVersionKeySerialization() throws Exception {
        TableSegmentKeyVersion kv = TableSegmentKeyVersion.NO_VERSION;
        assertEquals(kv, TableSegmentKeyVersion.fromBytes(kv.toBytes()));
        byte[] buf = serialize(kv);
        assertEquals(kv, deSerializeKeyVersion(buf));
    }

    @Test
    public void testSpecialVersions() {
        Assert.assertEquals(WireCommands.TableKey.NO_VERSION, TableSegmentKeyVersion.NO_VERSION.getSegmentVersion());
        Assert.assertEquals(WireCommands.TableKey.NOT_EXISTS, TableSegmentKeyVersion.NOT_EXISTS.getSegmentVersion());
        Assert.assertSame(TableSegmentKeyVersion.NO_VERSION, TableSegmentKeyVersion.from(WireCommands.TableKey.NO_VERSION));
        Assert.assertSame(TableSegmentKeyVersion.NOT_EXISTS, TableSegmentKeyVersion.from(WireCommands.TableKey.NOT_EXISTS));
    }

    private byte[] serialize(TableSegmentKeyVersion sc) throws IOException {
        @Cleanup
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        @Cleanup
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(sc);
        return baos.toByteArray();
    }

    private TableSegmentKeyVersion deSerializeKeyVersion(final byte[] buf) throws Exception {
        @Cleanup
        ByteArrayInputStream bais = new ByteArrayInputStream(buf);
        @Cleanup
        ObjectInputStream ois = new ObjectInputStream(bais);
        return (TableSegmentKeyVersion) ois.readObject();
    }
}
