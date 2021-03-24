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
package io.pravega.client.state.impl;

import io.pravega.client.segment.impl.Segment;
import io.pravega.client.state.Revision;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import lombok.Cleanup;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SerializationTest {

    @Test
    public void testRevision() throws IOException, ClassNotFoundException {
        RevisionImpl revision = new RevisionImpl(Segment.fromScopedName("Foo/Bar/1"), 2, 3);
        String string = revision.toString();
        Revision revision2 = Revision.fromString(string);
        assertEquals(revision, revision2);
        assertEquals(Segment.fromScopedName("Foo/Bar/1"), revision2.asImpl().getSegment());
        assertEquals(2, revision2.asImpl().getOffsetInSegment());
        assertEquals(3, revision2.asImpl().getEventAtOffset());

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        @Cleanup
        ObjectOutputStream oout = new ObjectOutputStream(bout);
        oout.writeObject(revision);
        byte[] byteArray = bout.toByteArray();
        ObjectInputStream oin = new ObjectInputStream(new ByteArrayInputStream(byteArray));
        Object revision3 = oin.readObject();
        assertEquals(revision, revision3);
        assertEquals(revision2, revision3);
    }
    
}
