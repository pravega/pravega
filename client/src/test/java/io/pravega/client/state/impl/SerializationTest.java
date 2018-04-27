/**
  * Copyright (c) 2018 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.state.impl;

import io.pravega.client.segment.impl.Segment;
import io.pravega.client.state.Revision;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SerializationTest {

    @Test
    public void testRevision() {
        RevisionImpl revision = new RevisionImpl(Segment.fromScopedName("Foo/Bar/1"), 2, 3);
        String string = revision.toString();
        Revision revision2 = Revision.fromString(string);
        assertEquals(revision, revision2);
        assertEquals(Segment.fromScopedName("Foo/Bar/1"), revision2.asImpl().getSegment());
        assertEquals(2, revision2.asImpl().getOffsetInSegment());
        assertEquals(3, revision2.asImpl().getEventAtOffset());
    }
    
}
