/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;


public class IdGeneratorTest {

    @Test
    public void getRequesterId() {
        long id1 = IdGenerator.getRequesterId();
        long id2 = IdGenerator.getRequesterId();
        assertEquals("Requester Id should be incremented", id1 >> Integer.SIZE, (id2 >> Integer.SIZE) -1);
        assertEquals("Request Id starts from 0", 0, (int) id1 );
        assertEquals("Request Id starts from 0", 0, (int) id2 );

        long idWithUpdatedRequestId = IdGenerator.getRequestId().applyAsLong(id1);
        assertEquals("Requester ID should be the same as earlier", id1 >> Integer.SIZE, idWithUpdatedRequestId >> Integer.SIZE);
        assertEquals("Request Id should be incremented.", 1, (int) idWithUpdatedRequestId);
    }
}