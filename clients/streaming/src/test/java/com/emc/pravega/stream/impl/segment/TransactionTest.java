/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.emc.pravega.stream.impl.segment;

import static org.junit.Assert.*;

import org.junit.Ignore;
import org.junit.Test;

@Ignore
public class TransactionTest {

    /**
     * Test if connection should be dropped once timeout is reached
     */
    @Test
    public void testTimeoutDropsTxn() {
        fail();
    }

    /**
     * Test if calling commit() twice results in any failure.
     */
    @Test
    public void testCommitTwice() {
        fail();
    }

    /**
     * Test if commit fails if drop is called prior to it.
     */
    @Test
    public void testDropThenCommit() {
        fail();
    }

    /**
     * Test if calling drop() twice results in any failure.
     */
    @Test
    public void testDropTwice() {
        fail();
    }

    /**
     * Test if commit succeeds once drop is trigger right after.
     */
    @Test
    public void testCommitThenDrop() {
        fail();
    }

    /**
     * Test if transaction status is maintained properly.
     */
    @Test
    public void testGetTxStatus() {
        fail();
    }

    /**
     * Check if log can be closed.
     */
    @Test
    public void testLogCloses() {
        fail();
    }
}
