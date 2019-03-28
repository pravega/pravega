/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.segmentstore.server.tables;

import org.junit.Test;

/**
 * Unit tests for the {@link TableCompactor} class.
 */
public class TableCompactorTests {

    /**
     * Tests the {@link TableCompactor#isCompactionRequired} method.
     */
    @Test
    public void testIsCompactionRequired() {

    }

    /**
     * Tests the {@link TableCompactor#compact} method when compaction is up-to-date.
     */
    @Test
    public void testCompactionUpToDate() {

    }

    /**
     * Tests the {@link TableCompactor#compact} method when compaction results in no entries needing copying.
     */
    @Test
    public void testCompactionNoCopy() {

    }

    /**
     * Tests the {@link TableCompactor#compact} method when compaction requires entries be copied.
     */
    @Test
    public void testCompactionWithCopy() {

    }
}
