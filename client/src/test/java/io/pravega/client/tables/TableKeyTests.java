/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.tables;

import lombok.val;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit tests for the {@link TableKey} class.
 */
public class TableKeyTests {
    @Test
    public void testConstructor() {
        String keyContents = "KeyContents";
        val k = new TableKey<>(keyContents, Version.NOT_EXISTS);
        Assert.assertSame(keyContents, k.getKey());
        Assert.assertSame(Version.NOT_EXISTS, k.getVersion());
    }
}
