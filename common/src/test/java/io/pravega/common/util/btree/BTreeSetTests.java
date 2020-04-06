/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.util.btree;

import io.pravega.test.common.ThreadPooledTestSuite;

/**
 * Unit tests for the {@link BTreeSet} class.
 */
public class BTreeSetTests extends ThreadPooledTestSuite {

    @Override
    protected int getThreadPoolSize() {
        return 1;
    }
}
