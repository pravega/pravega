/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.client.stream.impl;

import java.io.IOException;
import org.junit.Before;
import org.junit.Test;

public class SecureControllerImplTest extends ControllerImplTest {
    @Override
    @Before
    public void setup() throws IOException {
        this.testSecure = true;
        super.setup();
    }

    @Override
    @Test
    public void testRetries() {

    }
}
