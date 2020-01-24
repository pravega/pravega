/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.controller.server.eventProcessor;

import org.junit.Before;

public class SecureLocalControllerTest extends LocalControllerTest {
    @Override
    @Before
    public void setup() {
        this.authEnabled = true;
        super.setup();
    }

}