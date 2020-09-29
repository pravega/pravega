/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.cli.admin.utils;

import io.pravega.cli.admin.AdminCommandState;
import org.junit.Test;

import java.io.IOException;

public class ConfigUtilsTest {

    @Test
    public void testConfigUtils() throws IOException {
        AdminCommandState commandState = new AdminCommandState();
        System.setProperty("pravega.configurationFile", "../../config/admin-cli.properties");
        System.setProperty("pravegaservice", "pravegaservice");
        ConfigUtils.loadProperties(commandState);
    }

}
