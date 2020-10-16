/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.cli.user.config;

import io.pravega.cli.user.CommandArgs;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

public class ConfigCommandTest {

    @Test
    public void testConfigCommand() {
        InteractiveConfig config = InteractiveConfig.getDefault();
        CommandArgs commandArgs = new CommandArgs(Arrays.asList("controller-uri=0",
                "default-segment-count=1", "timeout-millis=2", "max-list-items=3", "pretty-print=true"), config);
        ConfigCommand.Set set = new ConfigCommand.Set(commandArgs);
        set.execute();
        Assert.assertEquals("0", config.getControllerUri());
        Assert.assertEquals(2, config.getTimeoutMillis());
        Assert.assertTrue(config.isPrettyPrint());
        Assert.assertEquals(3, config.getMaxListItems());
        Assert.assertNotNull(ConfigCommand.Set.descriptor());

        commandArgs = new CommandArgs(Collections.emptyList(), config);
        ConfigCommand.List list = new ConfigCommand.List(commandArgs);
        list.execute();
        Assert.assertNotNull(ConfigCommand.List.descriptor());
    }
}
