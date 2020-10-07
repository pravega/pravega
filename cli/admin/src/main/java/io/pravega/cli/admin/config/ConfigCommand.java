/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.cli.admin.config;

import io.pravega.cli.admin.AdminCommand;
import io.pravega.cli.admin.CommandArgs;

/**
 * Base for all Config-related commands.
 */
abstract class ConfigCommand extends AdminCommand {

    static final String COMPONENT = "config";

    ConfigCommand(CommandArgs args) {
        super(args);
    }
}
