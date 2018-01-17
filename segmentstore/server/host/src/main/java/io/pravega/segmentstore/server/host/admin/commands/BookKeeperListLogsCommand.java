/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.host.admin.commands;

/**
 * Lists all BookKeeper Logs.
 */
class BookKeeperListLogsCommand extends BookKeeperCommand{
    /**
     * Creates a new instance of the BookKeeperListLogsCommand.
     * @param args The arguments for the command.
     */
    BookKeeperListLogsCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() {

    }

    static CommandDescriptor descriptor() {
        return new CommandDescriptor(BookKeeperCommand.COMPONENT, "list-logs", "Lists all BookKeeper Logs.");
    }
}
