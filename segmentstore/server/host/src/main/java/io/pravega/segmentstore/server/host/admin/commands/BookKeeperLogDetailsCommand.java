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
 * Fetches details about a BookKeeperLog.
 */
public class BookKeeperLogDetailsCommand extends BookKeeperCommand {

    /**
     * Creates a new instance of the BookKeeperLogDetailsCommand.
     *
     * @param args The arguments for the command.
     */
    BookKeeperLogDetailsCommand(CommandArgs args) {
        super(args);
    }

    @Override
    public void execute() {
        throw new IllegalArgumentException();
    }

    static CommandDescriptor descriptor() {
        return new CommandDescriptor(BookKeeperCommand.COMPONENT, "log-details",
                "Lists metadata details about a BookKeeperLog, as well as optional Ledger information.",
                new ArgDescriptor("log-id", "Id of the log to get details for."),
                new ArgDescriptor("include-ledgers", "Whether to include BK ledger info as well (true|false)."));
    }
}
