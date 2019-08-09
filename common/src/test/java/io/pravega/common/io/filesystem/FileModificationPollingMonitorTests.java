/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.common.io.filesystem;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.function.Consumer;

public class FileModificationPollingMonitorTests extends FileModificationMonitorTests {

    private final static Consumer<File> NOOP_CONSUMER = c -> { };

    @Override
    FileModificationMonitor prepareObjectUnderTest(String path) throws FileNotFoundException {
        return new FileModificationPollingMonitor(path, NOOP_CONSUMER);
    }
}
