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

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.HashMap;

@Slf4j
public class FileOperations {

    public static boolean cleanupDirectories(HashMap<?, File> toDelete) {
        for (File dir : toDelete.values()) {
            log.info("Cleaning up " + dir);
            if (!FileUtils.deleteQuietly(dir)) {
                log.info("Failed deleting directory: {}", dir);
                return false;
            }
        }
        return true;
    }

}
