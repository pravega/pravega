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

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;

public class FileOperationsTests {

    @Test
    public void testDeleteNonExistentFile() throws IOException {
        HashMap<Integer, File> files = new HashMap<Integer, File>();
        File dir = Files.createTempDirectory(null).toFile();
        files.put(0, dir);
        // File should successfully be deleted.
        Assert.assertTrue(FileOperations.cleanupDirectories(files));
        // File does not exist, should throw exception.
        Assert.assertFalse(FileOperations.cleanupDirectories(files));
    }

}
