/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
