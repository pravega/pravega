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
package io.pravega.cli.admin.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;

public class FileHelper {

    /**
     * Creates the file (and parent directory if required).
     *
     * @param fileName The name of the file to create.
     * @return A {@link File} object representing the filename provided.
     * @throws FileAlreadyExistsException if the file already exists, to avoid any accidental overwrites.
     * @throws IOException if the file/directory creation fails.
     */
    public static File createFileAndDirectory(String fileName) throws IOException, FileAlreadyExistsException {
        File f = new File(fileName);
        // If file exists throw FileAlreadyExistsException, an existing file should not be overwritten with new data.
        if (f.exists()) {
            throw new FileAlreadyExistsException("Cannot write segment data into a file that already exists.");
        }
        if (f.getParentFile() != null) {
            if (!f.getParentFile().exists()) {
                f.getParentFile().mkdirs();
            }
        }
        f.createNewFile();
        return f;
    }
}
