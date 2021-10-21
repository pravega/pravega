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
package io.pravega.common.io;

import java.io.File;

/**
 * Extension methods to the java.io.File class.
 */
public class FileHelpers {
    /**
     * Deletes the given file or directory. If a directory, recursively deletes all sub-directories and files.
     *
     * @param file The target to delete.
     * @return True if the target was deleted, false otherwise.
     */
    public static boolean deleteFileOrDirectory(File file) {
        if (file.exists() && file.isDirectory()) {
            File[] files = file.listFiles();
            if (files != null) {
                for (File f : files) {
                    deleteFileOrDirectory(f);
                }
            }
        }

        return file.delete();
    }
}
