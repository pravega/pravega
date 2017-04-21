/**
 *
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
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
        if (file.exists()) {
            File[] files = file.listFiles();
            if (files != null) {
                for (File f : files) {
                    if (f.isDirectory()) {
                        deleteFileOrDirectory(f);
                    } else {
                        f.delete();
                    }
                }
            }
        }

        return file.delete();
    }
}
