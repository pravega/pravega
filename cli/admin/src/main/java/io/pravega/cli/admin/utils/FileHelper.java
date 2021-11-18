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
     * @throws IOException if the file/directory already exists or if creation fails.
     */
    public static File createFileAndDirectory(String fileName) throws IOException {
        File f = new File(fileName);
        // If file exists throw FileAlreadyExistsException, an existing file should not be overwritten with new data.
        if (f.exists()) {
            throw new FileAlreadyExistsException("Cannot write segment data into a file that already exists.");
        }
        if (!f.getParentFile().exists()) {
            f.getParentFile().mkdirs();
        }
        f.createNewFile();
        return f;
    }
}
