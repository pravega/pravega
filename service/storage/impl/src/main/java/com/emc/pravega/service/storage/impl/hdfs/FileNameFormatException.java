/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */

package com.emc.pravega.service.storage.impl.hdfs;

import java.io.IOException;

/**
 * Exception that indicates a malformed file name.
 */
class FileNameFormatException extends IOException {
    FileNameFormatException(String fileName, String message) {
        super(getMessage(fileName, message));
    }

    FileNameFormatException(String fileName, String message, Throwable cause) {
        super(getMessage(fileName, message), cause);
    }

    private static String getMessage(String fileName, String message) {
        return String.format("Invalid segment file name '%s'. %s", fileName, message);
    }
}
