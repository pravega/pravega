/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.storage.hdfs;

import java.io.IOException;

/**
 * Exception that indicates a malformed file name.
 */
class FileNameFormatException extends IOException {

    private static final long serialVersionUID = 1L;

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
