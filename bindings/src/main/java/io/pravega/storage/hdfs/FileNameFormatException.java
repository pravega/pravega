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
