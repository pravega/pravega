/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
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
package io.pravega.controller.store.stream;

/**
 * Exception thrown when data already exists in metadata.
 */
public class DataExistsException extends RuntimeException {
    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private static final String FORMAT_STRING = "Data %s already exists.";

    /**
     * Creates a new instance of DataExistsException class.
     *
     * @param name data identifier
     */
    public DataExistsException(final String name) {
        super(String.format(FORMAT_STRING, name));
    }

    /**
     * Creates a new instance of DataExistsException class.
     *
     * @param name  data identifier
     * @param cause error cause
     */
    public DataExistsException(final String name, final Throwable cause) {
        super(String.format(FORMAT_STRING, name), cause);
    }
}
