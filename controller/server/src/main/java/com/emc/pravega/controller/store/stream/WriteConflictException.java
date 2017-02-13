/**
 *  Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.emc.pravega.controller.store.stream;

/**
 * Exception thrown when you are attempting to update a stale value.
 */
public class WriteConflictException extends RuntimeException {

    private static final long serialVersionUID = 1L;
    private static final String FORMAT_STRING = "concurrency error for path %s.";

    /**
     * Creates a new instance of WriteConflictException class.
     *
     * @param value value
     */
    public WriteConflictException(final String value) {
        super(String.format(FORMAT_STRING, value));
    }
}
