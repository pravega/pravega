/**
 *  Copyright (c) 2016 Dell Inc. or its subsidiaries. All Rights Reserved
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
package com.emc.pravega.service.server.logs;

import com.emc.pravega.service.contracts.StreamingException;

/**
 * An exception that is thrown when serialization or deserialization fails.
 */
public class SerializationException extends StreamingException {
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * Creates a new instance of the SerializationException class.
     *
     * @param source  The name of the source of the exception.
     * @param message The detail message.
     */
    public SerializationException(String source, String message) {
        super(combine(source, message));
    }

    /**
     * Creates a new instance of the SerializationException class.
     *
     * @param source  The name of the source of the exception.
     * @param message The detail message.
     * @param cause   Te cause of the exception.
     */
    public SerializationException(String source, String message, Throwable cause) {
        super(combine(source, message), cause);
    }

    /**
     * Creates a new instance of the SerializationException class.
     *
     * @param source             The name of the source of the exception.
     * @param message            The detail message.
     * @param cause              Te cause of the exception.
     * @param enableSuppression  Whether or not suppression is enabled or disabled
     * @param writableStackTrace Whether or not the stack trace should be writable
     */
    public SerializationException(String source, String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(combine(source, message), cause, enableSuppression, writableStackTrace);
    }

    private static String combine(String source, String message) {
        return String.format("%s: %s", source, message);
    }
}
