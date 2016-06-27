/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.emc.logservice.contracts;

/**
 * General (unchecked) Streaming Exception.
 */
public class RuntimeStreamingException extends RuntimeException {
    /**
     * Creates a new instance of the RuntimeStreamingException class.
     *
     * @param message The detail message.
     */
    public RuntimeStreamingException(String message) {
        super(message);
    }

    /**
     * Creates a new instance of the RuntimeStreamingException class.
     *
     * @param message The detail message.
     * @param cause   The cause of the exception.
     */
    public RuntimeStreamingException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates a new instance of the RuntimeStreamingException class.
     *
     * @param message            The detail message.
     * @param cause              The cause of the exception.
     * @param enableSuppression  Whether or not suppression is enabled or disabled
     * @param writableStackTrace Whether or not the stack trace should be writable
     */
    public RuntimeStreamingException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
