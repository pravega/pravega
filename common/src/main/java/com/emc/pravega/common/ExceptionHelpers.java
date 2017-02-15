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

package com.emc.pravega.common;

import java.io.IOException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

/**
 * Helper methods with respect to Exceptions.
 */
public class ExceptionHelpers {

    /**
     * Determines if the given Throwable represents a fatal exception and cannot be handled.
     *
     * @param ex The Throwable to inspect.
     * @return True if a fatal error which must be rethrown, false otherwise (it can be handled in a catch block).
     */
    public static boolean mustRethrow(Throwable ex) {
        return ex instanceof OutOfMemoryError
                || ex instanceof StackOverflowError;
    }

    /**
     * Extracts the inner exception from any Exception that may have an inner exception.
     *
     * @param ex The exception to query.
     */
    public static Throwable getRealException(Throwable ex) {
        if (canInspectCause(ex)) {
            Throwable cause = ex.getCause();
            if (cause != null) {
                return getRealException(cause);
            }
        }

        return ex;
    }

    private static boolean canInspectCause(Throwable ex) {
        return ex instanceof CompletionException
                || ex instanceof ExecutionException
                || ex instanceof IOException;
    }
}
