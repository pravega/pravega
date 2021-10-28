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
package io.pravega.common.util;

/**
 * Exception thrown by {@link Retry} utility class when all of the configured number of attempts have failed.
 * The cause for this exception will be set to the final failure.
 */
public class RetriesExhaustedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public RetriesExhaustedException(Throwable last) {
        super(last);
    }
    
    public RetriesExhaustedException(String message, Throwable last) {
        super(message, last);
    }
}
