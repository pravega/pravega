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
package io.pravega.client.segment.impl;

/**
 * An exception indicating that a server operation has timed out.
 * This exception is typically thrown when a server operation exceeds
 * the expected time limit for completion by default it is 30 sec.
 */
public class ServerTimeoutException extends RuntimeException {
    /**
     * Creates a new instance of ServerTimeoutException class.
     *
     * @param msg The detail message, which provides context about the timeout exception.
     */
    public ServerTimeoutException(String msg) {
        super(msg);
    }

}
