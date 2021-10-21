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
package io.pravega.shared.protocol.netty;

/**
 * The message or sequence of messages that occurred make no sense and must be a result of a bug.
 */
public class InvalidMessageException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public InvalidMessageException() {
        super();
    }

    public InvalidMessageException(String string) {
        super(string);
    }

    public InvalidMessageException(Throwable throwable) {
        super(throwable);
    }
}
